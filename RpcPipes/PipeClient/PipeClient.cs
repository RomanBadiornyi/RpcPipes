using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeClient;
public class PipeClient<TP> : PipeConnectionManager, IDisposable, IAsyncDisposable
    where TP : IPipeProgress
{
    private readonly ILogger<PipeClient<TP>> _logger;
    
    private static Meter _meter = new("PipeClient");
    private static Counter<int> _serverConnectionsCounter = _meter.CreateCounter<int>("client.server-connections");
    private static Counter<int> _clientConnectionsCounter = _meter.CreateCounter<int>("client.client-connections");
    private static Counter<int> _sentMessagesCounter = _meter.CreateCounter<int>("client.sent-messages");
    private static Counter<int> _receivedMessagesCounter = _meter.CreateCounter<int>("client.received-messages");

    private readonly IPipeProgressReceiver<TP> _progressHandler;
    private readonly IPipeMessageWriter _messageWriter;

    private readonly Task _serverTask;
    private readonly CancellationTokenSource _serverTaskCancellation;

    private readonly string _sendPipe;
    private readonly string _progressPipe;
    private readonly string _receivePipe;

    private readonly ConcurrentDictionary<Guid, PipeClientRequestHandle> _requestQueue = new();

    private readonly ConcurrentDictionary<string, PipeMessageChannel<PipeClientRequestHandle>> _requestChannels = new();
    private readonly ConcurrentDictionary<string, PipeMessageChannel<PipeClientRequestHandle>> _progressChannels = new();

    public TimeSpan ProgressFrequency = TimeSpan.FromSeconds(5);
    public TimeSpan Deadline = TimeSpan.FromHours(1);

    public PipeClient(
        ILogger<PipeClient<TP>> logger, 
        string sendPipe,          
        string progressPipe, 
        string receivePipe,
        int instances, 
        IPipeProgressReceiver<TP> progressHandler, 
        IPipeMessageWriter messageWriter) :
            base(logger, instances, 1 * 1024, 4 * 1024, PipeOptions.Asynchronous | PipeOptions.WriteThrough)
    {
        _logger = logger;

        _progressHandler = progressHandler;
        _messageWriter = messageWriter;

        _sendPipe = sendPipe;
        _progressPipe = progressPipe;
        _receivePipe = receivePipe;

        _serverTaskCancellation = new CancellationTokenSource();

        OnServerConnect = () => _serverConnectionsCounter.Add(1);
        OnServerDisconnect = () => _serverConnectionsCounter.Add(-1);

        OnClientConnect = () => _clientConnectionsCounter.Add(1);
        OnClientDisconnect = () => _clientConnectionsCounter.Add(-1);

        _serverTask = Task
            .WhenAll(
                StartServerListener(
                    Instances, 
                    taskAction: () => RunServerMessageLoop(_serverTaskCancellation.Token, receivePipe, HandleReceiveMessage)
                )
            )
            //wait also until we complete all client connections
            .ContinueWith(_ => Task.WhenAll(_requestChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray()), CancellationToken.None)
            .ContinueWith(_ => Task.WhenAll(_progressChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray()), CancellationToken.None);
    }

    public async Task<TRep> SendRequest<TReq, TRep>(TReq request, CancellationToken token)
    {
        using var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);

        var requestMessage = new PipeClientRequestHandle(Guid.NewGuid(), _sendPipe, _progressPipe)
        {
            RequestCancellation = requestCancellation,
            ReceiveHandle = new SemaphoreSlim(0),
            ProgressCheckHandle = new SemaphoreSlim(1),
            ProgressCheckTime = DateTime.Now,
            ProgressCheckFrequency = ProgressFrequency            
        };

        var pipeRequest = new PipeMessageRequest<TReq>();
        var pipeResponse = new PipeMessageResponse<TRep>();

        pipeRequest.Request = request;
        pipeRequest.ProgressFrequency = ProgressFrequency;
        pipeRequest.Deadline = Deadline;        

        requestMessage.SendAction =
            (c, t) => SendRequest(requestMessage.Id, pipeRequest, c, t);
        requestMessage.ReceiveAction =
            async (p, t) => { pipeResponse = await ReadReply<TRep>(requestMessage.Id, p, t); };

        if (_requestQueue.TryAdd(requestMessage.Id, requestMessage))
        {
            ProcessClientMessage(
                _requestChannels, requestMessage.RequestPipe, requestMessage, HandleSendMessage, _serverTaskCancellation.Token);
        }
        else
            throw new InvalidOperationException($"Message with {requestMessage.Id} already scheduled");

        _logger.LogDebug("scheduled request execution for message {MessageId}", requestMessage.Id);
        try
        {
            await requestMessage.ReceiveHandle.WaitAsync(requestCancellation.Token);
            _logger.LogDebug("received reply for message {MessageId} from server", requestMessage.Id);
        }
        catch (OperationCanceledException)
        {
            _requestQueue.TryRemove(requestMessage.Id, out _);
            _logger.LogDebug("cancelled request message {MessageId}", requestMessage.Id);
            throw;
        }

        if (requestMessage.Exception != null)
            throw requestMessage.Exception;
        var responseError = pipeResponse?.ReplyError;
        if (responseError != null)
            throw responseError.ToException();
        return pipeResponse.Reply;
    }

    private async Task HandleSendMessage(
        PipeClientRequestHandle requestMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        using var sendCancellation = CancellationTokenSource
            .CreateLinkedTokenSource(cancellation, requestMessage.RequestCancellation.Token);
        var requestMessageSent = false;
        try
        {
            await requestMessage.SendAction.Invoke(protocol, sendCancellation.Token);
            requestMessageSent = true;
        }
        catch (Exception e)
        {
            requestMessage.Retries++;
            if (requestMessage.Retries < 3)
            {
                ProcessClientMessage(
                    _requestChannels, requestMessage.RequestPipe, requestMessage, HandleSendMessage, _serverTaskCancellation.Token);
            }
            else
            {
                requestMessage.Exception = e;
                throw;
            }
        }

        if (requestMessageSent)
        {
            _sentMessagesCounter.Add(1);
            ProcessClientMessage(_progressChannels, requestMessage.ProgressPipe, requestMessage, HandleProgressMessage, _serverTaskCancellation.Token);
            _logger.LogDebug("sent message {MessageId} for execution", requestMessage.Id);
        }
    }

    private async Task HandleProgressMessage(
        PipeClientRequestHandle requestMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        var requestMessageActive = false;
        var lastCheckInterval = DateTime.Now - requestMessage.ProgressCheckTime;
        if (lastCheckInterval < requestMessage.ProgressCheckFrequency)
        {
            await Task.Delay(requestMessage.ProgressCheckFrequency - lastCheckInterval, cancellation);
            ProcessClientMessage(_progressChannels, requestMessage.ProgressPipe, requestMessage, HandleProgressMessage, _serverTaskCancellation.Token);
            return;
        }

        if (!cancellation.IsCancellationRequested)
        {
            await requestMessage.ProgressCheckHandle.WaitAsync(cancellation);
            try
            {
                requestMessageActive = _requestQueue.ContainsKey(requestMessage.Id);
                if (requestMessageActive)
                {
                    var progressToken = new PipeProgressToken 
                    { 
                        Id = requestMessage.Id, 
                        Active = !requestMessage.RequestCancellation.IsCancellationRequested 
                    };
                    await protocol.TransferMessage(
                        new PipeMessageHeader { MessageId = requestMessage.Id }, 
                        (s, c) => _messageWriter.WriteData(progressToken, s, c), 
                        cancellation);
                    var progress = await protocol.ReceiveMessage(
                        (s, c) => _messageWriter.ReadData<TP>(s, c), 
                        cancellation);
                    if (progress != null)
                    {
                        if (progress.Progress > 0)
                        {
                            _logger.LogDebug("received progress update for message {MessageId} with value {Progress}", requestMessage.Id, progress.Progress);
                            await _progressHandler.ReceiveProgress(progress);
                        }
                    }
                    else
                    {
                        _logger.LogWarning("cancelling execution of request message {MessageId} as it's not handled by the server", requestMessage.Id);
                        requestMessage.RequestCancellation.Cancel();
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "unhandled error occurred while requesting progress for message {MessageId}", requestMessage.Id);
                throw;
            }
            finally
            {
                requestMessage.ProgressCheckHandle.Release();
                requestMessage.ProgressCheckTime = DateTime.Now;
                //if message still active, add it back to channel for subsequent progress check
                if (requestMessageActive)
                    ProcessClientMessage(_progressChannels, requestMessage.ProgressPipe, requestMessage, HandleProgressMessage, _serverTaskCancellation.Token);
            }
        }
    }

    private async Task HandleReceiveMessage(PipeProtocol protocol, CancellationToken cancellation)
    {
        PipeClientRequestHandle requestMessage = null;
        var header = await protocol
            .BeginReceiveMessage(id => {
                //ensure we stop progress task as soon as we started receiving reply
                if (_requestQueue.TryGetValue(id, out requestMessage))
                {
                    _requestQueue.TryRemove(id, out _);
                    _logger.LogDebug("received reply message {MessageId}, cancelled progress updated", requestMessage.Id);
                }
                else
                {
                    _logger.LogWarning("received reply message {MessageId} not found in request queue", id);
                }
            }, cancellation);

        if (header != null && requestMessage != null)
        {
            await requestMessage.ProgressCheckHandle.WaitAsync(requestMessage.RequestCancellation.Token);
            try
            {
                await requestMessage.ReceiveAction.Invoke(protocol, requestMessage.RequestCancellation.Token);
            }
            catch (Exception e)
            {
                requestMessage.Exception = e;
            }
            finally
            {
                requestMessage.ProgressCheckHandle.Release();
                requestMessage.ReceiveHandle.Release();
                _receivedMessagesCounter.Add(1);
                _logger.LogDebug("completed processing of reply message {MessageId}", header.MessageId);                
            }
        }
    }

    private async Task SendRequest<TReq>(Guid id, PipeMessageRequest<TReq> request, PipeProtocol protocol, CancellationToken token)
    {
        try
        {
            _logger.LogDebug("sending request message {MessageId} to server", id);
            var header = new PipeAsyncMessageHeader { MessageId = id, ReplyPipe = _receivePipe };
            await protocol.BeginTransferMessageAsync(header, token);
            await protocol.EndTransferMessage(id, Write, token);
            _logger.LogDebug("sent request message {MessageId} to server", id);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred while sending request message {MessageId} to server", id);
            throw;
        }

        Task Write(Stream stream, CancellationToken cancellation)
            => _messageWriter.WriteRequest(request, stream, cancellation);
    }

    private async Task<PipeMessageResponse<TRep>> ReadReply<TRep>(Guid id, PipeProtocol protocol, CancellationToken token)
    {
        try
        {
            _logger.LogDebug("receiving reply for message {MessageId} from server", id);
            var response = await protocol.EndReceiveMessage(id, Read, token);
            _logger.LogDebug("received reply for message {MessageId} from server", id);
            return response;
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred while receiving reply for message {MessageId} from server", id);
            throw;
        }

        ValueTask<PipeMessageResponse<TRep>> Read(Stream stream, CancellationToken cancellation)
            => _messageWriter.ReadResponse<TRep>(stream, cancellation);
        
    }

    public void Dispose()
    {
        _serverTaskCancellation.Cancel();
        _serverTask.Wait();
    }

    public async ValueTask DisposeAsync()
    {
        _serverTaskCancellation.Cancel();
        await _serverTask;
    }
}