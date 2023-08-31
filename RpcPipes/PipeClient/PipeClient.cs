using System.Collections.Concurrent;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeClient;
public class PipeClient<TP> : PipeConnectionManager, IDisposable, IAsyncDisposable
    where TP : IPipeProgress
{
    private readonly ILogger<PipeClient<TP>> _logger;

    private readonly IPipeProgressReceiver<TP> _progressHandler;
    private readonly IPipeMessageWriter _messageWriter;

    private readonly Task _serverTask;
    private readonly CancellationTokenSource _serverTaskCancellation;

    private readonly string _sendPipe;
    private readonly string _progressPipe;
    private readonly Guid _receivePipe;

    private int _activeConnections;
    private int _clientConnections;
    private int _sentMessages;
    private int _receivedMessages;

    private readonly ConcurrentDictionary<Guid, PipeClientRequestHandle> _requestQueue = new();

    private readonly ConcurrentDictionary<string, MessageChannel<PipeClientRequestHandle>> _requestChannels = new();
    private readonly ConcurrentDictionary<string, MessageChannel<PipeClientRequestHandle>> _progressChannels = new();

    public TimeSpan ProgressFrequency = TimeSpan.FromSeconds(5);
    public TimeSpan Deadline = TimeSpan.FromHours(1);

    public PipeClient(
        ILogger<PipeClient<TP>> logger, 
        string sendPipe,          
        string progressPipe, 
        Guid receivePipe,
        int instances, 
        IPipeProgressReceiver<TP> progressHandler, 
        IPipeMessageWriter messageWriter) :
            base(logger, instances, 4 * 1024, PipeOptions.Asynchronous | PipeOptions.WriteThrough)
    {
        _logger = logger;

        _progressHandler = progressHandler;
        _messageWriter = messageWriter;

        _sendPipe = sendPipe;
        _progressPipe = progressPipe;
        _receivePipe = receivePipe;

        _serverTaskCancellation = new CancellationTokenSource();

        OnServerConnect = () => Interlocked.Increment(ref _activeConnections);
        OnServerDisconnect = () => Interlocked.Decrement(ref _activeConnections);

        OnClientConnect = () => Interlocked.Increment(ref _clientConnections);
        OnClientDisconnect = () => Interlocked.Decrement(ref _clientConnections);

        var timer = new Timer(_ => {
            _logger.LogTrace(
                "DIAGNOSTIC: active connections {ActiveConnections}. client connections {ClientConnections}",
                _activeConnections, _clientConnections
            );
            _logger.LogTrace(
                "DIAGNOSTIC: sent messages {SentMessages}, received messages {ReceivedMessages}",
                _sentMessages, _receivedMessages
            );
        }, null, 0, 30000);

        _serverTask = Task.WhenAll(
                StartServerListener(Instances, () => {
                    return RunServerMessageLoop(
                        _serverTaskCancellation.Token,
                        receivePipe.ToString(),
                        HandleReceiveMessage
                    );
                })
            )
            //wait also until we complete all client connections
            .ContinueWith(_ => Task.WhenAll(_requestChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray()))
            .ContinueWith(_ => Task.WhenAll(_progressChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray()))
            .ContinueWith(_ => {
                timer.Dispose();
            }, CancellationToken.None);
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
            async (s, bs, t) => { pipeResponse = await ReadReply<TRep>(requestMessage.Id, s, bs, t); };

        if (_requestQueue.TryAdd(requestMessage.Id, requestMessage))
        {
            ProcessClientMessage(
                _requestChannels,
                requestMessage.RequestPipe,
                requestMessage,
                (r, s, c) => HandleSendMessage(r, s, c),
                _serverTaskCancellation.Token);
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
        PipeClientRequestHandle requestMessage, NamedPipeClientStream clientPipeStream, CancellationToken cancellation)
    {
        using var sendCancellation = CancellationTokenSource
            .CreateLinkedTokenSource(cancellation, requestMessage.RequestCancellation.Token);
        var requestMessageSent = false;
        try
        {
            await requestMessage.SendAction.Invoke(clientPipeStream, sendCancellation.Token);
            requestMessageSent = true;
        }
        catch (Exception e)
        {
            requestMessage.Retries++;
            if (requestMessage.Retries < 3)
            {
                ProcessClientMessage(
                    _requestChannels,
                    requestMessage.RequestPipe,
                    requestMessage,
                    (r, s, c) => HandleSendMessage(r, s, c),
                    _serverTaskCancellation.Token);
            }
            else
            {
                requestMessage.Exception = e;
                throw;
            }
        }

        if (requestMessageSent)
        {
            Interlocked.Increment(ref _sentMessages);
            ProcessClientMessage(
                _progressChannels,
                requestMessage.ProgressPipe,
                requestMessage,
                (r, s, c) => HandleProgressMessage(r, s, c),
                _serverTaskCancellation.Token);
            _logger.LogDebug("sent message {MessageId} for execution", requestMessage.Id);
        }
    }

    private async Task HandleProgressMessage(
        PipeClientRequestHandle requestMessage, Stream progressPipeStream, CancellationToken cancellation)
    {
        var requestMessageActive = false;
        var lastCheckInterval = DateTime.Now - requestMessage.ProgressCheckTime;
        if (lastCheckInterval < requestMessage.ProgressCheckFrequency)
        {
            await Task.Delay(requestMessage.ProgressCheckFrequency - lastCheckInterval, cancellation);
            ProcessClientMessage(
                _progressChannels,
                requestMessage.ProgressPipe,
                requestMessage,
                (r, s, c) => HandleProgressMessage(r, s, c),
                _serverTaskCancellation.Token);
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
                    var protocol = new PipeProtocol(progressPipeStream);
                    var progressToken = new PipeProgressToken 
                    { 
                        Id = requestMessage.Id, 
                        Active = !requestMessage.RequestCancellation.IsCancellationRequested 
                    };
                    await protocol.TransferMessage(
                        requestMessage.Id, 
                        (s, c) => _messageWriter.WriteData(progressToken, s, c), 
                        BufferSize, 
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
                {
                    ProcessClientMessage(
                        _progressChannels,
                        requestMessage.ProgressPipe,
                        requestMessage,
                        (r, s, c) => HandleProgressMessage(r, s, c),
                        _serverTaskCancellation.Token);
                }
            }
        }
    }

    private async Task HandleReceiveMessage(NamedPipeServerStream serverPipeStream, CancellationToken cancellation)
    {
        PipeClientRequestHandle requestMessage = null;
        var protocol = new PipeProtocol(serverPipeStream);
        var (messageId, bufferSize) = await protocol
            .BeginReceiveMessage(id => {
                //ensure we stop progress task as soon as we started receiving reply
                if (_requestQueue.TryGetValue(id, out requestMessage))
                {
                    _requestQueue.TryRemove(id, out _);
                    _logger.LogDebug("received reply message {MessageId}, cancelled progress updated", requestMessage.Id);
                }
                else
                {
                    _logger.LogWarning("received reply message {MessageId} not found in request queue");
                }
            }, cancellation);

        if (messageId != null && requestMessage != null)
        {
            await requestMessage.ProgressCheckHandle.WaitAsync(requestMessage.RequestCancellation.Token);
            try
            {
                await requestMessage.ReceiveAction.Invoke(serverPipeStream, bufferSize, requestMessage.RequestCancellation.Token);
            }
            catch (Exception e)
            {
                requestMessage.Exception = e;
            }
            finally
            {
                requestMessage.ProgressCheckHandle.Release();
                requestMessage.ReceiveHandle.Release();
                _logger.LogDebug("completed processing of reply message {MessageId}", messageId);
                Interlocked.Increment(ref _receivedMessages);
            }
        }
    }

    private async Task SendRequest<TReq>(Guid id, PipeMessageRequest<TReq> request, Stream client, CancellationToken token)
    {
        try
        {
            _logger.LogDebug("sending request message {MessageId} to server", id);
            var protocol = new PipeProtocol(client);
            
            await protocol.BeginTransferMessageAsync(id, BufferSize, _receivePipe, token);
            await protocol.EndTransferMessage(id, Write, BufferSize, token);
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

    private async Task<PipeMessageResponse<TRep>> ReadReply<TRep>(Guid id, Stream server, int bufferSize, CancellationToken token)
    {
        try
        {
            _logger.LogDebug("receiving reply for message {MessageId} from server", id);
            var protocol = new PipeProtocol(server);
            var response = await protocol.EndReceiveMessage(id, Read, bufferSize, token);
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