using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeClient;
public class PipeClient<TP> : IDisposable, IAsyncDisposable
    where TP : IPipeHeartbeat
{
    private readonly ILogger<PipeClient<TP>> _logger;
    
    private static Meter _meter = new("PipeClient");
    private static Counter<int> _sentMessagesCounter = _meter.CreateCounter<int>("sent-messages");
    private static Counter<int> _receivedMessagesCounter = _meter.CreateCounter<int>("received-messages");

    private readonly IPipeHeartbeatReceiver<TP> _heartBeatHandler;
    private readonly IPipeMessageWriter _messageWriter;

    private readonly Task _serverTask;
    private readonly CancellationTokenSource _serverTaskCancellation;

    private readonly string _sendPipe;
    private readonly string _heartBeatPipe;
    private readonly string _receivePipe;
    
    private readonly ConcurrentDictionary<Guid, PipeClientRequestHandle> _requestQueue = new();

    private readonly ConcurrentDictionary<string, PipeMessageChannel<PipeClientRequestHandle>> _requestChannels = new();
    private readonly ConcurrentDictionary<string, PipeMessageChannel<PipeClientRequestHandle>> _heartBeatsChannels = new();

    public PipeConnectionManager ConnectionPool { get; }

    public PipeClient(
        ILogger<PipeClient<TP>> logger, 
        string sendPipe,          
        string heartBeatPipe, 
        string receivePipe,
        int instances, 
        IPipeHeartbeatReceiver<TP> heartBeatHandler, 
        IPipeMessageWriter messageWriter)
    {
        _logger = logger;

        _heartBeatHandler = heartBeatHandler;
        _messageWriter = messageWriter;

        _sendPipe = sendPipe;
        _heartBeatPipe = heartBeatPipe;
        _receivePipe = receivePipe;
        
        ConnectionPool = new PipeConnectionManager(logger, _meter, instances, 1 * 1024, 4 * 1024, PipeOptions.Asynchronous | PipeOptions.WriteThrough);

        _serverTaskCancellation = new CancellationTokenSource();
        _serverTask = Task
            .WhenAll(ConnectionPool.ProcessServerMessages(receivePipe, HandleReceiveMessage, _serverTaskCancellation.Token))
            //wait until we complete all client connections
            .ContinueWith(_ => Task.WhenAll(_requestChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray()), CancellationToken.None)
            .ContinueWith(_ => Task.WhenAll(_heartBeatsChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray()), CancellationToken.None);
    }

    public async Task<TRep> SendRequest<TReq, TRep>(TReq request, PipeRequestContext context, CancellationToken token)
    {
        using var requestCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);

        var requestMessage = new PipeClientRequestHandle(Guid.NewGuid(), _sendPipe, _heartBeatPipe)
        {
            RequestCancellation = requestCancellation,
            ReceiveHandle = new SemaphoreSlim(0),
            HeartbeatCheckHandle = new SemaphoreSlim(1),
            HeartbeatCheckTime = DateTime.Now,
            HeartbeatCheckFrequency = context.Heartbeat            
        };

        var pipeRequest = new PipeMessageRequest<TReq>();
        var pipeResponse = new PipeMessageResponse<TRep>();

        pipeRequest.Request = request;
        pipeRequest.Heartbeat = context.Heartbeat;
        pipeRequest.Deadline = context.Deadline;        

        requestMessage.SendAction =
            (c, t) => SendRequest(requestMessage.Id, pipeRequest, c, t);
        requestMessage.ReceiveAction =
            async (p, t) => { pipeResponse = await ReadReply<TRep>(requestMessage.Id, p, t); };

        if (_requestQueue.TryAdd(requestMessage.Id, requestMessage))
        {
            ConnectionPool.ProcessClientMessage(
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
                ConnectionPool.ProcessClientMessage(
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
            ConnectionPool.ProcessClientMessage(
                _heartBeatsChannels, requestMessage.HeartbeatPipe, requestMessage, HandleHeartbeatMessage, _serverTaskCancellation.Token);
            _logger.LogDebug("sent message {MessageId} for execution", requestMessage.Id);
        }
    }

    private async Task HandleHeartbeatMessage(
        PipeClientRequestHandle requestMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        var requestMessageActive = false;
        var lastCheckInterval = DateTime.Now - requestMessage.HeartbeatCheckTime;
        if (lastCheckInterval < requestMessage.HeartbeatCheckFrequency)
        {
            await Task.Delay(requestMessage.HeartbeatCheckFrequency - lastCheckInterval, cancellation);
            ConnectionPool.ProcessClientMessage(
                _heartBeatsChannels, requestMessage.HeartbeatPipe, requestMessage, HandleHeartbeatMessage, _serverTaskCancellation.Token);
            return;
        }

        if (!cancellation.IsCancellationRequested)
        {
            await requestMessage.HeartbeatCheckHandle.WaitAsync(cancellation);
            try
            {
                requestMessageActive = _requestQueue.ContainsKey(requestMessage.Id);
                if (requestMessageActive)
                {
                    var pipeHeartbeatRequest = new PipeRequestHeartbeat 
                    { 
                        Id = requestMessage.Id, 
                        Active = !requestMessage.RequestCancellation.IsCancellationRequested 
                    };
                    await protocol.TransferMessage(
                        new PipeMessageHeader { MessageId = requestMessage.Id }, 
                        (s, c) => _messageWriter.WriteData(pipeHeartbeatRequest, s, c), 
                        cancellation);
                    var pipeHeartbeat = await protocol.ReceiveMessage(
                        (s, c) => _messageWriter.ReadData<TP>(s, c), 
                        cancellation);
                    if (pipeHeartbeat != null)
                    {
                        if (pipeHeartbeat.Progress > 0)
                        {
                            _logger.LogDebug("received heartbeat update for message {MessageId} with value {Progress}", requestMessage.Id, pipeHeartbeat.Progress);
                            await _heartBeatHandler.OnHeartbeatMessage(pipeHeartbeat);
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
                _logger.LogError(e, "unhandled error occurred while requesting heartbeat for message {MessageId}", requestMessage.Id);
                throw;
            }
            finally
            {
                requestMessage.HeartbeatCheckHandle.Release();
                requestMessage.HeartbeatCheckTime = DateTime.Now;
                //if message still active, add it back to channel for subsequent heartbeat check
                if (requestMessageActive)
                    ConnectionPool.ProcessClientMessage(
                        _heartBeatsChannels, requestMessage.HeartbeatPipe, requestMessage, HandleHeartbeatMessage, _serverTaskCancellation.Token);
            }
        }
    }

    private async Task HandleReceiveMessage(PipeProtocol protocol, CancellationToken cancellation)
    {
        PipeClientRequestHandle requestMessage = null;
        var header = await protocol
            .BeginReceiveMessage(id => {
                //ensure we stop heartbeat task as soon as we started receiving reply
                if (_requestQueue.TryGetValue(id, out requestMessage))
                {
                    _requestQueue.TryRemove(id, out _);
                    _logger.LogDebug("received reply message {MessageId}, cancelled heartbeat updated", requestMessage.Id);
                }
                else
                {
                    _logger.LogWarning("received reply message {MessageId} not found in request queue", id);
                }
            }, cancellation);

        if (header != null && requestMessage != null)
        {
            await requestMessage.HeartbeatCheckHandle.WaitAsync(requestMessage.RequestCancellation.Token);
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
                requestMessage.HeartbeatCheckHandle.Release();
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