using System.Collections.Concurrent;
using System.IO.Pipes;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace RpcPipes;

public class PipeServer<TP> : PipeTransport
    where TP: IPipeProgress
{
    private class RequestMessage
    {
        public Guid Id { get; set; }
        public CancellationTokenSource Cancellation { get; set; }
        public Func<Guid, CancellationTokenSource, Task> ExecuteAction { get; set; }
        public bool IsCompleted { get; set; }
    }

    private class ResponseMessage
    {
        public Guid Id { get; set; }
        public Func<NamedPipeClientStream, Task> Action { get; set; }
    }

    private readonly ILogger<PipeServer<TP>> _logger;

    private readonly string _sendPipe;
    private readonly string _receivePipe;
    private readonly string _progressPipe;

    private readonly IPipeMessageSerializer _serializer;

    private int _pendingMessages;
    private int _activeMessages;
    private int _handledMessages;

    private int _activeConnections;
    private int _clientConnections;

    private readonly object _sync = new();
    private bool _started;
    private Task _serverTask;
    private CancellationTokenSource _serverTaskCancellation;

    private readonly ConcurrentDictionary<Guid, RequestMessage> _requestsQueue = new();
    private readonly Channel<ResponseMessage> _responseChannel = Channel.CreateUnbounded<ResponseMessage>();

    public TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(60);

    public PipeServer(ILogger<PipeServer<TP>> logger, string sendPipe, string receivePipe, string progressPipe, int instances, IPipeMessageSerializer serializer) :
        base(logger, instances, 4 * 1024, PipeOptions.Asynchronous | PipeOptions.WriteThrough)
    {
        _logger = logger;

        _sendPipe = sendPipe;
        _receivePipe = receivePipe;
        _progressPipe = progressPipe;

        _serializer = serializer;
        _serverTask = null;
    }

    public Task Start<TReq, TRep>(
        IPipeMessageHandler<TReq, TRep> messageHandler,
        IPipeProgressHandler<TP> progressHandler,
        CancellationToken token)
    {
        lock (_sync)
        {
            if (_started)
                return _serverTask;
            var timer = new Timer(_ =>
            {
                _logger.LogTrace("DIAGNOSTIC: active connections {ActiveConnections}, client connections {ClientConnections}",
                    _activeConnections, _clientConnections);
                _logger.LogTrace("DIAGNOSTIC: handled messages {HandledMessages}, pending messages {PendingMessages}, active messages {ActiveMessages}, reply messages {ReplyMessages}",
                    _handledMessages, _pendingMessages, _activeMessages, _responseChannel.Reader.Count);
            }, null, 0, 30000);
            _serverTaskCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
            _serverTask = Task.WhenAll(
                StartServerListener(Instances, () => {
                    return RunServerMessageLoop(
                        _serverTaskCancellation.Token,
                        _receivePipe,
                        (s, c) => HandleReceiveMessage(messageHandler, s, c),
                        onConnect: () => Interlocked.Increment(ref _activeConnections),
                        onDisconnect: () => Interlocked.Decrement(ref _activeConnections)
                    );
                }),
                StartServerListener(Instances, () => {
                    return RunServerMessageLoop(
                        _serverTaskCancellation.Token,
                        _progressPipe,
                        (s, c) => HandleProgressMessage(progressHandler, s, c),
                        onConnect: () => Interlocked.Increment(ref _activeConnections),
                        onDisconnect: () => Interlocked.Decrement(ref _activeConnections)
                    );
                }),
                StartServerListener(Instances, () => {
                    return RunClientMessageLoop(
                        _serverTaskCancellation.Token,
                        _sendPipe,
                        ConnectionTimeout,
                        HandleReplyMessage,
                        onConnect: () => Interlocked.Increment(ref _clientConnections),
                        onDisconnect: () => Interlocked.Decrement(ref _clientConnections)
                    );
                })
            ).ContinueWith(_ => {
                _started = false;
                timer.Dispose();
            }, CancellationToken.None);
            _started = true;
            return _serverTask;
        }
    }

    private async Task HandleReceiveMessage<TReq, TRep>(IPipeMessageHandler<TReq, TRep> messageHandler, Stream stream, CancellationToken cancellation)
    {
        var cancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        RequestMessage requestMessage = null;

        var protocol = new PipeProtocol(stream, _serializer);
        var (messageId, bufferSize) = await protocol
            .BeginReceiveMessage(id => {
                //ensure we add current request to outstanding messages before we complete reading request payload
                //this way we ensure that when client starts checking progress - we already can reply as we know about this message
                requestMessage = new RequestMessage { Id = id };
                if (!_requestsQueue.TryAdd(id, requestMessage))
                    requestMessage = null;
            }, cancellation);
        if (messageId != null && requestMessage != null)
        {
            try
            {
                _logger.LogDebug("reading request message {MessageId}", requestMessage.Id);
                var request = await protocol.EndReceiveMessage<TReq>(messageId.Value, bufferSize, cancellation);
                _logger.LogDebug("read request message {MessageId}", requestMessage.Id);

                requestMessage.Cancellation = cancellationSource;
                requestMessage.ExecuteAction = async (id, c) =>
                {
                    await RunRequest(messageHandler, id, request, c.Token);
                };

                _logger.LogDebug("scheduling request execution for message {MessageId}", requestMessage.Id);
                Interlocked.Increment(ref _pendingMessages);
                ThreadPool.QueueUserWorkItem(ExecuteRequest, requestMessage);
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "unable to consume message {MessageId} due to error, reply error back to client", requestMessage.Id);
                var replyError = RequestException.CreateRequestException(e);
                var response = new PipeMessageResponse<TRep> { Reply = default, ReplyError = replyError };
                await ScheduleResponseReply(requestMessage.Id, response, cancellation);                 
            }
        }
    }

    private async Task HandleProgressMessage(IPipeProgressHandler<TP> progressHandler, Stream stream, CancellationToken cancellation)
    {
        var protocol = new PipeProtocol(stream, _serializer);
        var progressToken = await protocol.ReceiveMessage<ProgressToken>(cancellation);
        if (progressToken != null && !cancellation.IsCancellationRequested)
        {
            var progress = default(TP);
            if (_requestsQueue.TryGetValue(progressToken.Id, out var requestMessage) && requestMessage.Cancellation != null)
            {
                progress = progressHandler.GetProgress(progressToken, requestMessage.IsCompleted);
                _logger.LogDebug("send progress update for message {MessageId} to client with value {Progress}", progressToken.Id, progress.Progress);
                if (!progressToken.Active)
                {
                    _logger.LogDebug("requested to cancel requests execution for message {MessageId}", progressToken.Id);
                    requestMessage.Cancellation.Cancel();
                }
            }
            await protocol.TransferMessage(progressToken.Id, BufferSize, progress, cancellation);
        }
    }

    private async Task HandleReplyMessage(NamedPipeClientStream stream, CancellationToken cancellation)
    {
        var responseMessage = await _responseChannel.Reader.ReadAsync(cancellation);
        if (!cancellation.IsCancellationRequested)
        {
            await responseMessage.Action.Invoke(stream);
            _logger.LogDebug("message {MessageId} handling completed", responseMessage.Id);
        }
    }

    private async Task RunRequest<TReq, TRep>(IPipeMessageHandler<TReq, TRep> messageHandler, Guid id, TReq request, CancellationToken token)
    {
        TRep reply = default;
        RequestException replyError = default;
        try
        {
            Interlocked.Increment(ref _activeMessages);
            _logger.LogDebug("handling request for message {MessageId}", id);
            token.ThrowIfCancellationRequested();
            reply = await messageHandler.HandleRequest(id, request, token);
            _logger.LogDebug("handled request for message {MessageId}, sending reply back to client", id);
        }
        catch (OperationCanceledException e)
        {
            _logger.LogWarning("request execution cancelled for message {MessageId}, notify client", id);
            replyError = RequestException.CreateRequestException(e);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "request handler for message {MessageId} thrown unhandled error, sending error back to client", id);
            replyError = RequestException.CreateRequestException(e);
        }
        finally
        {
            Interlocked.Increment(ref _handledMessages);
            Interlocked.Decrement(ref _activeMessages);
        }
        var response = new PipeMessageResponse<TRep> { Reply = reply, ReplyError = replyError };
        await ScheduleResponseReply(id, response, token);
    }

    private async Task SendResponse<TRep>(Guid id, PipeMessageResponse<TRep> reply, Stream client, CancellationToken token)
    {
        try
        {
            _logger.LogDebug("sending reply for message {MessageId} back to client", id);
            var protocol = new PipeProtocol(client, _serializer);
            await protocol.TransferMessage(id, BufferSize, reply, token);
            _logger.LogDebug("sent reply for message {MessageId} back to client", id);
            _requestsQueue.TryRemove(id, out _);
        }
        catch (Exception e)
        {            
            if (reply.Reply != null) 
            {
                _logger.LogError(e, "error occurred while sending reply for message {MessageId} to the client, sending error back to client", id);
                var replyError = RequestException.CreateRequestException(e);
                var response = new PipeMessageResponse<TRep> { Reply = default, ReplyError = replyError };
                await ScheduleResponseReply(id, response, token);
            }
            else 
            {
                _logger.LogError(e, "unhandled error occurred while sending reply for message {MessageId} to the client", id);
                _requestsQueue.TryRemove(id, out _);
            }
        }        
    }

    private async Task ScheduleResponseReply<TRep>(Guid id, PipeMessageResponse<TRep> response, CancellationToken token)
    {
        _logger.LogDebug("scheduling reply for message {MessageId}", id);
        if (_requestsQueue.TryGetValue(id, out var requestMessage))
            requestMessage.IsCompleted = true;
        var responseMessage = new ResponseMessage
        {
            Id = id,
            Action = client => SendResponse(id, response, client, token)
        };
        await _responseChannel.Writer.WriteAsync(responseMessage, CancellationToken.None);
    }

    private void ExecuteRequest(object state)
    {
        var requestMessage = (RequestMessage)state;
        _ = ExecuteAsync(_serverTaskCancellation.Token);
        Interlocked.Decrement(ref _pendingMessages);

        async Task ExecuteAsync(CancellationToken token)
        {
            if (!_serverTaskCancellation.IsCancellationRequested)
            {
                try
                {                    
                    var cancellation = CancellationTokenSource.CreateLinkedTokenSource(requestMessage.Cancellation.Token, token);
                    await requestMessage.ExecuteAction.Invoke(requestMessage.Id, cancellation);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "unhandled error occurred while running client request {MessageId}", requestMessage.Id);
                }
            }
            else
            {
                _requestsQueue.TryRemove(requestMessage.Id, out _);
            }
        }
    }
}