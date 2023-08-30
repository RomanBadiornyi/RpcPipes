using System.Collections.Concurrent;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeServer;

public class PipeServer<TP> : PipeConnectionManager
    where TP: IPipeProgress
{    
    private readonly ILogger<PipeServer<TP>> _logger;

    private readonly string _receivePipe;
    private readonly string _progressPipe;
    private readonly IPipeProgressHandler<TP> _progressHandler;
    private readonly IPipeMessageWriter _serializer;

    private int _pendingMessages;
    private int _activeMessages;
    private int _handledMessages;
    private int _replyMessages;    

    private int _activeConnections;
    private int _clientConnections;

    private readonly object _sync = new();
    private bool _started;
    private Task _serverTask;
    private CancellationTokenSource _serverTaskCancellation;

    private readonly ConcurrentDictionary<Guid, PipeServerRequestHandle> _requestsQueue = new();
    private readonly ConcurrentDictionary<string, MessageChannel<PipeServerResponseHandle>> _responseChannels = new();

    public PipeServer(ILogger<PipeServer<TP>> logger, string receivePipe, string progressPipe, int instances, IPipeProgressHandler<TP> progressHandler, IPipeMessageWriter serializer) :
        base(logger, instances, 4 * 1024, PipeOptions.Asynchronous | PipeOptions.WriteThrough)
    {
        _logger = logger;

        _receivePipe = receivePipe;
        _progressPipe = progressPipe;
        _progressHandler = progressHandler;
        _serializer = serializer;
        _serverTask = null;

        OnServerConnect = () => Interlocked.Increment(ref _activeConnections);
        OnServerDisconnect = () => Interlocked.Decrement(ref _activeConnections);

        OnClientConnect = () => Interlocked.Increment(ref _clientConnections);
        OnClientDisconnect = () => Interlocked.Decrement(ref _clientConnections);
    }

    public Task Start<TReq, TRep>(
        IPipeMessageHandler<TReq, TRep> messageHandler,
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
                    _handledMessages, _pendingMessages, _activeMessages, _replyMessages);
            }, null, 0, 30000);
            _serverTaskCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
            _serverTask = Task.WhenAll(
                StartServerListener(Instances, () => {
                    return RunServerMessageLoop(
                        _serverTaskCancellation.Token,
                        _receivePipe,
                        (s, c) => HandleReceiveMessage(messageHandler, s, c)
                    );
                }),
                StartServerListener(Instances, () => {
                    return RunServerMessageLoop(
                        _serverTaskCancellation.Token,
                        _progressPipe,
                        (s, c) => HandleProgressMessage(s, c)
                    );
                })
            )
            //wait also until we complete all client connections
            .ContinueWith(_ => Task.WhenAll(_responseChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray()))
            .ContinueWith(_ => {
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
        PipeServerRequestHandle requestMessage = null;

        var protocol = new PipeProtocol(stream, _serializer);
        var (messageId, replyPipe, bufferSize) = await protocol
            .BeginReceiveMessageAsync(id => {
                //ensure we add current request to outstanding messages before we complete reading request payload
                //this way we ensure that when client starts checking progress - we already can reply as we know about this message
                requestMessage = new PipeServerRequestHandle { Id = id };
                if (!_requestsQueue.TryAdd(id, requestMessage))
                    requestMessage = null;
                else
                    _progressHandler.StartMessageHandling(requestMessage.Id, messageHandler as IPipeProgressReporter<TP>);
            }, cancellation);
        if (messageId != null && replyPipe != null && requestMessage != null)
        {
            try
            {
                _logger.LogDebug("reading request message {MessageId}", requestMessage.Id);
                var request = await protocol.EndReceiveMessage<TReq>(messageId.Value, bufferSize, cancellation);
                _logger.LogDebug("read request message {MessageId}", requestMessage.Id);

                requestMessage.Cancellation = cancellationSource;
                requestMessage.ExecuteAction = (id, c) => RunRequest(messageHandler, id, replyPipe.Value, request, c.Token);

                _logger.LogDebug("scheduling request execution for message {MessageId}", requestMessage.Id);
                Interlocked.Increment(ref _pendingMessages);
                ThreadPool.QueueUserWorkItem(ExecuteRequest, requestMessage);
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "unable to consume message {MessageId} due to error, reply error back to client", requestMessage.Id);
                var response = _serializer.CreateResponseContainer<TRep>();
                response.SetRequestException(e);
                ScheduleResponseReply(requestMessage.Id, replyPipe.Value, response);
            }
        }
    }

    private async Task HandleProgressMessage(Stream stream, CancellationToken cancellation)
    {
        var protocol = new PipeProtocol(stream, _serializer);
        var progressToken = await protocol.ReceiveMessage<PipeProgressToken>(cancellation);
        if (progressToken != null && !cancellation.IsCancellationRequested)
        {
            var progress = default(TP);
            if (_requestsQueue.TryGetValue(progressToken.Id, out var requestMessage))
            {
                if (requestMessage.Cancellation != null)
                {
                    progress = _progressHandler.GetProgress(progressToken.Id);
                    _logger.LogDebug("send progress update for message {MessageId} to client with value {Progress}", progressToken.Id, progress.Progress);
                    if (!progressToken.Active)
                    {
                        _logger.LogDebug("requested to cancel requests execution for message {MessageId}", progressToken.Id);
                        requestMessage.Cancellation.Cancel();
                    }
                }
            }
            else
            {
                _logger.LogWarning("requested progress for unknown message {MessageId}", progressToken.Id);
            }
            await protocol.TransferMessage(progressToken.Id, BufferSize, progress, cancellation);
        }
    }

    private async Task RunRequest<TReq, TRep>(
        IPipeMessageHandler<TReq, TRep> messageHandler, Guid id, Guid replyPipe, TReq request, CancellationToken token)
    {
        var response = _serializer.CreateResponseContainer<TRep>();
        try
        {
            Interlocked.Increment(ref _activeMessages);
            _logger.LogDebug("handling request for message {MessageId}", id);
            token.ThrowIfCancellationRequested();
            _progressHandler.StartMessageExecute(id, request);
            response.Reply = await messageHandler.HandleRequest(request, token);
            _logger.LogDebug("handled request for message {MessageId}, sending reply back to client", id);
        }
        catch (OperationCanceledException e)
        {
            _logger.LogWarning("request execution cancelled for message {MessageId}, notify client", id);
            response.SetRequestException(e);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "request handler for message {MessageId} thrown unhandled error, sending error back to client", id);
            response.SetRequestException(e);
        }
        finally
        {
            Interlocked.Increment(ref _handledMessages);
            Interlocked.Decrement(ref _activeMessages);
            _progressHandler.EndMessageExecute(id);
        }
        ScheduleResponseReply(id, replyPipe, response);
    }

    private async Task SendResponse<TRep>(
        Guid id, Guid replyPipe, PipeMessageResponse<TRep> response, NamedPipeClientStream client, CancellationToken token)
    {
        Interlocked.Decrement(ref _replyMessages);
        try
        {
            _logger.LogDebug("sending reply for message {MessageId} back to client", id);
            var protocol = new PipeProtocol(client, _serializer);
            await protocol.TransferMessage(id, BufferSize, response, token);
            _logger.LogDebug("sent reply for message {MessageId} back to client", id);

            _progressHandler.EndMessageHandling(id);
            _requestsQueue.TryRemove(id, out _);
        }
        catch (IOException) when (client.IsConnected == false)
        {
            throw;
        }
        catch (Exception e)
        {
            if (response.Reply != null)
            {
                _logger.LogError(e, "error occurred while sending reply for message {MessageId} to the client, sending error back to client", id);
                response = _serializer.CreateResponseContainer<TRep>();
                response.SetRequestException(e);
                ScheduleResponseReply(id, replyPipe, response);
            }
            else
            {
                _logger.LogError(e, "unhandled error occurred while sending reply for message {MessageId} to the client", id);
                _progressHandler.EndMessageHandling(id);
                _requestsQueue.TryRemove(id, out _);
            }
        }
    }

    private void ScheduleResponseReply<TRep>(Guid id, Guid replyPipe, PipeMessageResponse<TRep> response)
    {
        Interlocked.Increment(ref _replyMessages);
        _logger.LogDebug("scheduling reply for message {MessageId}", id);
        var responseMessage = new PipeServerResponseHandle(id, replyPipe.ToString())
        {
            Action = (s, c) => SendResponse(id, replyPipe, response, s, c)
        };

        var pipeName = responseMessage.ReplyPipe;
        ProcessClientMessage(
            _responseChannels,
            pipeName,
            responseMessage,
            (r, s, c) => r?.Action.Invoke(s, c), _serverTaskCancellation.Token);
    }

    private void ExecuteRequest(object state)
    {
        var requestMessage = (PipeServerRequestHandle)state;
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
                _progressHandler.EndMessageHandling(requestMessage.Id);
                _requestsQueue.TryRemove(requestMessage.Id, out _);
            }
        }
    }
}