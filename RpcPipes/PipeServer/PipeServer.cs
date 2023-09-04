using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeServer;

public class PipeServer<TP> : PipeConnectionManager
    where TP: IPipeHeartbeat
{
    private readonly ILogger<PipeServer<TP>> _logger;

    private static Meter _meter = new("PipeServer");
    private static Counter<int> _serverConnectionsCounter = _meter.CreateCounter<int>("server.server-connections");
    private static Counter<int> _clientConnectionsCounter = _meter.CreateCounter<int>("server.client-connections");
    private static Counter<int> _pendingMessagesCounter = _meter.CreateCounter<int>("server.pending-messages");
    private static Counter<int> _activeMessagesCounter = _meter.CreateCounter<int>("server.active-messages");
    private static Counter<int> _handledMessagesCounter = _meter.CreateCounter<int>("server.handled-messages");
    private static Counter<int> _replyMessagesCounter = _meter.CreateCounter<int>("server.reply-messages");

    private readonly string _receivePipe;
    private readonly string _heartbeatPipe;
    private readonly IPipeHeartbeatHandler<TP> _heartbeatHandler;
    private readonly IPipeMessageWriter _messageWriter;

    private readonly object _sync = new();
    private bool _started;
    private Task _serverTask;
    private CancellationTokenSource _serverTaskCancellation;

    private readonly ConcurrentDictionary<Guid, PipeServerRequestHandle> _requestsQueue = new();
    private readonly ConcurrentDictionary<string, PipeMessageChannel<PipeServerResponseHandle>> _responseChannels = new();

    public PipeServer(ILogger<PipeServer<TP>> logger, string receivePipe, string heartbeatPipe, int instances, IPipeHeartbeatHandler<TP> heartbeatHandler, IPipeMessageWriter messageWriter) :
        base(logger, instances, 1 * 1024, 4 * 1024, PipeOptions.Asynchronous | PipeOptions.WriteThrough)
    {
        _logger = logger;

        _receivePipe = receivePipe;
        _heartbeatPipe = heartbeatPipe;
        _heartbeatHandler = heartbeatHandler;
        _messageWriter = messageWriter;
        _serverTask = null;

        OnServerConnect = () => _serverConnectionsCounter.Add(1);
        OnServerDisconnect = () => _serverConnectionsCounter.Add(-1);

        OnClientConnect = () => _clientConnectionsCounter.Add(1);
        OnClientDisconnect = () => _clientConnectionsCounter.Add(-1);
    }

    public Task Start<TReq, TRep>(
        IPipeMessageHandler<TReq, TRep> messageHandler,
        CancellationToken token)
    {
        lock (_sync)
        {
            if (_started)
                return _serverTask;
            _serverTaskCancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
            _serverTask = Task
                .WhenAll(
                    StartServerListener(
                        Instances,
                        taskAction: () => RunServerMessageLoop(
                            _serverTaskCancellation.Token,
                            _receivePipe,
                            action: (s, c) => HandleReceiveMessage(messageHandler, s, c)
                        )),
                    StartServerListener(
                        Instances,
                        taskAction: () => RunServerMessageLoop(
                            _serverTaskCancellation.Token,
                            _heartbeatPipe,
                            action: HandleHeartbeatMessage
                        )
                    )
                )
                //wait also until we complete all client connections
                .ContinueWith(_ => Task.WhenAll(_responseChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray()), CancellationToken.None);

            _started = true;
            return _serverTask;
        }
    }

    private async Task HandleReceiveMessage<TReq, TRep>(IPipeMessageHandler<TReq, TRep> messageHandler, PipeProtocol protocol, CancellationToken cancellation)
    {
        PipeServerRequestHandle requestMessage = null;

        var header = await protocol
            .BeginReceiveMessageAsync(id => {
                //ensure we add current request to outstanding messages before we complete reading request payload
                //this way we ensure that when client starts doing heartbeat calls - we already can reply as we know about this message
                requestMessage = new PipeServerRequestHandle { Id = id };
                if (!_requestsQueue.TryAdd(id, requestMessage))
                    requestMessage = null;
                else
                    _heartbeatHandler.StartMessageHandling(requestMessage.Id, messageHandler as IPipeHeartbeatReporter<TP>);
            }, cancellation);
        if (header != null && requestMessage != null)
        {
            try
            {
                _logger.LogDebug("reading request message {MessageId}", requestMessage.Id);
                var pipeRequest = await protocol.EndReceiveMessage(header.MessageId, _messageWriter.ReadRequest<TReq>, cancellation);

                _logger.LogDebug("read request message {MessageId}", requestMessage.Id);

                requestMessage.Deadline = pipeRequest.Deadline;
                requestMessage.ExecuteAction = (id, c)
                    => RunRequest(messageHandler, id, header.ReplyPipe, pipeRequest.Request, c.Token);

                _logger.LogDebug("scheduling request execution for message {MessageId}", requestMessage.Id);

                _pendingMessagesCounter.Add(1);
                ThreadPool.QueueUserWorkItem(ExecuteRequest, requestMessage);
            }
            catch (Exception e)
            {
                _logger.LogWarning(e, "unable to consume message {MessageId} due to error, reply error back to client", requestMessage.Id);
                var pipeResponse = new PipeMessageResponse<TRep>();
                pipeResponse.SetRequestException(e);
                ScheduleResponseReply(requestMessage.Id, header.ReplyPipe, pipeResponse);
            }
        }
    }

    private async Task HandleHeartbeatMessage(PipeProtocol protocol, CancellationToken cancellation)
    {
        var pipeHeartbeat = default(TP);
        var pipeHeartbeatRequest = await protocol.ReceiveMessage(ReadHeartbeat, cancellation);        
        if (pipeHeartbeatRequest != null && !cancellation.IsCancellationRequested)
        {
            var heartbeatHeader = new PipeMessageHeader { MessageId = pipeHeartbeatRequest.Id };
            if (_requestsQueue.TryGetValue(pipeHeartbeatRequest.Id, out var requestMessage))
            {
                pipeHeartbeat = _heartbeatHandler.HeartbeatMessage(pipeHeartbeatRequest.Id);
                _logger.LogDebug("send heartbeat update for message {MessageId} to client with value {Progress}", pipeHeartbeatRequest.Id, pipeHeartbeat.Progress);
                if (!pipeHeartbeatRequest.Active)
                {
                    var requestCancellation = requestMessage.Cancellation;
                    if (requestCancellation != null)
                    {
                        _logger.LogDebug("requested to cancel requests execution for message {MessageId}", pipeHeartbeatRequest.Id);
                        try
                        {
                            requestCancellation.Cancel();
                        }
                        catch (ObjectDisposedException)
                        {
                            //
                        }
                    }
                }
            }
            else
            {
                _logger.LogWarning("requested heartbeat for unknown message {MessageId}", pipeHeartbeatRequest.Id);
            }
            await protocol.TransferMessage(heartbeatHeader, WriteHeartbeat, cancellation);
        }

        ValueTask<PipeRequestHeartbeat> ReadHeartbeat(Stream stream, CancellationToken token)
            => _messageWriter.ReadData<PipeRequestHeartbeat>(stream, token);        

        Task WriteHeartbeat(Stream stream, CancellationToken token)
            => _messageWriter.WriteData(pipeHeartbeat, stream, token);
    }

    private async Task RunRequest<TReq, TRep>(
        IPipeMessageHandler<TReq, TRep> messageHandler, Guid id, string replyPipe, TReq request, CancellationToken token)
    {
        var pipeResponse = new PipeMessageResponse<TRep>();
        try
        {
            _logger.LogDebug("handling request for message {MessageId}", id);
            token.ThrowIfCancellationRequested();

            _activeMessagesCounter.Add(1);                        
            _heartbeatHandler.StartMessageExecute(id, request);
            
            pipeResponse.Reply = await messageHandler.HandleRequest(request, token);
            _logger.LogDebug("handled request for message {MessageId}, sending reply back to client", id);
        }
        catch (OperationCanceledException e)
        {
            _logger.LogWarning("request execution cancelled for message {MessageId}, notify client", id);
            pipeResponse.SetRequestException(e);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "request handler for message {MessageId} thrown unhandled error, sending error back to client", id);
            pipeResponse.SetRequestException(e);
        }
        finally
        {
            _handledMessagesCounter.Add(1);
            _activeMessagesCounter.Add(-1);
            _heartbeatHandler.EndMessageExecute(id);
        }
        ScheduleResponseReply(id, replyPipe, pipeResponse);
    }

    private async Task SendResponse<TRep>(
        Guid id, string replyPipe, PipeMessageResponse<TRep> pipeResponse, PipeProtocol protocol, CancellationToken token)
    {
        _replyMessagesCounter.Add(-1);
        try
        {
            _logger.LogDebug("sending reply for message {MessageId} back to client", id);
            await protocol.TransferMessage(
                new PipeMessageHeader { MessageId = id },
                (s, c) => _messageWriter.WriteResponse(pipeResponse, s, c),
                token);
            _logger.LogDebug("sent reply for message {MessageId} back to client", id);

            _heartbeatHandler.EndMessageHandling(id);
            _requestsQueue.TryRemove(id, out _);
        }
        catch (IOException) when (protocol.Connected == false)
        {
            //force re-connect - as connection was dropped
            throw;
        }
        catch (InvalidDataException)
        {
            //force re-connect, possibly corrupted connection with the client
            throw;
        }
        catch (InvalidOperationException e)
        {
            _logger.LogError(e, "client rejected reply for message {MessageId} to the client", id);
            _heartbeatHandler.EndMessageHandling(id);
            _requestsQueue.TryRemove(id, out _);
            //drop message and force re-connect, possibly corrupted connection with the client
            throw;
        }
        catch (Exception e)
        {
            if (pipeResponse.Reply != null && pipeResponse.ReplyError == null)
            {
                //consider it as serialization error and try to reply error instead to the client
                _logger.LogError(e, "error occurred while sending reply for message {MessageId} to the client, sending error back to client", id);
                pipeResponse = new PipeMessageResponse<TRep>();
                pipeResponse.SetRequestException(e);
                ScheduleResponseReply(id, replyPipe, pipeResponse);
            }
            else
            {
                //this means that we were not able to send error back to client, in this case simply drop message
                _logger.LogError(e, "unhandled error occurred while sending reply for message {MessageId} to the client", id);
                _heartbeatHandler.EndMessageHandling(id);
                _requestsQueue.TryRemove(id, out _);
                //and throw exception to force re-connect
                throw;
            }
        }
    }

    private void ScheduleResponseReply<TRep>(Guid id, string replyPipe, PipeMessageResponse<TRep> pipeResponse)
    {
        _logger.LogDebug("scheduling reply for message {MessageId}", id);
        _replyMessagesCounter.Add(1);
        var responseMessage = new PipeServerResponseHandle(id, replyPipe)
        {
            Action = (p, c) => SendResponse(id, replyPipe, pipeResponse, p, c)
        };

        ProcessClientMessage(_responseChannels, responseMessage.ReplyPipe, responseMessage, InvokeSendResponse, _serverTaskCancellation.Token);

        static Task InvokeSendResponse(PipeServerResponseHandle response, PipeProtocol protocol, CancellationToken cancellation)
            => response?.Action.Invoke(protocol, cancellation);
    }

    private void ExecuteRequest(object state)
    {
        var requestMessage = (PipeServerRequestHandle)state;
        _ = ExecuteAsync(_serverTaskCancellation.Token);

        async Task ExecuteAsync(CancellationToken token)
        {
            _pendingMessagesCounter.Add(-1);
            if (!_serverTaskCancellation.IsCancellationRequested)
            {
                try
                {
                    using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
                    requestMessage.Cancellation = cancellation;
                    if (requestMessage.Deadline.TotalMilliseconds > 0)
                        cancellation.CancelAfter(requestMessage.Deadline);
                    await requestMessage.ExecuteAction.Invoke(requestMessage.Id, requestMessage.Cancellation);
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "unhandled error occurred while running client request {MessageId}", requestMessage.Id);
                }
                finally
                {
                    requestMessage.Cancellation = null;
                }
            }
            else
            {
                _heartbeatHandler.EndMessageHandling(requestMessage.Id);
                _requestsQueue.TryRemove(requestMessage.Id, out _);
            }
        }
    }
}