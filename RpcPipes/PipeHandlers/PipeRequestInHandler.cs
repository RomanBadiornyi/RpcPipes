using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeRequestInHandler
{
    private static Meter _meter = new(nameof(PipeRequestInHandler));
    private static Counter<int> _pendingMessagesCounter = _meter.CreateCounter<int>("pending-messages");
    private static Counter<int> _activeMessagesCounter = _meter.CreateCounter<int>("active-messages");
    private static Counter<int> _handledMessagesCounter = _meter.CreateCounter<int>("handled-messages");

    private ILogger _logger;
    private PipeMessageDispatcher _connectionPool;
    private IPipeHeartbeatHandler _heartbeatHandler;

    public string PipeName { get; }

    public PipeRequestInHandler(
        ILogger logger,
        string pipeName,
        PipeMessageDispatcher connectionPool,
        IPipeHeartbeatHandler heartbeatHandler)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        _heartbeatHandler = heartbeatHandler;
        PipeName = pipeName;
    }

    public Task Start(IPipeHeartbeatReporter heartbeatReporter, Func<PipeServerRequestMessage, bool> setupRequest)
    {
        return _connectionPool.ProcessServerMessages(PipeName, ReceiveMessage);

        Task ReceiveMessage(PipeProtocol protocol, CancellationToken cancellation)
            => HandleReceiveMessage(heartbeatReporter, setupRequest, protocol, cancellation);
    }

    private async Task HandleReceiveMessage(
        IPipeHeartbeatReporter heartbeatReporter, Func<PipeServerRequestMessage, bool> setupRequest, PipeProtocol protocol, CancellationToken cancellation)
    {
        PipeServerRequestMessage requestMessage = null;
        var header = await protocol
            .BeginReceiveMessageAsync((id, reply) => {
                //ensure we add current request to outstanding messages before we complete reading request payload
                //this way we ensure that when client starts doing heartbeat calls - we already can reply as we know about this message
                requestMessage = new PipeServerRequestMessage(id, reply);
                if (!_heartbeatHandler.StartMessageHandling(requestMessage.Id, cancellation, heartbeatReporter))
                    requestMessage = null;
                else
                    setupRequest.Invoke(requestMessage);
            }, cancellation);
        if (header != null && requestMessage != null)
        {
            if (await requestMessage.ReadRequest.Invoke(protocol, cancellation))
            {
                _logger.LogDebug("scheduling request execution for message {MessageId}", requestMessage.Id);
                _pendingMessagesCounter.Add(1);
                ThreadPool.QueueUserWorkItem(ExecuteRequest, requestMessage);
            }
        }
    }

    private void ExecuteRequest(object state)
    {
        var requestMessage = (PipeServerRequestMessage)state;
        _ = ExecuteAsync();

        async Task ExecuteAsync()
        {
            _pendingMessagesCounter.Add(-1);
            _activeMessagesCounter.Add(1);
            _heartbeatHandler.TryGetMessageCancellation(requestMessage.Id, out var requestCancellation);
            try
            {
                await requestMessage.RunRequest.Invoke(requestCancellation);
            }
            catch (OperationCanceledException)
            {
                return;
            }
            catch (Exception e)
            {
                _logger.LogError(e, "unhandled error occurred while running client request {MessageId}", requestMessage.Id);
            }
            finally
            {
                _handledMessagesCounter.Add(1);
                _activeMessagesCounter.Add(-1);
            }
        }
    }
}