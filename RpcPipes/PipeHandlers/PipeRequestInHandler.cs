using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeRequestInHandler
{
    private readonly static Meter Meter = new(nameof(PipeRequestInHandler));
    private readonly static Counter<int> PendingMessagesCounter = Meter.CreateCounter<int>("pending-messages");
    private readonly static Counter<int> ActiveMessagesCounter = Meter.CreateCounter<int>("active-messages");
    private readonly static Counter<int> HandledMessagesCounter = Meter.CreateCounter<int>("handled-messages");

    private readonly ILogger _logger;
    private readonly PipeMessageDispatcher _connectionPool;
    private readonly IPipeHeartbeatHandler _heartbeatHandler;

    public string Pipe { get; }

    public PipeRequestInHandler(
        ILogger logger,
        string pipe,
        PipeMessageDispatcher connectionPool,
        IPipeHeartbeatHandler heartbeatHandler)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        _heartbeatHandler = heartbeatHandler;
        Pipe = pipe;
    }

    public Task Start(IPipeHeartbeatReporter heartbeatReporter, Func<PipeServerRequestMessage, bool> setupRequest)
    {
        return _connectionPool.ProcessServerMessages(Pipe, ReceiveMessage);

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
                PendingMessagesCounter.Add(1);
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
            PendingMessagesCounter.Add(-1);
            ActiveMessagesCounter.Add(1);
            _heartbeatHandler.TryGetMessageCancellation(requestMessage.Id, out var requestCancellation);
            try
            {
                await requestMessage.RunRequest.Invoke(requestCancellation);
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception e)
            {
                _logger.LogError(e, "unhandled error occurred while running client request {MessageId}", requestMessage.Id);
            }
            finally
            {
                HandledMessagesCounter.Add(1);
                ActiveMessagesCounter.Add(-1);
            }
        }
    }
}