using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeRequestInHandler : IPipeMessageReceiver
{
    private readonly static Meter Meter = new(nameof(PipeRequestInHandler));
    private readonly static Counter<int> PendingMessagesCounter = Meter.CreateCounter<int>("pending-messages");
    private readonly static Counter<int> ActiveMessagesCounter = Meter.CreateCounter<int>("active-messages");
    private readonly static Counter<int> HandledMessagesCounter = Meter.CreateCounter<int>("handled-messages");

    private readonly ILogger _logger;
    private readonly IPipeHeartbeatHandler _heartbeatHandler;
    private readonly IPipeHeartbeatReporter _heartbeatReporter;
    private readonly Func<PipeServerRequestMessage, bool> _setupRequest;

    public string Pipe { get; }
    public Task ServerTask { get; }

    public PipeRequestInHandler(
        ILogger logger,
        string pipe,
        PipeMessageDispatcher connectionPool,
        IPipeHeartbeatHandler heartbeatHandler,
        IPipeHeartbeatReporter heartbeatReporter,
        Func<PipeServerRequestMessage, bool> setupRequest)
    {
        _logger = logger;
        _heartbeatHandler = heartbeatHandler;
        _heartbeatReporter = heartbeatReporter;
        _setupRequest = setupRequest;
        Pipe = pipe;
        ServerTask = connectionPool.ProcessServerMessages(this);
    }

    public async Task<bool> ReceiveMessage(PipeProtocol protocol, CancellationToken cancellation)
    {
        PipeServerRequestMessage requestMessage = null;
        var header = await protocol
            .BeginReceiveMessageAsync((id, reply) => {
                //ensure we add current request to outstanding messages before we complete reading request payload
                //this way we ensure that when client starts doing heartbeat calls - we already can reply as we know about this message
                requestMessage = new PipeServerRequestMessage(id, reply);
                if (!_heartbeatHandler.StartMessageHandling(requestMessage.Id, cancellation, _heartbeatReporter))
                    requestMessage = null;
                else
                    _setupRequest.Invoke(requestMessage);
            }, cancellation);
        if (header != null && requestMessage != null)
        {
            if (await requestMessage.ReadRequest.Invoke(protocol, cancellation))
            {
                _logger.LogDebug("scheduling request execution for message {MessageId}", requestMessage.Id);
                PendingMessagesCounter.Add(1);
                ThreadPool.QueueUserWorkItem(ExecuteRequest, requestMessage);
            }
            return false;
        }
        return true;
    }

    private void ExecuteRequest(object state)
    {
        var requestMessage = (PipeServerRequestMessage)state;
        _ = ExecuteAsync();

        async Task ExecuteAsync()
        {
            PendingMessagesCounter.Add(-1);
            ActiveMessagesCounter.Add(1);
            _heartbeatHandler.TryGetMessageState(requestMessage.Id, out var messageState);
            try
            {
                await requestMessage.RunRequest.Invoke(messageState.Cancellation);
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