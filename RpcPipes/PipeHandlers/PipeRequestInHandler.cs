using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeData;
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
    private readonly Func<PipeServerRequestMessage, CancellationToken, bool> _onMessageReceived;
    private readonly Func<PipeServerRequestMessage, CancellationTokenSource> _getMessageCancellation;

    public string Pipe { get; }
    public Task ServerTask { get; }

    public PipeRequestInHandler(
        ILogger logger,
        string pipe,
        PipeMessageDispatcher connectionPool,
        Func<PipeServerRequestMessage, CancellationToken, bool> onMessageReceived,
        Func<PipeServerRequestMessage, CancellationTokenSource> getMessageCancellation)
    {
        _logger = logger;        
        _onMessageReceived = onMessageReceived;
        _getMessageCancellation = getMessageCancellation;
        Pipe = pipe;
        ServerTask = connectionPool.ProcessServerMessages(this);
    }

    public async Task<bool> ReceiveMessage(PipeProtocol protocol, CancellationToken cancellation)
    {
        var message = await protocol.BeginReceiveMessage<PipeAsyncMessageHeader, PipeServerRequestMessage>(HeaderToMessage, cancellation);
        if (message != default)
        {
            if (await message.ReadRequest.Invoke(protocol, cancellation))
            {
                _logger.LogDebug("scheduling request execution for message {MessageId}", message.Id);
                PendingMessagesCounter.Add(1);
                ThreadPool.QueueUserWorkItem(ExecuteRequest, message);
            }
            return false;
        }
        return true;

        PipeServerRequestMessage HeaderToMessage(PipeAsyncMessageHeader header)
        {
            //ensure we add current request to outstanding messages before we complete reading request payload
            //this way we ensure that when client starts doing heartbeat calls - we already can reply as we know about this message
            var message = new PipeServerRequestMessage(header.MessageId, header.ReplyPipe);
            if (!_onMessageReceived.Invoke(message, cancellation))
                return default;
            return message;
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
            try
            {
                var cancellationSource = _getMessageCancellation.Invoke(requestMessage);
                await requestMessage.RunRequest.Invoke(cancellationSource);
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