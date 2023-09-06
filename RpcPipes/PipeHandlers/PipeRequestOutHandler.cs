using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeRequestOutHandler    
{
    private static Meter _meter = new(nameof(PipeRequestOutHandler));
    private static Counter<int> _sentMessagesCounter = _meter.CreateCounter<int>("sent-messages");

    private ILogger _logger;
    private CancellationTokenSource _cancellation;
    
    private PipeConnectionManager _connectionPool;

    private readonly ConcurrentDictionary<string, PipeMessageChannel<PipeClientRequestMessage>> _requestChannels = new();

    public string PipeName { get; }
    public Task[] ChannelTasks => _requestChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray();

    public PipeRequestOutHandler(ILogger logger, string pipeName, PipeConnectionManager connectionPool, CancellationTokenSource cancellation)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        _cancellation = cancellation;
        PipeName = pipeName;
    }

    public void PublishRequestMessage(PipeClientRequestMessage requestMessage, Action<PipeClientRequestMessage> onMessageSend)
    {
        var messageQueueRequest = new PipeMessageChannelQueue<PipeClientRequestMessage>
        {
            MessageChannels = _requestChannels,
            Message = requestMessage
        };
        _connectionPool.ProcessClientMessage(PipeName, messageQueueRequest, InvokeHandleSendMessage, _cancellation.Token);

        Task InvokeHandleSendMessage(PipeClientRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
            => HandleSendMessage(onMessageSend, requestMessage, protocol, cancellation);

    }

    private async Task HandleSendMessage(Action<PipeClientRequestMessage> onMessageSend, PipeClientRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        using var sendCancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellation, requestMessage.RequestCancellation.Token);
        try
        {
            await requestMessage.SendAction.Invoke(protocol, sendCancellation.Token);

            _sentMessagesCounter.Add(1);
            onMessageSend.Invoke(requestMessage);
            _logger.LogDebug("sent message {MessageId} for execution", requestMessage.Id);

        }
        catch (Exception e)
        {
            HandleSendMessageError(onMessageSend, requestMessage, e);
        }
    }

    private void HandleSendMessageError(Action<PipeClientRequestMessage> onMessageSend, PipeClientRequestMessage requestMessage, Exception e)
    {
        requestMessage.Retries++;
        if (requestMessage.Retries < 3)
        {
            //push message back to the channel so we will attempt to retry sending it
            PublishRequestMessage(requestMessage, onMessageSend);
        }
        else
        {
            requestMessage.ReceiveTask.TrySetException(e);
            _logger.LogError(e, "unable to send message {MessageId} due to error", requestMessage.Id);
        }
    }
}