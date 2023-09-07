using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeExceptions;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeRequestOutHandler    
{
    private static Meter _meter = new(nameof(PipeRequestOutHandler));
    private static Counter<int> _sentMessagesCounter = _meter.CreateCounter<int>("sent-messages");

    private ILogger _logger;
    
    private PipeConnectionManager _connectionPool;

    private readonly ConcurrentDictionary<string, PipeMessageChannel<PipeClientRequestMessage>> _requestChannels = new();

    public string PipeName { get; }
    public Task[] ChannelTasks => _requestChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray();

    public PipeRequestOutHandler(ILogger logger, string pipeName, PipeConnectionManager connectionPool)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        PipeName = pipeName;
    }

    public void PublishRequestMessage(PipeClientRequestMessage requestMessage, Action<PipeClientRequestMessage> onMessageSend)
    {
        var messageQueueRequest = new PipeMessageChannelQueue<PipeClientRequestMessage>
        {
            MessageChannels = _requestChannels,
            Message = requestMessage
        };
        _connectionPool.ProcessClientMessage(PipeName, messageQueueRequest, InvokeHandleSendMessage);

        Task InvokeHandleSendMessage(PipeClientRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
            => HandleSendMessage(onMessageSend, requestMessage, protocol, cancellation);

    }

    private async Task HandleSendMessage(Action<PipeClientRequestMessage> onMessageSend, PipeClientRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        try
        {
            requestMessage.RequestCancellation.Token.ThrowIfCancellationRequested();
            //if we get to this point and request not cancelled we send message to server without interruption by passing global cancellation.
            await requestMessage.SendAction.Invoke(protocol, cancellation);
            _sentMessagesCounter.Add(1);
            onMessageSend.Invoke(requestMessage);
            _logger.LogDebug("sent message {MessageId} for execution", requestMessage.Id);
        }
        catch (OperationCanceledException)
        {
            requestMessage.ReceiveTask.TrySetCanceled();
            return;
        }
        catch (PipeDataException e)
        {
            requestMessage.ReceiveTask.SetException(e);
            return;
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