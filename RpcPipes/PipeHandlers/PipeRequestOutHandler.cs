using System.Diagnostics.Metrics;
using System.Threading.Channels;
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
    
    private PipeMessageDispatcher _connectionPool;

    private readonly Channel<PipeClientRequestMessage> _requestChannel = Channel.CreateUnbounded<PipeClientRequestMessage>();

    public string PipeName { get; }
    public Task ClientTask { get; private set; } = Task.CompletedTask;

    public PipeRequestOutHandler(ILogger logger, string pipeName, PipeMessageDispatcher connectionPool, Func<PipeClientRequestMessage, ValueTask> onMessageSend)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        PipeName = pipeName;
        ClientTask = _connectionPool.ProcessClientMessages(_requestChannel, GetTargetPipeName, InvokeHandleSendMessage);

        string GetTargetPipeName(PipeClientRequestMessage requestMessage)
            => PipeName;

        Task InvokeHandleSendMessage(PipeClientRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
            => HandleSendMessage(onMessageSend, requestMessage, protocol, cancellation);
    }

    public ValueTask PublishRequestMessage(PipeClientRequestMessage requestMessage)
        => _requestChannel.Writer.WriteAsync(requestMessage);

    private async Task HandleSendMessage(Func<PipeClientRequestMessage, ValueTask> onMessageSend, PipeClientRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        try
        {
            requestMessage.RequestCancellation.ThrowIfCancellationRequested();            
            //if we get to this point and request not cancelled we send message to server without interruption by passing global cancellation.
            await requestMessage.SendAction.Invoke(protocol, cancellation);
            _sentMessagesCounter.Add(1);
            await onMessageSend.Invoke(requestMessage);
            _logger.LogDebug("sent message {MessageId} for execution", requestMessage.Id);
        }
        catch (OperationCanceledException e)
        {
            requestMessage.RequestTask.TrySetException(e);
            return;
        }
        catch (PipeDataException e)
        {
            requestMessage.RequestTask.TrySetException(e);
            return;
        }
        catch (Exception e)
        {
            await HandleSendMessageError(requestMessage, e);
        }
    }

    private async Task HandleSendMessageError(PipeClientRequestMessage requestMessage, Exception e)
    {
        requestMessage.Retries++;
        if (requestMessage.Retries < 3)
        {
            //push message back to the channel so we will attempt to retry sending it
            await PublishRequestMessage(requestMessage);
        }
        else
        {
            requestMessage.RequestTask.TrySetException(e);
            _logger.LogError(e, "unable to send message {MessageId} due to error", requestMessage.Id);
        }
    }
}