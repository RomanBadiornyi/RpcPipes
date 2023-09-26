using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeExceptions;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeRequestOutHandler : IPipeMessageSender<PipeClientRequestMessage>
{
    private readonly static Meter _meter = new(nameof(PipeRequestOutHandler));
    private readonly static Counter<int> _sentMessagesCounter = _meter.CreateCounter<int>("sent-messages");

    private readonly ILogger _logger;
    private readonly string _pipe;
    private readonly PipeRequestHandler _requestHandler;
    private readonly Channel<PipeClientRequestMessage> _requestChannel;

    public Task<bool> ClientTask { get; }

    public PipeRequestOutHandler(
        ILogger logger,
        string pipe,
        PipeMessageDispatcher connectionPool,
        PipeRequestHandler requestHandler)
    {
        _logger = logger;
        _pipe = pipe;
        _requestHandler = requestHandler;
        _requestChannel =  Channel.CreateUnbounded<PipeClientRequestMessage>();
        ClientTask = connectionPool.ProcessClientMessages(_requestChannel, this)
            .ContinueWith(t => 
            {
                return t.IsCompleted;
            }, CancellationToken.None);
    }

    public async ValueTask Publish(PipeClientRequestMessage message)
    {
        await _requestChannel.Writer.WriteAsync(message);
    }

    public string TargetPipe(PipeClientRequestMessage message)
        => _pipe;

    public async Task HandleMessage(PipeClientRequestMessage message, PipeProtocol protocol, CancellationToken cancellation)
    {
        //since we pick message from the messages queue it could be picked up with some delay with respect to when it was actually triggered
        //during this delay client might decide to cancel request execution, if this is the case - don't call server and simply set error 
        if (message.RequestCancellation.IsCancellationRequested)
        {
            var requestCancelledError = new TaskCanceledException("Request execution has been cancelled");
            message.RequestTask.TrySetException(requestCancelledError);
        }
        else 
        {
            await message.SendAction.Invoke(protocol, cancellation);            
            await _requestHandler.RequestMessageSent(message);
            _sentMessagesCounter.Add(1);
            _logger.LogDebug("sent message {MessageId} for execution", message.Id);
        }        
    }

    public async ValueTask HandleError(PipeClientRequestMessage message, Exception error, CancellationToken cancellation)
    {
        if (error is PipeDataException || cancellation.IsCancellationRequested || error is PipeConnectionsExhausted)
            message.RequestTask.TrySetException(error);
        else
            await HandleSendMessageError(message, error);
    }

    private async ValueTask HandleSendMessageError(PipeClientRequestMessage requestMessage, Exception e)
    {
        requestMessage.Retries++;
        if (requestMessage.Retries < 3)
        {
            //push message back to the channel so we will attempt to retry sending it
            _logger.LogDebug(e, "retry sending request message {MessageId} due to error {ErrorMessage}", requestMessage.Id, e.Message);
            await Publish(requestMessage);            
        }
        else
        {
            requestMessage.RequestTask.TrySetException(e);
            _logger.LogError(e, "unable to send request message {MessageId} due to error {ErrorMessage}", requestMessage.Id, e.Message);
        }
    }    
}