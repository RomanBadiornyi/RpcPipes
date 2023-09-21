using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeExceptions;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeReplyOutHandler : IPipeMessageSender<PipeServerRequestMessage>
{
    private readonly static Meter Meter = new(nameof(PipeReplyOutHandler));
    private readonly static Counter<int> ReplyMessagesCounter = Meter.CreateCounter<int>("reply-messages");
    
    private readonly ILogger _logger;
    private readonly IPipeHeartbeatHandler _heartbeatHandler;
    private readonly Channel<PipeServerRequestMessage> _responseChannel;

    public Task ClientTask { get; }

    public PipeReplyOutHandler(
        ILogger logger, 
        PipeMessageDispatcher connectionPool,
        IPipeHeartbeatHandler heartbeatHandler)
    {
        _logger = logger;
        _heartbeatHandler = heartbeatHandler;
        _responseChannel = Channel.CreateUnbounded<PipeServerRequestMessage>();
        ClientTask = connectionPool.ProcessClientMessages(_responseChannel, this);
    }

    public async ValueTask Publish(PipeServerRequestMessage message)
    {
        _logger.LogDebug("scheduling reply for message {MessageId}", message.Id);
        ReplyMessagesCounter.Add(1);
        await _responseChannel.Writer.WriteAsync(message);
    }

    public string TargetPipe(PipeServerRequestMessage message)
        => message.Pipe;

    public async Task HandleMessage(PipeServerRequestMessage message, PipeProtocol protocol, CancellationToken cancellation)
    {
        _logger.LogDebug("scheduled reply for message {MessageId}", message.Id);        
        await message.SendResponse.Invoke(protocol, cancellation);            
        _heartbeatHandler.EndMessageHandling(message.Id);
        message.OnMessageCompleted.Invoke(null, true);        
        ReplyMessagesCounter.Add(-1);
    }

    public async ValueTask HandleError(PipeServerRequestMessage message, Exception error)
    {
        if (error is PipeDataException)
            await ReportErrorOrCompleteAsync(message, error);            
        else 
            await RetryOrComplete(message, error);
    }        

    private async ValueTask RetryOrComplete(PipeServerRequestMessage requestMessage, Exception e)
    {
        requestMessage.Retries += 1;
        if (requestMessage.Retries >= 3)
        {
            //we did retry 3 times, if still no luck - drop message
            _heartbeatHandler.EndMessageHandling(requestMessage.Id);
            requestMessage.OnMessageCompleted.Invoke(e, false);
            _logger.LogError(e, "unable to send message {MessageId} due to error", requestMessage.Id);
        }
        else
        {
            //publish to retry
            await Publish(requestMessage);
        }
    }

    private async Task ReportErrorOrCompleteAsync(PipeServerRequestMessage requestMessage, Exception e)
    {
        requestMessage.Retries += 1;
        if (requestMessage.Retries >= 3 || !await requestMessage.ReportError(e))
        {
            //this means that we were not able to send error back to client, in this case simply drop message
            _heartbeatHandler.EndMessageHandling(requestMessage.Id);
            requestMessage.OnMessageCompleted.Invoke(e, false);
        }
    }    
}