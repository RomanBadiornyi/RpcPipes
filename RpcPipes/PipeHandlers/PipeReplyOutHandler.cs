using System.Diagnostics.Metrics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeExceptions;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeReplyOutHandler
{
    private readonly static Meter Meter = new(nameof(PipeReplyOutHandler));
    private readonly static Counter<int> ReplyMessagesCounter = Meter.CreateCounter<int>("reply-messages");
    
    private readonly ILogger _logger;
    private readonly IPipeHeartbeatHandler _heartbeatHandler;

    private readonly Channel<PipeServerRequestMessage> _responseChannel = Channel.CreateUnbounded<PipeServerRequestMessage>();

    public Task ClientTask { get; }
    
    public PipeReplyOutHandler(
        ILogger logger, 
        PipeMessageDispatcher connectionPool,
        IPipeHeartbeatHandler heartbeatHandler)
    {
        _logger = logger;
        _heartbeatHandler = heartbeatHandler;
        ClientTask = connectionPool.ProcessClientMessages(_responseChannel, GetTargetPipeName, InvokeSendResponse);

        string GetTargetPipeName(PipeServerRequestMessage requestMessage)
            => requestMessage.Pipe;

        Task InvokeSendResponse(PipeServerRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
            => SendResponse(requestMessage, protocol, cancellation);        
    }

    public ValueTask PublishResponseMessage(PipeServerRequestMessage requestMessage)
    {
        _logger.LogDebug("scheduling reply for message {MessageId}", requestMessage.Id);
        ReplyMessagesCounter.Add(1);
        return _responseChannel.Writer.WriteAsync(requestMessage);
    }

    private async Task SendResponse(PipeServerRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        _logger.LogDebug("scheduled reply for message {MessageId}", requestMessage.Id);
        ReplyMessagesCounter.Add(-1);
        try
        {
            await requestMessage.SendResponse.Invoke(protocol, cancellation);            
            _heartbeatHandler.EndMessageHandling(requestMessage.Id);
            requestMessage.OnMessageCompleted.Invoke(null, true);
        }
        catch (OperationCanceledException)
        {            
        }
        catch (PipeDataException e)
        {
            ReportErrorOrComplete(requestMessage, e);            
        }
        catch (Exception e)
        {
            RetryOrComplete(requestMessage, e);
            throw;
        }
    }

    private void RetryOrComplete(PipeServerRequestMessage requestMessage, Exception e)
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
            PublishResponseMessage(requestMessage);
        }
    }

    private void ReportErrorOrComplete(PipeServerRequestMessage requestMessage, Exception e)
    {
        requestMessage.Retries += 1;
        if (requestMessage.Retries >= 3 || !requestMessage.ReportError(e))
        {
            //this means that we were not able to send error back to client, in this case simply drop message
            _heartbeatHandler.EndMessageHandling(requestMessage.Id);
            requestMessage.OnMessageCompleted.Invoke(e, false);
        }
    }

}