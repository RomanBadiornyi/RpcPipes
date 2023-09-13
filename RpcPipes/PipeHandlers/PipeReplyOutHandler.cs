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
    private static Meter _meter = new(nameof(PipeReplyOutHandler));
    private static Counter<int> _replyMessagesCounter = _meter.CreateCounter<int>("reply-messages");
    
    private ILogger _logger;

    private PipeMessageDispatcher _connectionPool;
    private IPipeHeartbeatHandler _heartbeatHandler;

    private readonly Channel<PipeServerRequestMessage> _responseChannel = Channel.CreateUnbounded<PipeServerRequestMessage>();

    public Task ClientTask { get; private set;} = Task.CompletedTask;
    
    public PipeReplyOutHandler(
        ILogger logger, 
        PipeMessageDispatcher connectionPool,
        IPipeHeartbeatHandler heartbeatHandler)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        _heartbeatHandler = heartbeatHandler;
        ClientTask = connectionPool.ProcessClientMessages(_responseChannel, GetTargetPipeName, InvokeSendResponse);

        string GetTargetPipeName(PipeServerRequestMessage requestMessage)
            => requestMessage.PipeName;

        Task InvokeSendResponse(PipeServerRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
            => SendResponse(requestMessage, protocol, cancellation);        
    }

    public ValueTask PublishResponseMessage(PipeServerRequestMessage requestMessage)
    {
        _logger.LogDebug("scheduling reply for message {MessageId}", requestMessage.Id);
        _replyMessagesCounter.Add(1);
        return _responseChannel.Writer.WriteAsync(requestMessage);
    }

    private async Task SendResponse(PipeServerRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        _logger.LogDebug("scheduled reply for message {MessageId}", requestMessage.Id);
        _replyMessagesCounter.Add(-1);
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