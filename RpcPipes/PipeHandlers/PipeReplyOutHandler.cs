using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeReplyOutHandler
{
    private static Meter _meter = new(nameof(PipeReplyOutHandler));
    private static Counter<int> _replyMessagesCounter = _meter.CreateCounter<int>("reply-messages");
    
    private ILogger _logger;
    private CancellationTokenSource _cancellation;

    private PipeConnectionManager _connectionPool;
    private IPipeHeartbeatHandler _heartbeatHandler;

    private readonly ConcurrentDictionary<string, PipeMessageChannel<PipeServerRequestMessage>> _responseChannels = new();    

    public Task[] ChannelTasks => _responseChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray();
    
    public PipeReplyOutHandler(
        ILogger logger, 
        PipeConnectionManager connectionPool,
        IPipeHeartbeatHandler heartbeatHandler, 
        CancellationTokenSource cancellation)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        _heartbeatHandler = heartbeatHandler;
        _cancellation = cancellation;
    }

    public void PublishResponseMessage(PipeServerRequestMessage requestMessage)
    {
        _logger.LogDebug("scheduling reply for message {MessageId}", requestMessage.Id);
        _replyMessagesCounter.Add(1);
        var messageQueueRequest = new PipeMessageChannelQueue<PipeServerRequestMessage> 
        { 
            MessageChannels = _responseChannels, 
            Message = requestMessage 
        };
        _connectionPool.ProcessClientMessage(requestMessage.ReplyPipe, messageQueueRequest, InvokeSendResponse, _cancellation.Token);

        Task InvokeSendResponse(PipeServerRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
            => SendResponse(requestMessage, protocol, cancellation);
    }

    private async Task SendResponse(PipeServerRequestMessage requestMessage, PipeProtocol protocol, CancellationToken token)
    {
        _replyMessagesCounter.Add(-1);
        try
        {
            await requestMessage.SendResponse.Invoke(protocol, token);            
            _heartbeatHandler.EndMessageHandling(requestMessage.Id);
            requestMessage.OnMessageCompleted.Invoke(null, true);
        }
        catch (IOException e) when (protocol.Connected == false)
        {
            _logger.LogError(e, "connection got interrupted while sending reply for message {MessageId} to the client", requestMessage.Id);            
            RetryOrComplete(requestMessage, e);
            throw;
        }
        catch (InvalidDataException e)
        {
            _logger.LogError(e, "client incorrectly acknowledged receiving reply for message {MessageId} to the client", requestMessage.Id);            
            RetryOrComplete(requestMessage, e);
            throw;
        }
        catch (InvalidOperationException e)
        {
            _logger.LogError(e, "client did not acknowledge receiving reply for message {MessageId} to the client", requestMessage.Id);            
            RetryOrComplete(requestMessage, e);
            throw;
        }
        catch (Exception e)
        {            
            ReportErrorOrComplete(requestMessage, e);            
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
            _logger.LogError(e, "unhandled error occurred while sending reply for message {MessageId} to the client", requestMessage.Id);
            _heartbeatHandler.EndMessageHandling(requestMessage.Id);
            requestMessage.OnMessageCompleted.Invoke(e, false);
        }
    }

}