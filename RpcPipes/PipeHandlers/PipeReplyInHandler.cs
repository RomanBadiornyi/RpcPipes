using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeReplyInHandler : IPipeMessageReceiver
{
    private static readonly Meter Meter = new(nameof(PipeReplyInHandler));
    private static readonly Counter<int> ReceivedMessagesCounter = Meter.CreateCounter<int>("received-messages");
    
    private readonly ILogger _logger;
    private readonly PipeRequestHandler _requestHandler;

    public string Pipe { get; }
    public Task ServerTask { get; }

    public PipeReplyInHandler(
        ILogger logger, 
        string pipe, 
        PipeMessageDispatcher connectionPool, 
        PipeRequestHandler requestHandler)
    {
        _logger = logger;
        _requestHandler = requestHandler;
        Pipe = pipe;
        ServerTask = connectionPool.ProcessServerMessages(this);
    }

    public async Task ReceiveMessage(PipeProtocol protocol, CancellationToken cancellation)
    {
        PipeClientRequestMessage requestMessage = null;
        var header = await protocol
            .BeginReceiveMessage(id => { requestMessage = TryMarkMessageAsCompleted(id, _requestHandler); }, cancellation);
        if (header != null && requestMessage != null)
        {
            await requestMessage.HeartbeatCheckHandle.WaitAsync(cancellation);
            try
            {
                await ReceiveMessage(requestMessage, protocol, cancellation);   
            }
            finally
            {
                requestMessage.HeartbeatCheckHandle.Release();
            }            
        }
    }

    private PipeClientRequestMessage TryMarkMessageAsCompleted(Guid id, PipeRequestHandler requestHandler)
    {
        //ensure we stop heartbeat task as soon as we started receiving reply
        var requestMessage = requestHandler.GetRequestMessageById(id);
        if (requestMessage != null)
        {
            requestMessage.RequestCompleted = true;
            _logger.LogDebug("received reply message {MessageId}, cancelled heartbeat updated", requestMessage.Id);
        }
        else
        {
            _logger.LogWarning("received reply message {MessageId} not found in request queue", id);
        }
        return requestMessage;
    }

    private async Task ReceiveMessage(PipeClientRequestMessage requestMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        try
        {
            await requestMessage.ReceiveAction.Invoke(protocol, cancellation);
            requestMessage.RequestTask.TrySetResult(true);
        }
        catch (OperationCanceledException)
        {
            requestMessage.RequestTask.TrySetCanceled();
        }
        catch (Exception e)
        {
            requestMessage.RequestTask.TrySetException(e);
        }
        finally
        {                
            ReceivedMessagesCounter.Add(1);
            _logger.LogDebug("completed processing of reply message {MessageId}", requestMessage.Id);                
        }
    }    
}