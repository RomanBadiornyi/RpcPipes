using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeReplyInHandler
{
    private static readonly Meter Meter = new(nameof(PipeReplyInHandler));
    private static readonly Counter<int> ReceivedMessagesCounter = Meter.CreateCounter<int>("received-messages");
    
    private readonly ILogger _logger;
    private readonly PipeMessageDispatcher _connectionPool;

    public string Pipe { get; }

    public PipeReplyInHandler(ILogger logger, string pipe, PipeMessageDispatcher connectionPool)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        Pipe = pipe;
    }

    public Task Start(Func<Guid, PipeClientRequestMessage> requestProvider)
    {
        return _connectionPool.ProcessServerMessages(Pipe, InvokeReceiveMessage);

        Task InvokeReceiveMessage(PipeProtocol protocol, CancellationToken cancellation)
            => HandleReceiveMessage(requestProvider, protocol, cancellation);
    }

    private async Task HandleReceiveMessage(Func<Guid, PipeClientRequestMessage> requestProvider, PipeProtocol protocol, CancellationToken cancellation)
    {
        PipeClientRequestMessage requestMessage = null;
        var header = await protocol
            .BeginReceiveMessage(id => { requestMessage = TryMarkMessageAsCompleted(id, requestProvider); }, cancellation);
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

    private PipeClientRequestMessage TryMarkMessageAsCompleted(Guid id, Func<Guid, PipeClientRequestMessage> requestProvider)
    {
        //ensure we stop heartbeat task as soon as we started receiving reply
        var requestMessage = requestProvider.Invoke(id);
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