using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeReplyInHandler
{
    private static Meter _meter = new(nameof(PipeReplyInHandler));
    private static Counter<int> _receivedMessagesCounter = _meter.CreateCounter<int>("received-messages");
    
    private ILogger _logger;
    
    private PipeConnectionManager _connectionPool;
    private CancellationTokenSource _cancellation;

    public string PipeName { get; }

    public PipeReplyInHandler(ILogger logger, string pipeName, PipeConnectionManager connectionPool, CancellationTokenSource cancellation)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        _cancellation = cancellation;
        PipeName = pipeName;
    }

    public Task Start(Func<Guid, PipeClientRequestMessage> requestProvider)
    {
        return _connectionPool.ProcessServerMessages(PipeName, ReceiveMessage, _cancellation.Token);

        Task ReceiveMessage(PipeProtocol protocol, CancellationToken cancellation)
            => HandleReceiveMessage(requestProvider, protocol, cancellation);
    }

    private async Task HandleReceiveMessage(Func<Guid, PipeClientRequestMessage> requestProvider, PipeProtocol protocol, CancellationToken cancellation)
    {
        PipeClientRequestMessage requestMessage = null;
        var header = await protocol
            .BeginReceiveMessage(id => { requestMessage = TryMarkMessageAsCompleted(id, requestProvider); }, cancellation);
        if (header != null && requestMessage != null)
        {
            await requestMessage.HeartbeatCheckHandle.WaitAsync(requestMessage.RequestCancellation.Token);
            try
            {
                await ReceiveMessage(requestMessage, protocol);   
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

    private async Task ReceiveMessage(PipeClientRequestMessage requestMessage, PipeProtocol protocol)
    {
        try
        {
            await requestMessage.ReceiveAction.Invoke(protocol, requestMessage.RequestCancellation.Token);
            requestMessage.ReceiveTask.TrySetResult(true);
        }
        catch (OperationCanceledException)
        {
            requestMessage.ReceiveTask.TrySetCanceled();
        }
        catch (Exception e)
        {
            requestMessage.ReceiveTask.TrySetException(e);
        }
        finally
        {                
            _receivedMessagesCounter.Add(1);
            _logger.LogDebug("completed processing of reply message {MessageId}", requestMessage.Id);                
        }
    }
}