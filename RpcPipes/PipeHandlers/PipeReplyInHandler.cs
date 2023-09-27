using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeData;
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

    public async Task<bool> ReceiveMessage(PipeProtocol protocol, CancellationToken cancellation)
    {
        var message = await protocol.BeginReceiveMessage<PipeMessageHeader, PipeClientRequestMessage>(HeaderToMessage, cancellation);
        if (message != default)
        {
            await message.HeartbeatCheckHandle.WaitAsync(cancellation);
            try
            {
                //ensure we stop heartbeat task as soon as we started receiving reply
                message.RequestCompleted = true;
                _logger.LogDebug("received reply message {MessageId}, cancelled heartbeat updated", message.Id);
                await ReceiveMessage(message, protocol, cancellation);   
            }
            finally
            {
                message.HeartbeatCheckHandle.Release();
            }            
            return false;
        }
        return true;

        PipeClientRequestMessage HeaderToMessage(PipeMessageHeader header)
        {
            var message = _requestHandler.GetRequestMessageById(header.MessageId);
            if (message == null)
                _logger.LogWarning("received reply message {MessageId} not found in request queue", header.MessageId);
            return message;
        }
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
            requestMessage.RequestTask.TrySetException(new OperationCanceledException("Reply rejected due to dispose of client"));
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