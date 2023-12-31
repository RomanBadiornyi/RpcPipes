using Microsoft.Extensions.Logging;
using RpcPipes.PipeData;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal abstract class PipeHeartbeatInHandler : IPipeMessageReceiver
{
    public abstract string Pipe { get; }
    public abstract Task ServerTask { get; }
    
    public abstract Task<bool> ReceiveMessage(PipeProtocol protocol, CancellationToken cancellation);
}

internal class PipeHeartbeatInHandler<TP> : PipeHeartbeatInHandler
    where TP: class
{
    private readonly ILogger _logger;
    private readonly IPipeHeartbeatHandler<TP> _heartbeatHandler;
    private readonly IPipeMessageWriter _messageWriter;

    public override string Pipe { get; }
    public override Task ServerTask { get; }

    public PipeHeartbeatInHandler(
        ILogger logger,
        string pipe,
        PipeMessageDispatcher connectionPool,
        IPipeHeartbeatHandler<TP> heartbeatHandler,
        IPipeMessageWriter messageWriter)
    {
        _logger = logger;
        _heartbeatHandler = heartbeatHandler;
        _messageWriter = messageWriter;
        Pipe = pipe;
        ServerTask = connectionPool.ProcessServerMessages(this);
    }

    public override async Task<bool> ReceiveMessage(PipeProtocol protocol, CancellationToken cancellation)
    {
        PipeMessageHeartbeat<TP> pipeHeartbeat;
        var (pipeHeartbeatRequest, pipeHeartbeatRequestReceived) = await protocol.TryReceiveMessage(ReadHeartbeat, cancellation);
        if (pipeHeartbeatRequestReceived && pipeHeartbeatRequest != null && !cancellation.IsCancellationRequested)
        {
            pipeHeartbeat = _heartbeatHandler.HeartbeatMessage(pipeHeartbeatRequest.Id);
            if (_heartbeatHandler.TryGetMessageState(pipeHeartbeatRequest.Id, out var messageState))
            {
                if (!pipeHeartbeatRequest.Active)
                {
                    _logger.LogDebug("requested to cancel requests execution for message {MessageId}", pipeHeartbeatRequest.Id);
                    try
                    {
                        messageState.Cancellation.Cancel();
                    }
                    catch (ObjectDisposedException)
                    {
                        //can happen if between calling heartbeat - message execution completes, in this case do nothing
                    }
                }
                _logger.LogDebug("send heartbeat update for message {MessageId} to client with value {Progress}", pipeHeartbeatRequest.Id, pipeHeartbeat.Progress);
            }
            else
            {
                _logger.LogWarning("requested heartbeat for unknown message {MessageId}", pipeHeartbeatRequest.Id);
            }
            var heartbeatHeader = new PipeMessageHeader { MessageId = pipeHeartbeatRequest.Id };
            await protocol.TryTransferMessage(heartbeatHeader, WriteHeartbeat, cancellation);
            return false;
        }
        return true;

        async ValueTask<PipeRequestHeartbeat> ReadHeartbeat(PipeChunkReadStream stream, CancellationToken c)
            => await new PipeRequestHeartbeat().ReadFromStream(stream, c);

        async Task WriteHeartbeat(Stream stream, CancellationToken c)
            => await _messageWriter.WriteHeartbeat(pipeHeartbeat, stream, c);
    }
}