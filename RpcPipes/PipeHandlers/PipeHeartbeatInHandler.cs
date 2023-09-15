using Microsoft.Extensions.Logging;
using RpcPipes.PipeData;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeHeartbeatInHandler
{
    private readonly ILogger _logger;
    private readonly PipeMessageDispatcher _connectionPool;
    private readonly IPipeMessageWriter _messageWriter;

    public string Pipe { get; }

    public PipeHeartbeatInHandler(
        ILogger logger,
        string pipe,
        PipeMessageDispatcher connectionPool,
        IPipeMessageWriter messageWriter)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        _messageWriter = messageWriter;
        Pipe = pipe;
    }

    public Task Start<TP>(IPipeHeartbeatHandler<TP> heartbeatHandler)
        where TP: IPipeHeartbeat
    {
        return _connectionPool.ProcessServerMessages(Pipe, HeartbeatMessage);

        Task HeartbeatMessage(PipeProtocol protocol, CancellationToken cancellation)
            => HandleHeartbeatMessage(heartbeatHandler, protocol, cancellation);
    }

    private async Task HandleHeartbeatMessage<TP>(IPipeHeartbeatHandler<TP> heartbeatHandler, PipeProtocol protocol, CancellationToken cancellation)
        where TP: IPipeHeartbeat
    {
        TP pipeHeartbeat;
        var (pipeHeartbeatRequest, _) = await protocol.ReceiveMessage(ReadHeartbeat, cancellation);
        if (pipeHeartbeatRequest != null && !cancellation.IsCancellationRequested)
        {
            pipeHeartbeat = heartbeatHandler.HeartbeatMessage(pipeHeartbeatRequest.Id);
            if (heartbeatHandler.TryGetMessageCancellation(pipeHeartbeatRequest.Id, out var requestCancellation))
            {
                if (!pipeHeartbeatRequest.Active)
                {
                    _logger.LogDebug("requested to cancel requests execution for message {MessageId}", pipeHeartbeatRequest.Id);
                    try
                    {
                        requestCancellation.Cancel();
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
            await protocol.TransferMessage(heartbeatHeader, WriteHeartbeat, cancellation);
        }

        ValueTask<PipeRequestHeartbeat> ReadHeartbeat(Stream stream, CancellationToken c)
            => _messageWriter.ReadData<PipeRequestHeartbeat>(stream, c);

        Task WriteHeartbeat(Stream stream, CancellationToken c)
            => _messageWriter.WriteData(pipeHeartbeat, stream, c);
    }
}