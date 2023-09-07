using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeData;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeHeartbeatOutHandler<TP>
    where TP: IPipeHeartbeat
{
    private ILogger _logger;

    private PipeConnectionManager _connectionPool;
    private IPipeMessageWriter _messageWriter;
    private IPipeHeartbeatReceiver<TP> _heartbeatReceiver;

    private readonly ConcurrentDictionary<string, PipeMessageChannel<PipeClientHeartbeatMessage>> _heartBeatsChannels = new();

    public string PipeName { get; }
    public Task[] ChannelTasks => _heartBeatsChannels.Values.Select(c => c.ChannelTask).Where(t => t != null).ToArray();

    public PipeHeartbeatOutHandler(ILogger logger, string pipeName, PipeConnectionManager connectionPool, IPipeHeartbeatReceiver<TP> heartbeatReceiver, IPipeMessageWriter messageWriter)
    {
        _logger = logger;
        _connectionPool = connectionPool;
        _messageWriter = messageWriter;
        _heartbeatReceiver = heartbeatReceiver;
        PipeName = pipeName;
    }

    public void PublishHeartbeatMessage(PipeClientHeartbeatMessage heartbeatMessage)
    {
        var messageQueueRequest = new PipeMessageChannelQueue<PipeClientHeartbeatMessage>
        {
            MessageChannels = _heartBeatsChannels,
            Message = heartbeatMessage
        };
        _connectionPool.ProcessClientMessage(PipeName, messageQueueRequest, HandleHeartbeatMessage);
    }

    private async Task HandleHeartbeatMessage(
        PipeClientHeartbeatMessage heartbeatMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        if (!await ReadyForHeartbeat(heartbeatMessage, cancellation))
        {
            TryRedoHeartbeat(heartbeatMessage, cancellation);
            return;
        }
        if (cancellation.IsCancellationRequested)
            return;
        await heartbeatMessage.HeartbeatCheckHandle.WaitAsync(cancellation);
        try
        {
            if (!heartbeatMessage.RequestCompleted)
                await DoHeartbeatAsync(heartbeatMessage, protocol, cancellation);
        }
        finally
        {
            heartbeatMessage.HeartbeatCheckHandle.Release();
            heartbeatMessage.HeartbeatCheckTime = DateTime.Now;
            TryRedoHeartbeat(heartbeatMessage, cancellation);
        }
    }

    private async Task<bool> ReadyForHeartbeat(PipeClientHeartbeatMessage heartbeatMessage, CancellationToken cancellation)
    {
        var lastCheckInterval = DateTime.Now - heartbeatMessage.HeartbeatCheckTime;
        if (lastCheckInterval < heartbeatMessage.HeartbeatCheckFrequency)
        {
            await Task.Delay(heartbeatMessage.HeartbeatCheckFrequency - lastCheckInterval, cancellation);
            return false;
        }
        return true;
    }

    private void TryRedoHeartbeat(PipeClientHeartbeatMessage heartbeatMessage, CancellationToken cancellation)
    {        
        if (!heartbeatMessage.RequestCompleted && !cancellation.IsCancellationRequested)
            PublishHeartbeatMessage(heartbeatMessage);            
    }

    private async Task DoHeartbeatAsync(PipeClientHeartbeatMessage heartbeatMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        var pipeHeartbeatRequest = new PipeRequestHeartbeat
        {
            Id = heartbeatMessage.Id,
            Active = !heartbeatMessage.RequestCancellation.IsCancellationRequested
        };

        var pipeMessageHeader = new PipeMessageHeader { MessageId = heartbeatMessage.Id };
        await protocol.TransferMessage(pipeMessageHeader, WriteHeartbeat, cancellation);
        var pipeHeartbeat = await protocol.ReceiveMessage(ReadHeartbeat, cancellation);

        if (pipeHeartbeat != null)
        {
            if (pipeHeartbeat.Progress > 0)
            {
                _logger.LogDebug("received heartbeat update for message {MessageId} with value {Progress}", heartbeatMessage.Id, pipeHeartbeat.Progress);
                await _heartbeatReceiver.OnHeartbeatMessage(pipeHeartbeat);
            }
        }
        else
        {
            _logger.LogWarning("cancelling execution of request message {MessageId} as it's not handled by the server", heartbeatMessage.Id);
            heartbeatMessage.RequestCancellation.Cancel();
        }

        Task WriteHeartbeat(Stream stream, CancellationToken cancellation)
            => _messageWriter.WriteData(pipeHeartbeatRequest, stream, cancellation);

        ValueTask<TP> ReadHeartbeat(Stream stream, CancellationToken cancellation)
            => _messageWriter.ReadData<TP>(stream, cancellation);
    }
}