using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeData;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal class PipeHeartbeatOutHandler<TP>
    where TP: IPipeHeartbeat
{
    private readonly ILogger _logger;

    private readonly IPipeMessageWriter _messageWriter;
    private readonly IPipeHeartbeatReceiver<TP> _heartbeatReceiver;

    private readonly Channel<PipeClientHeartbeatMessage> _heartBeatsChannel = Channel.CreateUnbounded<PipeClientHeartbeatMessage>();

    public string Pipe { get; }
    public Task ClientTask { get; }

    public PipeHeartbeatOutHandler(ILogger logger, string pipe, PipeMessageDispatcher connectionPool, IPipeHeartbeatReceiver<TP> heartbeatReceiver, IPipeMessageWriter messageWriter)
    {
        _logger = logger;
        _messageWriter = messageWriter;
        _heartbeatReceiver = heartbeatReceiver;

        Pipe = pipe;
        ClientTask = connectionPool.ProcessClientMessages(_heartBeatsChannel, GetTargetPipeName, HandleHeartbeatMessage);
    }

    public ValueTask PublishHeartbeatMessage(PipeClientHeartbeatMessage heartbeatMessage)
        => _heartBeatsChannel.Writer.WriteAsync(heartbeatMessage);

    private string GetTargetPipeName(PipeClientHeartbeatMessage heartbeatMessage)
        => Pipe;
        
    private async Task HandleHeartbeatMessage(
        PipeClientHeartbeatMessage heartbeatMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        if (!await ReadyForHeartbeat(heartbeatMessage, cancellation))
        {
            await TryRedoHeartbeat(heartbeatMessage, cancellation);
            return;
        }
        if (cancellation.IsCancellationRequested)
            return;
        await heartbeatMessage.HeartbeatCheckHandle.WaitAsync(cancellation);
        try
        {
            if (!heartbeatMessage.RequestCompleted)
                await DoHeartbeat(heartbeatMessage, protocol, cancellation);
        }
        finally
        {
            heartbeatMessage.HeartbeatCheckHandle.Release();
            heartbeatMessage.HeartbeatCheckTime = DateTime.Now;
            await TryRedoHeartbeat(heartbeatMessage, cancellation);
        }
    }

    private static async Task<bool> ReadyForHeartbeat(PipeClientHeartbeatMessage heartbeatMessage, CancellationToken cancellation)
    {
        var lastCheckInterval = DateTime.Now - heartbeatMessage.HeartbeatCheckTime;
        if (lastCheckInterval < heartbeatMessage.HeartbeatCheckFrequency)
        {
            await Task.Delay(heartbeatMessage.HeartbeatCheckFrequency - lastCheckInterval, cancellation);
            return false;
        }
        return true;
    }

    private async Task TryRedoHeartbeat(PipeClientHeartbeatMessage heartbeatMessage, CancellationToken cancellation)
    {        
        if (!heartbeatMessage.RequestCompleted && !cancellation.IsCancellationRequested)
            await PublishHeartbeatMessage(heartbeatMessage);            
    }

    private async Task DoHeartbeat(PipeClientHeartbeatMessage heartbeatMessage, PipeProtocol protocol, CancellationToken cancellation)
    {
        var pipeHeartbeatRequest = new PipeRequestHeartbeat
        {
            Id = heartbeatMessage.Id,
            Active = !heartbeatMessage.RequestCancellation.IsCancellationRequested
        };

        var pipeMessageHeader = new PipeMessageHeader { MessageId = heartbeatMessage.Id };
        await protocol.TransferMessage(pipeMessageHeader, WriteHeartbeat, cancellation);
        var (pipeHeartbeat, received) =  await protocol.ReceiveMessage(ReadHeartbeat, cancellation);
        if (received)
        {
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
                heartbeatMessage.HeartbeatCancellation.Cancel();            
            }
        }        

        Task WriteHeartbeat(Stream stream, CancellationToken c)
            => _messageWriter.WriteData(pipeHeartbeatRequest, stream, c);

        ValueTask<TP> ReadHeartbeat(Stream stream, CancellationToken c)
            => _messageWriter.ReadData<TP>(stream, c);
    }
}