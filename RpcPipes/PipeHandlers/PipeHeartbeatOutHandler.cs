using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeData;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal abstract class PipeHeartbeatOutHandler : IPipeMessageSender<PipeClientHeartbeatMessage>
{
    public abstract Task ClientTask { get; }
    public abstract ValueTask Publish(PipeClientHeartbeatMessage message);

    public abstract string TargetPipe(PipeClientHeartbeatMessage message);
    public abstract Task HandleMessage(PipeClientHeartbeatMessage message, PipeProtocol protocol, CancellationToken cancellation);
    public abstract ValueTask HandleError(PipeClientHeartbeatMessage message, Exception error, CancellationToken cancellation);
}

internal class PipeHeartbeatOutHandler<TP> : PipeHeartbeatOutHandler
    where TP: IPipeHeartbeat
{
    private readonly ILogger _logger;
    public readonly string _pipe;
    private readonly IPipeMessageWriter _messageWriter;
    private readonly IPipeHeartbeatReceiver<TP> _heartbeatReceiver;
    private readonly Channel<PipeClientHeartbeatMessage> _heartBeatsChannel;

    public override Task ClientTask { get; }

    public PipeHeartbeatOutHandler(
        ILogger logger,
        string pipe,
        PipeMessageDispatcher connectionPool,
        IPipeHeartbeatReceiver<TP> heartbeatReceiver,
        IPipeMessageWriter messageWriter)
    {
        _logger = logger;
        _pipe = pipe;
        _messageWriter = messageWriter;
        _heartbeatReceiver = heartbeatReceiver;
        _heartBeatsChannel = Channel.CreateUnbounded<PipeClientHeartbeatMessage>();
        ClientTask = connectionPool.ProcessClientMessages(_heartBeatsChannel, this);
    }

    public override async ValueTask Publish(PipeClientHeartbeatMessage message)
    {
        await _heartBeatsChannel.Writer.WriteAsync(message);
    }

    public override string TargetPipe(PipeClientHeartbeatMessage message)
        => _pipe;

    public override async Task HandleMessage(PipeClientHeartbeatMessage message, PipeProtocol protocol, CancellationToken cancellation)
    {
        if (cancellation.IsCancellationRequested || message.RequestCompleted)
            return;

        if (!await ReadyForHeartbeat(message, cancellation))
        {
            await TryRedoHeartbeat(message, cancellation);
            return;
        }
        await message.HeartbeatCheckHandle.WaitAsync(cancellation);
        try
        {
            if (!message.RequestCompleted)
                await DoHeartbeat(message, protocol, cancellation);
        }
        finally
        {
            message.HeartbeatCheckHandle.Release();
            message.HeartbeatCheckTime = DateTime.Now;
            message.HeartbeatErrors = 0;
            await TryRedoHeartbeat(message, cancellation);
        }
    }

    public override async ValueTask HandleError(PipeClientHeartbeatMessage message, Exception error, CancellationToken cancellation)
    {
        message.HeartbeatErrors++;
        if (message.HeartbeatErrors > 10)
        {
            _logger.LogWarning("cancelling execution of request message {MessageId} due to heartbeat failure", message.Id);
            message.HeartbeatCancellation.Cancel();
        }
        else
        {
            if (!await ReadyForHeartbeat(message, cancellation))
            {
                await TryRedoHeartbeat(message, cancellation);
                return;
            }
        }
    }


    private static async Task<bool> ReadyForHeartbeat(PipeClientHeartbeatMessage heartbeatMessage, CancellationToken cancellation)
    {
        var lastCheckInterval = DateTime.Now - heartbeatMessage.HeartbeatCheckTime;
        if (lastCheckInterval < heartbeatMessage.HeartbeatCheckFrequency)
        {
            try
            {
                await Task.Delay(heartbeatMessage.HeartbeatCheckFrequency - lastCheckInterval, cancellation);
            }
            catch (OperationCanceledException)
            {
                return false;
            }
            return false;
        }
        return true;
    }

    private async Task TryRedoHeartbeat(PipeClientHeartbeatMessage heartbeatMessage, CancellationToken cancellation)
    {
        if (!heartbeatMessage.RequestCompleted && !cancellation.IsCancellationRequested)
            await Publish(heartbeatMessage);
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