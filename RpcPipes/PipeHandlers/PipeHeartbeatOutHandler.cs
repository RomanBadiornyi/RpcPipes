using System.Collections.Concurrent;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using RpcPipes.PipeData;
using RpcPipes.PipeExceptions;
using RpcPipes.PipeHeartbeat;
using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

internal abstract class PipeHeartbeatOutHandler : IPipeMessageSender<PipeClientHeartbeatMessage>
{
    public abstract Task<bool> ClientTask { get; }
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
    private readonly Channel<PipeClientHeartbeatMessage> _heartbeatsChannel;
    private readonly ConcurrentStack<PipeClientHeartbeatMessage> _heartbeatsPaused;
    private readonly Timer _heartbeatResumer;
    private readonly TimeSpan _heartbeatsPauseInterval;

    public override Task<bool> ClientTask { get; }

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
        _heartbeatsChannel = Channel.CreateUnbounded<PipeClientHeartbeatMessage>();

        _heartbeatsPauseInterval = TimeSpan.FromMilliseconds(100);
        _heartbeatsPaused = new ConcurrentStack<PipeClientHeartbeatMessage>();
        _heartbeatResumer = new Timer(_ => ResumeHeartbeat(connectionPool.Cancellation), this, 0, (int)_heartbeatsPauseInterval.TotalMilliseconds);

        ClientTask = Task.WhenAll(connectionPool.ProcessClientMessages(_heartbeatsChannel, this))
            .ContinueWith(t =>
            {                
                _heartbeatResumer.Dispose();
                _heartbeatsPaused.Clear();
                return t.IsCompleted;
            }, CancellationToken.None)
            .ContinueWith(t => t.Result, CancellationToken.None);
    }

    private void ResumeHeartbeat(CancellationToken cancellation)
    {
        while (!cancellation.IsCancellationRequested)
        {
            if (_heartbeatsPaused.TryPop(out var head))
            {
                var message = head;

                var itemsVerified = false;
                var itemMinWaitTime = _heartbeatsPauseInterval.TotalMilliseconds + 1;

                while (!itemsVerified && !cancellation.IsCancellationRequested)
                {
                    if (!ShouldDoHeartbeat(message, out var delay))
                    {
                        itemMinWaitTime = Math.Min(itemMinWaitTime, delay.TotalMilliseconds);
                        _heartbeatsPaused.Push(message);
                    }
                    else
                        _heartbeatsChannel.Writer.TryWrite(message);

                    //check if next item in stack not available or equal to original head - exit,
                    //so rest will be verified during next time iteration
                    if (_heartbeatsPaused.TryPeek(out message))
                    {
                        if (message == head)
                            itemsVerified = true;
                        else
                            itemsVerified = !_heartbeatsPaused.TryPop(out message);
                    }
                    else
                    {
                        itemsVerified = true;
                    }
                }
                //repeat check if there are items with wait time less than check interval
                //so that we check heartbeat for paused messages on time
                if (TimeSpan.FromMilliseconds(itemMinWaitTime) < _heartbeatsPauseInterval)
                {
                    continue;
                }
                else
                {
                    break;
                }
            }
            else
            {
                break;
            }
        }
    }

    public override async ValueTask Publish(PipeClientHeartbeatMessage message)
    {
        await _heartbeatsChannel.Writer.WriteAsync(message);
    }

    public override string TargetPipe(PipeClientHeartbeatMessage message)
        => _pipe;

    public override async Task HandleMessage(PipeClientHeartbeatMessage message, PipeProtocol protocol, CancellationToken cancellation)
    {
        if (cancellation.IsCancellationRequested || message.RequestCompleted)
            return;

        if (!ShouldDoHeartbeat(message, out _))
        {
            _heartbeatsPaused.Push(message);
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
            message.HeartbeatCheckTime = DateTime.UtcNow;
            message.HeartbeatForced = false;
            await TryRedoHeartbeat(message, cancellation);
        }
    }

    public override async ValueTask HandleError(PipeClientHeartbeatMessage message, Exception error, CancellationToken cancellation)
    {
        if (error is PipeConnectionsExhausted)
        {
            _logger.LogWarning("cancelling execution of request message {MessageId} due to heartbeat failure", message.Id);
            message.HeartbeatCancellation.Cancel();
        }
        else
        {
            if (!ShouldDoHeartbeat(message, out _))
            {
                _heartbeatsPaused.Push(message);
                return;
            }
            await TryRedoHeartbeat(message, cancellation);
        }
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
        var (pipeHeartbeat, received) =  await protocol.TryReceiveMessage(ReadHeartbeat, cancellation);
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
            else if (!heartbeatMessage.RequestCancellation.IsCancellationRequested)
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

    private bool ShouldDoHeartbeat(PipeClientHeartbeatMessage message, out TimeSpan delay)
    {
        if (message.HeartbeatForced || message.HeartbeatCheckTime == DateTime.MinValue)
            return true;
        delay = DateTime.UtcNow - message.HeartbeatCheckTime;
        if (delay > message.HeartbeatCheckFrequency)
            return true;
        return false;
    }
}