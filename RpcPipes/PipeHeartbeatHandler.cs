using System.Collections.Concurrent;

namespace RpcPipes;

public abstract class PipeHeartbeatHandler<TOut> : IPipeHeartbeatHandler<TOut>
    where TOut : IPipeHeartbeat
{
    private readonly ConcurrentDictionary<Guid, IPipeHeartbeatReporter<TOut>> _heartbeatReporterById = new();
    private readonly ConcurrentDictionary<Guid, object> _messageIdByHandle = new();

    protected abstract TOut GetNotStartedHeartbeat();
    protected abstract TOut GetCompletedHeartbeat();

    public void StartMessageHandling(Guid messageId, IPipeHeartbeatReporter<TOut> heartbeatReporter)
    {
        _heartbeatReporterById.TryAdd(messageId, heartbeatReporter);
    }

    public void StartMessageExecute(Guid messageId, object message)
    {
        _messageIdByHandle.TryAdd(messageId, message);
    }

    public TOut HeartbeatMessage(Guid messageId)
    {
        if (_heartbeatReporterById.TryGetValue(messageId, out var heartbeatReporter))
        {
            if (heartbeatReporter != null && _messageIdByHandle.TryGetValue(messageId, out var message))
            {
                var heartbeat = heartbeatReporter.HeartbeatMessage(message);
                if (heartbeat == null)
                    return GetCompletedHeartbeat();
                return heartbeat;
            }
            return GetNotStartedHeartbeat();
        }
        else
        {
            return default;
        }
    }

    public void EndMessageExecute(Guid messageId)
    {
        _messageIdByHandle.TryRemove(messageId, out _);        
    }

    public void EndMessageHandling(Guid messageId)
    {
        _heartbeatReporterById.TryRemove(messageId, out _);
    }
}