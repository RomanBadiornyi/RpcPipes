using System.Collections.Concurrent;

namespace RpcPipes.PipeHeartbeat;

public abstract class PipeHeartbeatHandler<TOut> : IPipeHeartbeatHandler<TOut>
    where TOut : IPipeHeartbeat
{
    private readonly ConcurrentDictionary<Guid, (IPipeHeartbeatReporter<TOut> Reporter, CancellationTokenSource Cancellation)> _messageHandleByMessageId = new();
    private readonly ConcurrentDictionary<Guid, object> _messageByMessageId = new();

    protected abstract TOut GetNotStartedHeartbeat();
    protected abstract TOut GetCompletedHeartbeat();

    public bool StartMessageHandling(Guid messageId, CancellationToken token, IPipeHeartbeatReporter heartbeatReporter)
    {
        var cancellationSource = CancellationTokenSource.CreateLinkedTokenSource(token);
        return _messageHandleByMessageId.TryAdd(messageId, (heartbeatReporter as IPipeHeartbeatReporter<TOut>, cancellationSource));
    }

    public void StartMessageExecute(Guid messageId, object message)
    {
        _messageByMessageId.TryAdd(messageId, message);
    }

    public bool TryGetMessageCancellation(Guid messageId, out CancellationTokenSource cancellation)
    {
        if (_messageHandleByMessageId.TryGetValue(messageId, out var handle))
        {
            cancellation = handle.Cancellation;
            return true;
        }
        cancellation = null;
        return false;
    }    

    public virtual TOut HeartbeatMessage(Guid messageId)
    {
        if (_messageHandleByMessageId.TryGetValue(messageId, out var handle))
        {
            var heartbeatReporter = handle.Reporter; 
            if (heartbeatReporter != null && _messageByMessageId.TryGetValue(messageId, out var message))
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
        _messageByMessageId.TryRemove(messageId, out _);        
    }

    public bool EndMessageHandling(Guid messageId)
    {
        if (_messageHandleByMessageId.TryRemove(messageId, out var handle)) 
        {
            handle.Cancellation.Dispose();
            return true;
        }
        return false;
    }
}