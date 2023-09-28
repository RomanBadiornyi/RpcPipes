using System.Collections.Concurrent;

namespace RpcPipes.PipeHeartbeat;

public abstract class PipeHeartbeatHandler<TOut> : IPipeHeartbeatHandler<TOut>
{
    private readonly ConcurrentDictionary<Guid, PipeHeartbeatMessageState<TOut>> _messageHandleByMessageId = new();

    protected abstract PipeMessageHeartbeat<TOut> GetNotStartedHeartbeat();
    protected abstract PipeMessageHeartbeat<TOut> GetCompletedHeartbeat();

    public bool StartMessageHandling(Guid messageId, CancellationToken token, IPipeHeartbeatReporter heartbeatReporter)
    {
        return _messageHandleByMessageId.TryAdd(messageId, new PipeHeartbeatMessageState<TOut>
        {
            Cancellation = CancellationTokenSource.CreateLinkedTokenSource(token),
            Reporter = heartbeatReporter as IPipeHeartbeatReporter<TOut>,
            Completed = false
        });
    }

    public void StartMessageExecute(Guid messageId, object message)
    {
        if (_messageHandleByMessageId.TryGetValue(messageId, out var messageSate))
        {
            messageSate.Request = message;
            messageSate.Started = true;
        }
    }

    public bool TryGetMessageState(Guid messageId, out PipeMessageState messageState)
    {
        if (_messageHandleByMessageId.TryGetValue(messageId, out var heartbeatMessageState))
        {
            messageState = heartbeatMessageState;
            return true;
        }
        messageState = null;
        return false;
    }

    public virtual PipeMessageHeartbeat<TOut> HeartbeatMessage(Guid messageId)
    {
        if (_messageHandleByMessageId.TryGetValue(messageId, out var handle))
        {
            var heartbeatReporter = handle.Reporter;
            var request = handle.Request;
            if (heartbeatReporter != null && request != null)
            {
                var heartbeat = heartbeatReporter.HeartbeatMessage(request);
                if (heartbeat != null)
                    return heartbeat;
            }
            if (handle.Started)
                return GetCompletedHeartbeat();
            return GetNotStartedHeartbeat();
        }
        else
        {
            return default;
        }
    }

    public void EndMessageExecute(Guid messageId)
    {
        if (_messageHandleByMessageId.TryGetValue(messageId, out var handle))
        {
            handle.Request = null;
        }
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