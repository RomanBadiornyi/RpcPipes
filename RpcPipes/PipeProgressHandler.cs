using System.Collections.Concurrent;
using RpcPipes;

public abstract class PipeProgressHandler<TOut> : IPipeProgressHandler<TOut>
    where TOut : IPipeProgress
{
    private readonly ConcurrentDictionary<Guid, IPipeProgressReporter<TOut>> _progressReporterById = new();
    private readonly ConcurrentDictionary<Guid, object> _messageIdByHandle = new();

    protected abstract TOut GetNotStartedProgress();
    protected abstract TOut GetCompletedProgress();

    public void StartMessageHandling(Guid messageId, IPipeProgressReporter<TOut> progressReporter)
    {
        _progressReporterById.TryAdd(messageId, progressReporter);
    }

    public void StartMessageExecute(Guid messageId, object message)
    {
        _messageIdByHandle.TryAdd(messageId, message);
    }

    public TOut GetProgress(Guid messageId)
    {
        if (_progressReporterById.TryGetValue(messageId, out var progressReporter))
        {
            if (_messageIdByHandle.TryGetValue(messageId, out var message))
            {
                var progress = progressReporter.GetProgress(message);
                if (progress == null)
                    return GetCompletedProgress();
                return progress;
            }
            return GetNotStartedProgress();
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
        _progressReporterById.TryRemove(messageId, out _);
    }
}