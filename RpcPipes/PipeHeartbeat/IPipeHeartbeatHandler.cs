namespace RpcPipes.PipeHeartbeat;

public interface IPipeHeartbeatHandler
{
    bool StartMessageHandling(Guid messageId, CancellationToken cancellation, IPipeHeartbeatReporter heartbeatReporter);    
    void StartMessageExecute(Guid messageId, object message);
    bool TryGetMessageCancellation(Guid messageId, out CancellationTokenSource cancellationSource);
    void EndMessageExecute(Guid messageId);
    bool EndMessageHandling(Guid messageId);
}

public interface IPipeHeartbeatHandler<out TOut> : IPipeHeartbeatHandler
    where TOut : IPipeHeartbeat
{

    TOut HeartbeatMessage(Guid messageId);
}
