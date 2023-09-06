namespace RpcPipes.PipeHeartbeat;

public interface IPipeHeartbeatHandler
{
    bool StartMessageHandling(
        Guid messageId, CancellationToken token, IPipeHeartbeatReporter heartbeatReporter);    
    void StartMessageExecute(Guid messageId, object message);
    bool TryGetMessageCancellation(Guid messageId, out CancellationTokenSource token);
    void EndMessageExecute(Guid messageId);
    bool EndMessageHandling(Guid messageId);
}

public interface IPipeHeartbeatHandler<TOut> : IPipeHeartbeatHandler
    where TOut : IPipeHeartbeat
{

    TOut HeartbeatMessage(Guid messageId);
}
