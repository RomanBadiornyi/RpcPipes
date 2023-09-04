namespace RpcPipes;

public interface IPipeHeartbeatHandler<TOut>
    where TOut : IPipeHeartbeat
{
    void StartMessageHandling(Guid messageId, IPipeHeartbeatReporter<TOut> heartbeatReporter);    
    void StartMessageExecute(Guid messageId, object message);

    TOut HeartbeatMessage(Guid messageId);

    void EndMessageExecute(Guid messageId);
    void EndMessageHandling(Guid messageId);
}
