namespace RpcPipes.PipeHeartbeat;

public class PipeMessageState
{
    public CancellationTokenSource Cancellation { get; set; }
    
    public object RequestState { get; set; }
    public object Request { get; set; }

    public bool Started { get; set;}
    public bool Completed { get; set; }
}

internal class PipeHeartbeatMessageState<TOut> : PipeMessageState
    where TOut : IPipeHeartbeat
{
    public IPipeHeartbeatReporter<TOut> Reporter { get; set; }    
}

public interface IPipeHeartbeatHandler
{
    bool TryGetMessageState(Guid messageId, out PipeMessageState messageState);

    bool StartMessageHandling(Guid messageId, CancellationToken cancellation, IPipeHeartbeatReporter heartbeatReporter);    
    void StartMessageExecute(Guid messageId, object message);
    
    void EndMessageExecute(Guid messageId);
    bool EndMessageHandling(Guid messageId);
}

public interface IPipeHeartbeatHandler<out TOut> : IPipeHeartbeatHandler
    where TOut : IPipeHeartbeat
{

    TOut HeartbeatMessage(Guid messageId);
}
