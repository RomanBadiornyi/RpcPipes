namespace RpcPipes.PipeHeartbeat;

public interface IPipeHeartbeatReporter
{
}

public interface IPipeHeartbeatReporter<TOut> : IPipeHeartbeatReporter
{
    PipeMessageHeartbeat<TOut> HeartbeatMessage(object message);
}