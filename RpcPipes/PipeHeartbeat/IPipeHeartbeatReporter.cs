namespace RpcPipes.PipeHeartbeat;

public interface IPipeHeartbeatReporter
{
}

public interface IPipeHeartbeatReporter<out TOut> : IPipeHeartbeatReporter
    where TOut : IPipeHeartbeat
{
    TOut HeartbeatMessage(object message);
}