namespace RpcPipes;

public interface IPipeHeartbeatReporter<out TOut>
    where TOut : IPipeHeartbeat
{
    TOut HeartbeatMessage(object message);
}