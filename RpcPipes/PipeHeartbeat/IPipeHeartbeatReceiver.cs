namespace RpcPipes.PipeHeartbeat;

public interface IPipeHeartbeatReceiver<in TP>
    where TP : IPipeHeartbeat
{
    Task OnHeartbeatMessage(TP heartbeat);
}