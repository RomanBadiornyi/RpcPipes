namespace RpcPipes.PipeHeartbeat;

public interface IPipeHeartbeatReceiver<TP>
{
    Task OnHeartbeatMessage(PipeMessageHeartbeat<TP> heartbeat);
}