using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeHeartbeat;

public class PipeHeartbeatMessageHandler : PipeHeartbeatHandler<HeartbeatMessage>
{
    protected override HeartbeatMessage GetNotStartedHeartbeat()
    {
        return new HeartbeatMessage(0, string.Empty);
    }

    protected override HeartbeatMessage GetCompletedHeartbeat()
    {
        return new HeartbeatMessage(1, string.Empty);
    }    
}