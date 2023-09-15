using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeHeartbeat;

public class PipeHeartbeatMessageHandler : PipeHeartbeatHandler<PipeHeartbeatMessage>
{
    protected override PipeHeartbeatMessage GetNotStartedHeartbeat()
    {
        return new PipeHeartbeatMessage(0, string.Empty);
    }

    protected override PipeHeartbeatMessage GetCompletedHeartbeat()
    {
        return new PipeHeartbeatMessage(1, string.Empty);
    }    
}