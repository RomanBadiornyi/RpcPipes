using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeHeartbeat;

public class PipeHeartbeatMessageHandler : PipeHeartbeatHandler<PipeState>
{
    protected override PipeMessageHeartbeat<PipeState> GetNotStartedHeartbeat()
        => new()
        { 
            Progress = 0, RequestState = new PipeState("NotStarted", "Task Not Started")
        };

    protected override PipeMessageHeartbeat<PipeState> GetCompletedHeartbeat()
        => new()
        { 
            Progress = 1, RequestState = new PipeState("Completed", "Task Completed")
        };
}