namespace RpcPipes.PipeMessages;

internal class PipeClientHeartbeatMessage : IPipeMessage
{
    public Guid Id { get;  }

    public CancellationToken RequestCancellation { get; set; }
    public bool RequestCompleted { get; set; }

    public CancellationTokenSource HeartbeatCancellation { get; set; }
    public SemaphoreSlim HeartbeatCheckHandle { get; set; }

    public bool HeartbeatStarted { get; set; }
    public TimeSpan HeartbeatCheckFrequency { get; set; }
    public DateTime HeartbeatCheckTime { get; set; }
    public bool HeartbeatForced { get; set; }


    public PipeClientHeartbeatMessage(Guid id)
    {
        Id = id;
    }
}