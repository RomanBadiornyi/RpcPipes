namespace RpcPipes.PipeMessages;

internal class PipeClientHeartbeatMessage 
{
    public Guid Id { get;  }

    public SemaphoreSlim HeartbeatCheckHandle { get; set; }    

    public TimeSpan HeartbeatCheckFrequency { get; set; }
    public DateTime HeartbeatCheckTime { get; set; }

    public CancellationTokenSource RequestCancellation { get; set; }
    public bool RequestCompleted { get; set; }
    
    public PipeClientHeartbeatMessage(Guid id)
    {
        Id = id;
    }
}