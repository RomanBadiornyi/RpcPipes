namespace RpcPipes.PipeClient;

internal class PipeClientRequestHandle
{
    public Guid Id { get;  }
    public string RequestPipe { get; }
    public string HeartbeatPipe { get; }

    public SemaphoreSlim ReceiveHandle { get; set; }
    public SemaphoreSlim HeartbeatCheckHandle { get; set; }
    public DateTime HeartbeatCheckTime { get; set; }
    public TimeSpan HeartbeatCheckFrequency { get; set; }

    public Func<PipeProtocol, CancellationToken, Task> SendAction { get; set; }
    public Func<PipeProtocol, CancellationToken, Task> ReceiveAction { get; set; }

    public CancellationTokenSource RequestCancellation { get; set; }

    public int Retries { get; set; }
    public Exception Exception { get; set; }

    public PipeClientRequestHandle(Guid id, string requestPipe, string heartbeatPipe)
    {
        Id = id;
        RequestPipe = requestPipe;
        HeartbeatPipe = heartbeatPipe;
    }
}
