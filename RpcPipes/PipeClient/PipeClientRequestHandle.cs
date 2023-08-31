using System.IO.Pipes;

namespace RpcPipes.PipeClient;

internal class PipeClientRequestHandle
{
    public Guid Id { get;  }
    public string RequestPipe { get; }
    public string ProgressPipe { get; }

    public SemaphoreSlim ReceiveHandle { get; set; }
    public SemaphoreSlim ProgressCheckHandle { get; set; }
    public DateTime ProgressCheckTime { get; set; }
    public TimeSpan ProgressCheckFrequency { get; set; }

    public Func<NamedPipeClientStream, CancellationToken, Task> SendAction { get; set; }
    public Func<NamedPipeServerStream, int, CancellationToken, Task> ReceiveAction { get; set; }

    public CancellationTokenSource RequestCancellation { get; set; }

    public int Retries { get; set; }
    public Exception Exception { get; set; }

    public PipeClientRequestHandle(Guid id, string requestPipe, string progressPipe)
    {
        Id = id;
        RequestPipe = requestPipe;
        ProgressPipe = progressPipe;
    }
}
