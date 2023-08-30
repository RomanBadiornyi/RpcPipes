namespace RpcPipes.PipeServer;

internal class PipeServerRequestHandle
{
    public Guid Id { get; set; }
    public CancellationTokenSource Cancellation { get; set; }
    public Func<Guid, CancellationTokenSource, Task> ExecuteAction { get; set; }
}
