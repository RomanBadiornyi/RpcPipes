namespace RpcPipes.PipeServer;

internal class PipeServerResponseHandle
{
    public Guid Id { get; }
    public string ReplyPipe { get; }
    public Func<PipeProtocol, CancellationToken, Task> Action { get; set; }

    public PipeServerResponseHandle(Guid id, string replyPipe)
    {
        Id = id;
        ReplyPipe = replyPipe;
    }
}
