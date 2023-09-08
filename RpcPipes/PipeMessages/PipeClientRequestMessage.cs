using RpcPipes.PipeTransport;

namespace RpcPipes.PipeMessages;

internal class PipeClientRequestMessage : PipeClientHeartbeatMessage
{
    public TaskCompletionSource<bool> RequestTask { get; set; }

    public Func<PipeProtocol, CancellationToken, Task> SendAction { get; set; }
    public Func<PipeProtocol, CancellationToken, Task> ReceiveAction { get; set; }    

    public int Retries { get; set; }

    public PipeClientRequestMessage(Guid id) : base(id)
    {
    }
}