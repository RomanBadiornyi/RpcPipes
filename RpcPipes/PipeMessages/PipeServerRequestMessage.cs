using RpcPipes.PipeTransport;

namespace RpcPipes.PipeMessages;

internal class PipeServerRequestMessage : IPipeMessage
{
    public Guid Id { get;  }
    public string ReplyPipe { get;set; }

    public int Retries { get; set; }
    
    public Func<PipeProtocol, CancellationToken, Task<bool>> ReadRequest { get; set; }
    public Func<CancellationTokenSource, Task> RunRequest { get; set; }    
    public Func<PipeProtocol, CancellationToken, Task> SendResponse { get; set; }
    public Func<Exception, bool> ReportError { get; set; }
    public Action<Exception, bool> OnMessageCompleted { get; set; }

    public PipeServerRequestMessage(Guid id, string reply)
    {
        Id = id;
        ReplyPipe = reply;
    }
}
