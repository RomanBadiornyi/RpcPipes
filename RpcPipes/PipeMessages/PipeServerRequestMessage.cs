using RpcPipes.PipeTransport;

namespace RpcPipes.PipeMessages;

internal class PipeServerRequestMessage : IPipeMessage
{
    public Guid Id { get;  }
    public string Pipe { get;set; }

    public int Retries { get; set; }
    
    public Func<PipeProtocol, CancellationToken, Task<bool>> ReadRequest { get; set; }
    public Func<CancellationTokenSource, Task> RunRequest { get; set; }    
    public Func<PipeProtocol, CancellationToken, Task> SendResponse { get; set; }
    public Func<Exception, ValueTask<bool>> ReportError { get; set; }

    public PipeServerRequestMessage(Guid id, string pipe)
    {
        Id = id;
        Pipe = pipe;
    }
}
