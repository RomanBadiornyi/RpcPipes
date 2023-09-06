namespace RpcPipes.PipeData;

public class PipeMessageException
{
    public string ClassName { get; set; }
    public string Message { get; set; }
    public PipeMessageException InnerException { get; set; }
    public List<string> StackTrace { get; set; }

    public Exception ToException() 
        => new PipeServerException(this);
}
