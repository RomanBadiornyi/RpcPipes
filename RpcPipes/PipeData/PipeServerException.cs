namespace RpcPipes.PipeData;

public class PipeServerException : Exception
{
    public string ClassName { get; }
    public override string StackTrace { get; }

    public PipeServerException(PipeMessageException exception) : 
        base(exception.Message, exception.InnerException != null ? new PipeServerException(exception.InnerException) : null)
    {
        ClassName = exception.ClassName;
        StackTrace = string.Join(Environment.NewLine, exception.StackTrace);
    }
}