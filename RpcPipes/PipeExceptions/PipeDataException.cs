namespace RpcPipes.PipeExceptions;

public class PipeDataException : Exception
{
    public PipeDataException(string message, Exception inner) : base(message, inner) {}
}
