namespace RpcPipes.PipeExceptions;

public class PipeProtocolException : Exception
{
    public PipeProtocolException(string message, Exception inner) : base(message, inner) {}
}
