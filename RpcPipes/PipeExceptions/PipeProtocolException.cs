namespace RpcPipes.PipeExceptions;

public class PipeProtocolException : PipeNetworkException
{
    public PipeProtocolException(string message, Exception inner) : base(message, inner) {}
}