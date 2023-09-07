namespace RpcPipes.PipeExceptions;

public class PipeNetworkException : Exception
{
    public PipeNetworkException(string message, Exception inner) : base(message, inner) {}
}
