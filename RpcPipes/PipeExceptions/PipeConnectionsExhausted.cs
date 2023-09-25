namespace RpcPipes.PipeExceptions;

public class PipeConnectionsExhausted : Exception
{
    public PipeConnectionsExhausted(string name, int instances, Exception lastError) : 
        base($"Connection pool {name} of {instances} connections run out of available connections", lastError)
    {
    }
}