namespace RpcPipes.PipeConnections;

public interface IPipeConnection
{
    int Id { get;}
    string Name { get; }

    bool Connected { get; }
    bool InUse { get; }
    bool Released { get; }

    DateTime LastUsedAt { get; }

    Task<bool> TryReleaseConnection(int timeoutMilliseconds, CancellationToken cancellation);
}

public interface IPipeConnection<T> : IPipeConnection
    where T : Stream
{    
    Task<(bool Used, Exception Error)> UseConnection(Func<T, Task> useFunc, CancellationToken cancellation);    
}
