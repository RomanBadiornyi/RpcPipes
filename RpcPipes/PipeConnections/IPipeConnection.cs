using System.IO.Pipes;

namespace RpcPipes.PipeConnections;

public interface IPipeConnection
{
    int Id { get;}
    string Name { get; }
    bool InUse { get; }

    DateTime LastUsedAt { get; }

    bool VerifyIfConnected();
    bool VerifyIfExpired(DateTime currentTime);
    
    void Disconnect(string reason);
}

public interface IPipeConnection<T> : IPipeConnection
    where T : PipeStream
{
    ValueTask<(bool Connected, bool Dispatched, Exception Error)> UseConnection(
        Func<T, Task> useFunc, Func<IPipeConnection, bool> usePredicateFunc, CancellationToken cancellation);
}
