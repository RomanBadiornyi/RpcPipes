using System.IO.Pipes;
using RpcPipes.PipeExceptions;

namespace RpcPipes.PipeConnections;

public abstract class PipeConnection<T> : IPipeConnection
    where T: PipeStream
{
    protected T Connection;
    protected volatile bool Connected;
    protected volatile bool Used;

    public int Id { get; }
    public string Name { get; }

    public bool InUse => Used;

    public DateTime LastUsedAt { get; private set; }
    public TimeSpan ConnectionExpiryTime { get; }

    public virtual int ConnectionErrors { get; private set; }

    protected PipeConnection(int id, string name, TimeSpan connectionExpiryTime)
    {
        Id = id;
        Name = name;
        ConnectionExpiryTime = connectionExpiryTime;
        LastUsedAt = DateTime.UtcNow;
    }

    public async ValueTask<(bool Connected, bool Dispatched, Exception Error)> UseConnection(
        Func<T, Task> useFunc,
        Func<IPipeConnection, bool> usePredicateFunc,
        CancellationToken cancellation)
    {
        var retries = 0;
        var maxRetries = 1;
        var shouldRetry = true;
        var shouldDispatch = false;
        Exception error = null;

        while (shouldRetry)
        {
            try
            {
                shouldRetry = false;
                Used = true;

                shouldDispatch = usePredicateFunc == null || usePredicateFunc.Invoke(this);
                LastUsedAt = DateTime.UtcNow;

                (var connected, error) = await TryConnect(cancellation);
                if (error != null)
                    ConnectionErrors += 1;
                else
                    ConnectionErrors = 0;

                if  (connected && shouldDispatch)
                {
                    LastUsedAt = DateTime.UtcNow;
                    await useFunc.Invoke(Connection);
                    LastUsedAt = DateTime.UtcNow;
                    return (VerifyIfConnected(), true, error);
                }
            }
            //connection could be dropped on invocation, in that case simply retry invocation
            catch (PipeNetworkException e) when (VerifyIfConnected() == false && retries < maxRetries && shouldDispatch)
            {
                Disconnect($"error: {e.Message}");
                shouldRetry = true;
                retries++;
            }
            //otherwise report error
            catch (Exception e)
            {
                var reason = e is OperationCanceledException ? "cancelled" : $"error: {e.Message}";
                Disconnect(reason);
                return (VerifyIfConnected(), false, e);
            }
            finally
            {
                Used = false;
            }
        }

        if (!shouldDispatch)
            return (VerifyIfConnected(), true, error);

        return (VerifyIfConnected(), false, error);
    }

    public bool VerifyIfConnected()
        => Connected && Connection is { IsConnected: true };
    public bool VerifyIfExpired(DateTime currentTime)
        => LastUsedAt + ConnectionExpiryTime < currentTime;

    public abstract void Disconnect(string reason);
    protected abstract Task<(bool Ok, Exception Error)> TryConnect(CancellationToken cancellation);
}