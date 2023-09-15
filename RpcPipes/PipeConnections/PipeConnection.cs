using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public abstract class PipeConnection<T> : IPipeConnection
    where T: PipeStream
{
    private readonly ILogger _logger;

    protected T Connection;
    protected volatile bool Connected;
    protected volatile bool Used;    

    public int Id { get; }
    public string Name { get; }

    public bool InUse => Used;

    public DateTime LastUsedAt { get; private set; }
    public TimeSpan ConnectionRetryTimeout { get; }
    public TimeSpan ConnectionExpiryTime { get; }

    protected PipeConnection(ILogger logger, int id, string name, TimeSpan connectionRetryTime, TimeSpan connectionExpiryTime)
    {
        _logger = logger;
        Id = id;
        Name = name;
        ConnectionRetryTimeout = connectionRetryTime;
        ConnectionExpiryTime = connectionExpiryTime;
        LastUsedAt = DateTime.UtcNow;
    }

    public async ValueTask<(bool Connected, bool Dispatched, Exception Error)> UseConnection(
        Func<T, Task> useFunc, Func<IPipeConnection, bool> usePredicateFunc, CancellationToken cancellation)
    {
        static bool VerifyIfDispatched(bool ok, bool shouldUse)
            => ok || !shouldUse;

        var ok = false;
        var shouldUse = true;

        try
        {
            Used = true;
            shouldUse = usePredicateFunc == null || usePredicateFunc.Invoke(this);
            LastUsedAt = DateTime.UtcNow;

            (ok, var error) = await TryConnect(cancellation);            
            if  (ok && shouldUse)
            {
                LastUsedAt = DateTime.UtcNow;
                await useFunc.Invoke(Connection);
                LastUsedAt = DateTime.UtcNow;
                return (VerifyIfConnected(), VerifyIfDispatched(true, true), error);
            }
            if (!shouldUse)
            {
                return (VerifyIfConnected(), VerifyIfDispatched(ok, false), error);
            }
            if (error is not OperationCanceledException)
            {
                _logger.LogWarning("apply {Delay} due to unhandled connection error '{Error}'", ConnectionRetryTimeout, error.Message);
                await Task.Delay(ConnectionRetryTimeout, cancellation);
            }
            return (VerifyIfConnected(), VerifyIfDispatched(false, true), error);
        }
        catch (Exception ex)
        {         
            var reason = ex is OperationCanceledException ? "cancelled" : $"error: {ex.Message}";
            Disconnect(reason);
            return (VerifyIfConnected(), VerifyIfDispatched(ok, shouldUse), ex);
        }
        finally
        {
            Used = false;
        }
    }

    public bool VerifyIfConnected()
        => Connected && Connection is { IsConnected: true };
    public bool VerifyIfExpired(DateTime currentTime)
        => LastUsedAt + ConnectionExpiryTime < currentTime;

    public abstract void Disconnect(string reason);
    protected abstract Task<(bool Ok, Exception Error)> TryConnect(CancellationToken cancellation);
}