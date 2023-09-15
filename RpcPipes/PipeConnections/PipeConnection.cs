using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

internal abstract class PipeConnection<T> : IPipeConnection<T>
    where T: PipeStream
{
    private ILogger _logger;

    protected T _connection;
    protected volatile bool _connected;
    protected volatile bool _inUse;    

    public int Id { get; }
    public string Name { get; }

    public bool InUse => _inUse;

    public DateTime LastUsedAt { get; private set; }
    public TimeSpan ConnectionRetryTimeout { get; }
    public TimeSpan ConnectionExpiryTime { get; }

    public PipeConnection(ILogger logger, int id, string name, TimeSpan connectionRetryTime, TimeSpan connectionExpiryTime)
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

        bool ok = false;
        bool shouldUse = true;
        Exception error;

        try
        {
            _inUse = true;
            shouldUse = usePredicateFunc == null || usePredicateFunc.Invoke(this);
            LastUsedAt = DateTime.UtcNow;

            (ok, error) = await TryConnect(cancellation);            
            if  (ok && shouldUse)
            {
                LastUsedAt = DateTime.UtcNow;
                await useFunc.Invoke(_connection);
                LastUsedAt = DateTime.UtcNow;
                return (VerifyIfConnected(), VerifyIfDispatched(ok, shouldUse), error);
            }
            if (!shouldUse)
            {
                return (VerifyIfConnected(), VerifyIfDispatched(ok, shouldUse), error);
            }
            if (!ok && shouldUse && error is not OperationCanceledException)
            {
                _logger.LogWarning("apply {Delay} due to unhandled connection error '{Error}'", ConnectionRetryTimeout, error.Message);
                await Task.Delay(ConnectionRetryTimeout);
            }
            return (VerifyIfConnected(), VerifyIfDispatched(ok, shouldUse), error);
        }
        catch (Exception ex)
        {            
            Disconnect($"error: {ex.Message}");
            return (VerifyIfConnected(), VerifyIfDispatched(ok, shouldUse), ex);
        }
        finally
        {
            _inUse = false;
        }
    }

    public bool VerifyIfConnected()
        => _connected && _connection != null && _connection.IsConnected;
    public bool VerifyIfExpired(DateTime currentTime)
        => LastUsedAt + ConnectionExpiryTime < currentTime;

    public abstract void Disconnect(string reason);
    protected abstract Task<(bool Ok, Exception Error)> TryConnect(CancellationToken cancellation);
}