using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public abstract class PipeConnectionPoolBase : IDisposable
{
    private ILogger _logger;
    private Timer _expiryTimer;

    protected CancellationToken Cancellation { get; private set; }
    protected IPipeConnectionSettings Settings { get; private set; }

    protected PipeConnectionPoolBase(ILogger logger, CancellationToken cancellation, IPipeConnectionSettings settings)
    {
        _logger = logger;
        Settings = settings;
        Cancellation = cancellation;

        Cancellation.Register(() => {
            _expiryTimer?.Dispose();
            _expiryTimer = null;
        });

        if (Cancellation.IsCancellationRequested)
            return;

        _expiryTimer = new Timer(_ => Cleanup(), this, 0, (int)Settings.ConnectionReleaseTimeout.TotalMilliseconds);
    }

    public abstract void Cleanup();
    
    protected async ValueTask<(T Connection, PipeConnectionGroup<T> ConnectionPool, Exception Error)> Get<T>(
        ConcurrentDictionary<string, PipeConnectionGroup<T>> connections,
        string pipeName,
        Func<string, PipeConnectionGroup<T>> poolCreateFunc)
            where T : class, IPipeConnection
    {
        var connectionPool = connections.GetOrAdd(pipeName, poolCreateFunc);
        var (connection, errors) = await connectionPool.BorrowConnection(100);
        if (connection == null)
            return (null, null, new IndexOutOfRangeException($"Run out of available connections on the pool {pipeName}", errors));
        return (connection, connectionPool, null);
    }

    protected void Put<T>(
        ConcurrentDictionary<string, PipeConnectionGroup<T>> connections,
        PipeConnectionGroup<T> connectionPool,
        T connection,
        (bool Connected, bool Dispatched, Exception Error) useResult)
            where T : class, IPipeConnection
    {
        if (connections.TryGetValue(connectionPool.Name, out var currentConnectionPool) &&
            currentConnectionPool.Equals(connectionPool))
        {
            if ((!useResult.Connected || !useResult.Dispatched) && useResult.Error != null)
            {
                //error return connection and report error
                connectionPool.ReturnConnection(connection, useResult.Error);
            }
            else
            {
                //if all good - simply return connection
                connectionPool.ReturnConnection(connection, null);
            }
        }
        else
        {
            //this could happen during race condition when
            // - ReleaseConnections removed whole connection pool considering all connections already released
            // - just before pool gets removed - somebody comes and uses connection from it
            // - in this case we simply shut down this connection as pool isn't tracked anymore
            connectionPool.ReturnConnection(connection, new InvalidOperationException("outdated pool"));
        }
    }

    protected void Release<T>(
        ConcurrentDictionary<string, PipeConnectionGroup<T>> connections,
        Func<IPipeConnection, bool> releaseCondition,
        string type,
        string reason)
            where T: IPipeConnection
    {
        foreach(var connectionPool in connections.Values)
        {
            if (connectionPool.DisableConnections(releaseCondition, reason) &&
                connectionPool.IsUnusedPool(Settings.ConnectionDisposeTimeout.TotalMilliseconds))
            {
                //if we disabled all connections and connection pool indicates that it's not in use - remove it from pools
                connections.TryRemove(connectionPool.Name, out _);
                _logger.LogInformation("connection pool {PoolName} of type {Type} cleaned up '{Reason}'", connectionPool.Name, type, reason);
            }
            else
            {
                //if pool active - trigger function to restore broken connections
                connectionPool.RestoreConnections();
            }
        }
    }

    protected void Stop<T>(ConcurrentDictionary<string, PipeConnectionGroup<T>> connections, string type)
        where T: IPipeConnection
    {
        foreach(var connectionPool in connections.Values)
        {
            connectionPool.DisableConnections(_ => true, "stopped");
            connections.TryRemove(connectionPool.Name, out _);
            _logger.LogInformation("connection pool {PoolName} of type {Type} cleaned up '{Reason}'", connectionPool.Name, type, "stopped");
        }
    }    

    public virtual void Dispose()
    {
        _expiryTimer?.Dispose();
        _expiryTimer = null;
    }
}