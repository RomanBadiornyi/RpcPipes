using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public partial class PipeConnectionPool : IAsyncDisposable
{
    private ILogger _logger;
    private Meter _meter;

    private Timer _expiryTimer;
    private int _expiryCheckIntervalMilliseconds = 500;
    private int _expiryCleanupTimeout = 50;

    //prevents running connections cleanup concurrently
    private SemaphoreSlim _connectionCleanLock = new SemaphoreSlim(1);

    private CancellationToken _cancellation;
    private ConcurrentDictionary<string, PipeConnectionGroup<PipeServerConnection>> _connectionsServer = new();
    private ConcurrentDictionary<string, PipeConnectionGroup<PipeClientConnection>> _connectionsClient = new();

    public int Instances { get; }
    public int Buffer { get; }

    public TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(3);
    public TimeSpan ConnectionExpiryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionRetryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionDisposeTimeout = TimeSpan.FromSeconds(10);

    public IEnumerable<IPipeConnection<NamedPipeServerStream>> ConnectionsServer =>
        _connectionsServer.Values.SelectMany(pool => pool.FreeConnections.Concat(pool.DisabledConnections)).Distinct();
    public IEnumerable<IPipeConnection<NamedPipeClientStream>> ConnectionsClient =>
        _connectionsClient.Values.SelectMany(pool => pool.FreeConnections.Concat(pool.DisabledConnections)).Distinct();

    public PipeConnectionPool(ILogger logger, Meter meter, int instances, int buffer, CancellationToken cancellation)
    {
        _logger = logger;
        _meter = meter;
        _cancellation = cancellation;

        _cancellation.Register(() => {
            _expiryTimer?.Dispose();
            _expiryTimer = null;
        });

        Instances = instances;
        Buffer = buffer;

        if (_cancellation.IsCancellationRequested)
            return;

        _expiryTimer = new Timer(async _ => { await CleanupExpiredConnections(); }, this, 0, _expiryCheckIntervalMilliseconds);
    }

    public async ValueTask<(bool Connected, bool Dispatched, Exception Error)> UseClientConnection(
        string pipeName, Func<IPipeConnection, bool> connectionActivityPredicateFunc, Func<NamedPipeClientStream, Task> connectionActivityFunc)
    {
        var (connection, connectionPool) = await GetOrCreateConnection(pipeName, _connectionsClient, CreateNewConnectionPool);
        var useResult = await connection.UseConnection(connectionActivityFunc, connectionActivityPredicateFunc, _cancellation);
        PutOrReleaseConnection(_connectionsClient, connectionPool, connection, useResult);
        return useResult;

        PipeConnectionGroup<PipeClientConnection> CreateNewConnectionPool(string name)
            => new PipeConnectionGroup<PipeClientConnection>(name, Instances, CreateNewConnection);

        PipeClientConnection CreateNewConnection(int id, string name)
            => new(_logger, _meter, id, name, ConnectionTimeout, ConnectionRetryTimeout, ConnectionExpiryTimeout);
    }

    public async ValueTask<(bool Connected, bool Dispatched, Exception Error)> UseServerConnection(
        string pipeName, Func<IPipeConnection, bool> connectionActivityPredicateFunc, Func<NamedPipeServerStream, Task> connectionActivityFunc)
    {
        var (connection, connectionPool) = await GetOrCreateConnection(pipeName, _connectionsServer, CreateNewConnectionPool);
        var useResult = await connection.UseConnection(connectionActivityFunc, connectionActivityPredicateFunc, _cancellation);
        PutOrReleaseConnection(_connectionsServer, connectionPool, connection, useResult);
        return useResult;

        PipeConnectionGroup<PipeServerConnection> CreateNewConnectionPool(string name)
            => new PipeConnectionGroup<PipeServerConnection>(name, Instances, CreateNewConnection);

        PipeServerConnection CreateNewConnection(int id, string name)
            => new(_logger, _meter, id, name, Instances, Buffer, ConnectionRetryTimeout, ConnectionExpiryTimeout);
    }

    public async Task CleanupExpiredConnections()
    {
        if (!await _connectionCleanLock.WaitAsync(_expiryCleanupTimeout))
            return;
        try
        {
            var currentTime = DateTime.UtcNow;

            ReleaseConnections(_connectionsClient, ShouldCleanupClient, "expired");
            ReleaseConnections(_connectionsServer, ShouldCleanupServer, "expired");

            bool ShouldCleanupClient(IPipeConnection connection)
                => !connection.InUse && connection.VerifyIfExpired(currentTime);

            bool ShouldCleanupServer(IPipeConnection connection)
                => !connection.VerifyIfConnected() && connection.VerifyIfExpired(currentTime);
        }
        finally
        {
            _connectionCleanLock.Release();
        }
    }

    private async ValueTask<(T Connection, PipeConnectionGroup<T> ConnectionPool)> GetOrCreateConnection<T>(
        string pipeName,
        ConcurrentDictionary<string, PipeConnectionGroup<T>> connections,
        Func<string, PipeConnectionGroup<T>> poolCreateFunc)
            where T : class, IPipeConnection
    {
        T connection = null;
        PipeConnectionGroup<T> connectionPool = null;
        var tries = 0;
        while (connection == null && tries < 10)
        {
            connectionPool = connections.GetOrAdd(pipeName, poolCreateFunc);
            if (!connections.ContainsKey(pipeName))
                continue;
            if (connectionPool.FreeConnections.TryPop(out connection))
                break;
            if (connectionPool.DisabledConnections.TryDequeue(out connection))
                break;
            if (tries < 10)
                await Task.Delay(TimeSpan.FromSeconds(_expiryCleanupTimeout));
            tries++;
        }
        if (connection == null)
            throw new IndexOutOfRangeException($"Run out of available connections on the pool {connectionPool.Name}");
        connectionPool.BusyConnections.TryAdd(connection.Id, connection);
        return (connection, connectionPool);
    }

    private void PutOrReleaseConnection<T>(
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
                //after error disconnect and return connection to DisabledConnections queue
                //this way such connection will be picked in the end when we run out of free connections
                connection.Disconnect($"error: {useResult.Error.Message}");
                connectionPool.DisabledConnections.Enqueue(connection);
            }
            else
            {
                //if all good - return connection to free FreeConnections stack so that next message will pick most recently used connection
                //this way during load decrease we will let connections in the bottom of the stack to get expired first
                //and gradually reduce number of active connections
                connectionPool.FreeConnections.Push(connection);
            }
        }
        else
        {
            //this could happen during race condition when
            // - ReleaseConnections removed whole connection pool considering all connections already released
            // - just before pool gets removed - somebody comes and uses connection from it
            // - in this case we simply shut down this connection as pool isn't tracked anymore
            connectionPool.DisabledConnections.Enqueue(connection);
            connection.Disconnect("outdated pool");
        }
        connectionPool.BusyConnections.TryRemove(connection.Id, out _);
    }

    private void ReleaseConnections<T>(
        ConcurrentDictionary<string, PipeConnectionGroup<T>> connections, Func<IPipeConnection, bool> releaseCondition, string reason)
        where T: IPipeConnection
    {
        foreach(var connectionPool in connections.Values)
        {
            //ensure all disabled connections are in disconnected state
            foreach(var connection in connectionPool.DisabledConnections)
            {
                if (releaseCondition.Invoke(connection))
                    connection.Disconnect(reason);
            }
            //try to release free connections based on provided delegate
            var allDisconnected = true;
            foreach(var connection in connectionPool.FreeConnections)
            {
                if (releaseCondition.Invoke(connection))
                    connection.Disconnect(reason);
                else
                    allDisconnected = false;
            }
            //if we disconnected all connections from FreeConnections -
            //move them now to DisabledConnections in order to indicate that we no longer have FreeConnections available
            if (allDisconnected)
            {
                while (connectionPool.FreeConnections.TryPop(out var connection))
                    connectionPool.DisabledConnections.Enqueue(connection);
            }

            //lock connections counts for cleanup checks
            var free = connectionPool.FreeConnections.Count;
            var busy = connectionPool.BusyConnections.Count;
            var disabled = connectionPool.DisabledConnections.Count;


            if (free == 0 && disabled == connectionPool.Instances)
            {
                connections.TryRemove(connectionPool.Name, out _);
                _logger.LogInformation("connection pool {PoolName} cleaned up '{Reason}'", connectionPool.Name, reason);
            }
            //bellow is a bit fearful logic which attempts to indicate case when we might loose track of some connections due to unpredicted error
            //in such case we will remove this pool from tracking so that new one will be created on next connection request
            var instancesCurrent = free + busy + disabled;
            //since we don't have locks, for short period of time it is possible that bellow condition will be false
            //that is why we detect it as a problem only if it happens 10 times in a row - that is something very unlikely to happen, 
            //so put warning log for tracking
            if (instancesCurrent != connectionPool.Instances)
                Interlocked.Increment(ref connectionPool.IncorrectConnectionsDetected);
            else
                connectionPool.IncorrectConnectionsDetected = 0;

            if (connectionPool.IncorrectConnectionsDetected > 10)
            {
                connections.TryRemove(connectionPool.Name, out _);
                _logger.LogWarning("connection pool {PoolName} has wrong connections {Connections} when expected {Instances}",
                    connectionPool.Name, instancesCurrent, Instances);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _expiryTimer?.Dispose();
        _expiryTimer = null;

        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.CancelAfter(ConnectionDisposeTimeout);

        await _connectionCleanLock.WaitAsync(cancellationSource.Token);
        try
        {
            ReleaseConnections(_connectionsClient, c => true, "disposed");
            ReleaseConnections(_connectionsServer, c => true, "disposed");
        }
        finally
        {
            _connectionCleanLock.Release();
        }
    }
}