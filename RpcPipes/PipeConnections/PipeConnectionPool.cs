using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public class PipeConnectionPool : IAsyncDisposable
{
    private readonly ILogger _logger;
    private readonly Meter _meter;

    private Timer _expiryTimer;
    private readonly int _expiryCheckIntervalMilliseconds = 500;
    private readonly int _expiryCleanupTimeout = 50;

    private readonly CancellationToken _cancellation;
    private readonly ConcurrentDictionary<string, PipeConnectionGroup<PipeServerConnection>> _connectionsServer = new();
    private readonly ConcurrentDictionary<string, PipeConnectionGroup<PipeClientConnection>> _connectionsClient = new();

    public int Instances { get; }
    public int Buffer { get; }

    public TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(3);
    public TimeSpan ConnectionExpiryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionRetryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionDisposeTimeout = TimeSpan.FromSeconds(10);

    public IEnumerable<PipeConnection<NamedPipeServerStream>> ConnectionsServer =>
        _connectionsServer.Values.SelectMany(pool => pool.Connections);
    public IEnumerable<PipeConnection<NamedPipeClientStream>> ConnectionsClient =>
        _connectionsClient.Values.SelectMany(pool => pool.Connections);

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

        _expiryTimer = new Timer(_ => Cleanup(), this, 0, _expiryCheckIntervalMilliseconds);
    }

    public async ValueTask<(bool Connected, bool Dispatched, Exception Error)> UseClientConnection(
        string pipeName, 
        Func<IPipeConnection, bool> activityPredicateFunc, 
        Func<NamedPipeClientStream, Task> activityFunc)
    {
        var (connection, connectionPool, error) = await Get(_connectionsClient, pipeName, CreateNewConnectionPool);
        if (error != null)
            return (false, false, error);
        var useResult = await connection.UseConnection(activityFunc, activityPredicateFunc, _cancellation);
        Put(_connectionsClient, connectionPool, connection, useResult);
        return useResult;

        PipeConnectionGroup<PipeClientConnection> CreateNewConnectionPool(string name)
            => new(name, Instances, CreateNewConnection);

        PipeClientConnection CreateNewConnection(int id, string name)
            => new(_logger, _meter, id, name, ConnectionTimeout, ConnectionRetryTimeout, ConnectionExpiryTimeout);
    }

    public async ValueTask<(bool Connected, bool Dispatched, Exception Error)> UseServerConnection(
        string pipeName, 
        Func<IPipeConnection, bool> activityPredicateFunc, 
        Func<NamedPipeServerStream, Task> activityFunc)
    {
        var (connection, connectionPool, error) = await Get(_connectionsServer, pipeName, CreateNewConnectionPool);
        if (error != null)
            return (false, false, error);
        var useResult = await connection.UseConnection(activityFunc, activityPredicateFunc, _cancellation);
        Put(_connectionsServer, connectionPool, connection, useResult);
        return useResult;

        PipeConnectionGroup<PipeServerConnection> CreateNewConnectionPool(string name)
            => new(name, Instances, CreateNewConnection);

        PipeServerConnection CreateNewConnection(int id, string name)
            => new(_logger, _meter, id, name, Instances, Buffer, ConnectionRetryTimeout, ConnectionExpiryTimeout);
    }

    public void Cleanup()
    {
        var currentTime = DateTime.UtcNow;

        Release(_connectionsClient, ShouldCleanupClient, "expired");
        Release(_connectionsServer, ShouldCleanupServer, "expired");

        bool ShouldCleanupClient(IPipeConnection connection)
            => !connection.InUse && connection.VerifyIfExpired(currentTime);

        bool ShouldCleanupServer(IPipeConnection connection)
            => !connection.VerifyIfConnected() && connection.VerifyIfExpired(currentTime);
    }

    private async ValueTask<(T Connection, PipeConnectionGroup<T> ConnectionPool, Exception Error)> Get<T>(
        ConcurrentDictionary<string, PipeConnectionGroup<T>> connections,
        string pipeName,        
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
            if (connectionPool.BorrowConnection(out connection))
                break;
            await Task.Delay(TimeSpan.FromSeconds(_expiryCleanupTimeout), _cancellation);                
            tries++;
        }
        if (connection == null)
            return (null, null, new IndexOutOfRangeException($"Run out of available connections on the pool {pipeName}"));
        return (connection, connectionPool, null);
    }

    private static void Put<T>(
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
                connectionPool.ReturnConnection(connection, true);
            }
            else
            {
                //if all good - return connection to free FreeConnections stack so that next message will pick most recently used connection
                //this way during load decrease we will let connections in the bottom of the stack to get expired first
                //and gradually reduce number of active connections
                connectionPool.ReturnConnection(connection, false);
            }
        }
        else
        {
            //this could happen during race condition when
            // - ReleaseConnections removed whole connection pool considering all connections already released
            // - just before pool gets removed - somebody comes and uses connection from it
            // - in this case we simply shut down this connection as pool isn't tracked anymore
            connection.Disconnect("outdated pool");
            connectionPool.ReturnConnection(connection, true);
        }
    }

    private void Release<T>(
        ConcurrentDictionary<string, PipeConnectionGroup<T>> connections, 
        Func<IPipeConnection, bool> releaseCondition, 
        string reason)
            where T: IPipeConnection
    {
        foreach(var connectionPool in connections.Values)
        {
            //ensure all disabled connections are in disconnected state
            foreach(var connection in connectionPool.Disabled)
            {
                if (releaseCondition.Invoke(connection))
                    connection.Disconnect(reason);
            }
            //try to release free connections based on provided delegate
            var allDisconnected = true;
            foreach(var connection in connectionPool.Free)
            {
                if (releaseCondition.Invoke(connection))
                    connection.Disconnect(reason);
                else
                    allDisconnected = false;
            }
            //if we disconnected all connections from FreeConnections -
            //move them now to DisabledConnections in order to indicate that we no longer have FreeConnections available
            if (allDisconnected)
                connectionPool.DisableConnections();

            //lock connections counts for cleanup checks
            if (connectionPool.Unused())
            {
                connections.TryRemove(connectionPool.Name, out _);
                _logger.LogInformation("connection pool {PoolName} cleaned up '{Reason}'", connectionPool.Name, reason);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        _expiryTimer?.Dispose();
        _expiryTimer = null;

        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.CancelAfter(ConnectionDisposeTimeout);
        
        Release(_connectionsClient, _ => true, "disposed");
        Release(_connectionsServer, _ => true, "disposed");
        await Task.CompletedTask;
    }
}