using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public class PipeConnectionPool : IAsyncDisposable
{
    internal class ConnectionPool<T>
    {
        //ensures sync access to connections within the pool
        public SemaphoreSlim ConnectionPoolLock = new SemaphoreSlim(1);        

        public string Name { get; set; }

        public int Instances { get; set; }
        public int MaxInstances { get; set; }

        public ConcurrentStack<T> FreeConnections { get; set; }
        public ConcurrentDictionary<int, T> AllConnections { get; set; }
    }

    private ILogger _logger;
    private Meter _meter;

    private Timer _expiryTimer;
    private int _expiryCheckIntervalMilliseconds = 500;
    private int _expiryCleanupTimeout = 50;

    //prevents running connections cleanup concurrently
    private SemaphoreSlim _connectionCleanLock = new SemaphoreSlim(1);

    private CancellationToken _cancellation;
    private ConcurrentDictionary<string, ConnectionPool<PipeServerConnection>> _connectionsServer = new();
    private ConcurrentDictionary<string, ConnectionPool<PipeClientConnection>> _connectionsClient = new();

    public int Instances { get; }
    public int Buffer { get; }

    public TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(3);
    public TimeSpan ConnectionExpiryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionRetryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionDisposeTimeout = TimeSpan.FromSeconds(10);

    public IEnumerable<IPipeConnection<NamedPipeServerStream>> ServerConnections =>
        _connectionsServer.Values.SelectMany(pool => pool.AllConnections.Values);
    public IEnumerable<IPipeConnection<NamedPipeClientStream>> ClientConnections =>
        _connectionsClient.Values.SelectMany(pool => pool.AllConnections.Values);

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

    public async ValueTask<(bool Connected, bool Used, Exception Error)> UseClientConnection(
        string pipeName, Func<NamedPipeClientStream, Task> connectionActivityFunc, Action<IPipeConnection> connectionCallback = null)
    {
        var (connection, connectionPool) = await GetOrCreateConnection(pipeName, _connectionsClient, CreateNewConnectionPool, CreateNewConnection);
        connectionCallback?.Invoke(connection);
        var useResult = await connection.UseConnection(connectionActivityFunc, _cancellation);
        await PutOrReleaseConnection(connectionPool, connection, useResult);

        return useResult;

        ConnectionPool<PipeClientConnection> CreateNewConnectionPool(string pipeName)
            => CreateConnectionPool<PipeClientConnection>(pipeName);

        PipeClientConnection CreateNewConnection(int connectionId)
            => new(_logger, _meter, connectionId, pipeName, ConnectionTimeout, ConnectionRetryTimeout);
    }

    public async ValueTask<(bool Connected, bool Used, Exception Error)> UseServerConnection(
        string pipeName, Func<NamedPipeServerStream, Task> connectionActivityFunc, Action<IPipeConnection> connectionCallback = null)
    {
        var (connection, connectionPool) = await GetOrCreateConnection(pipeName, _connectionsServer, CreateNewConnectionPool, CreateNewConnection);
        connectionCallback?.Invoke(connection);
        var useResult = await connection.UseConnection(connectionActivityFunc, _cancellation);
        await PutOrReleaseConnection(connectionPool, connection, useResult);
        
        return useResult;

        ConnectionPool<PipeServerConnection> CreateNewConnectionPool(string pipeName)
            => CreateConnectionPool<PipeServerConnection>(pipeName);

        PipeServerConnection CreateNewConnection(int connectionId)
            => new(_logger, _meter, connectionId, pipeName, Instances, Buffer, ConnectionRetryTimeout);
    }

    public async Task<bool> CleanupExpiredConnections()
    {
        if (!await _connectionCleanLock.WaitAsync(_expiryCleanupTimeout))
            return false;
        try
        {
            var currentTime = DateTime.UtcNow;

            var client = await ReleaseConnections(
                _connectionsClient, ShouldCleanupClient, _expiryCleanupTimeout, CancellationToken.None);
            var server = await ReleaseConnections(
                _connectionsServer, ShouldCleanupServer, _expiryCleanupTimeout, CancellationToken.None);

            return client && server;

            bool ShouldCleanupClient<TStream>(IPipeConnection<TStream> connection) where TStream : Stream
                => !connection.InUse && connection.LastUsedAt + ConnectionExpiryTimeout < currentTime;

            bool ShouldCleanupServer<TStream>(IPipeConnection<TStream> connection) where TStream : Stream
                => !connection.Connected && connection.LastUsedAt + ConnectionExpiryTimeout < currentTime;
        }
        finally
        {
            _connectionCleanLock.Release();
        }
    }

    private async ValueTask<(T Connection, ConnectionPool<T> ConnectionPool)> GetOrCreateConnection<T>(
        string pipeName,
        ConcurrentDictionary<string, ConnectionPool<T>> connections,
        Func<string, ConnectionPool<T>> poolCreateFunc,
        Func<int, T> connectionCreateFunc)
            where T : class, IPipeConnection
    {
        T connection = null;
        ConnectionPool<T> connectionPool = null;
        while (connection == null)
        {
            connectionPool = connections.GetOrAdd(pipeName, poolCreateFunc);

            //acquire lock on connection pool so we can pick what connection to use without race conditions
            await connectionPool.ConnectionPoolLock.WaitAsync(_cancellation);
            //ensure it was not cleaned by ReleaseConnections before we acquired ConnectionPoolLock and if so - retry
            if (!connections.ContainsKey(pipeName))
            {
                connectionPool.ConnectionPoolLock.Release();
                continue;
            }
            try
            {
                //create connection and release lock
                if (!connectionPool.FreeConnections.TryPop(out connection))
                {
                    //check if maybe there is some disabled connection we can re-create
                    foreach (var item in connectionPool.AllConnections)
                    {
                        if (item.Value.Disabled)
                        {
                            connection = connectionCreateFunc(item.Key);
                            connectionPool.AllConnections[item.Key] = connection;
                            break;
                        }
                    }
                    if (connection == null)
                    {
                        //if no free connections and free slots available, check if we can create new one
                        if (connectionPool.Instances < connectionPool.MaxInstances)
                        {
                            connectionPool.Instances += 1;
                            connection = connectionPool.AllConnections.GetOrAdd(connectionPool.Instances, connectionCreateFunc);
                        }
                        else
                            throw new IndexOutOfRangeException($"Run out of available connections on the pool {connectionPool.Name}");
                    }                    
                }
            }
            finally
            {
                connectionPool.ConnectionPoolLock.Release();
            }
        }
        return (connection, connectionPool);
    }

    private async ValueTask PutOrReleaseConnection<T>(
        ConnectionPool<T> connectionPool, T connection, (bool Connected, bool Used, Exception Error) useResult)
        where T : class, IPipeConnection
    {
        if ((!useResult.Connected || !useResult.Used) && useResult.Error != null)
        {
            //acquire lock on connection pool and disable this connection, so it won't be used by subsequent calls
            await connectionPool.ConnectionPoolLock.WaitAsync(_cancellation);
            connection.DisableConnection();
            connectionPool.ConnectionPoolLock.Release();
        }
        else
        {
            connectionPool.FreeConnections.Push(connection);
        }
    }

    private async Task<bool> ReleaseConnections<T>(
        ConcurrentDictionary<string, ConnectionPool<T>> connections,
        Func<T, bool> releaseCondition,
        int timeout,
        CancellationToken cancellation)
            where T: IPipeConnection
    {
        var allReleased = true;
        foreach(var connectionPool in connections)
        {
            var connectionPoolConnectionsCount = connectionPool.Value.AllConnections.Count;
            var connectionPoolReleasedSomeConnections = false;
            var connectionPoolReleasedAllConnections = true;
            foreach(var connection in connectionPool.Value.AllConnections)
            {
                if (connection.Value == null)
                    continue;
                var released = true;
                if (!releaseCondition.Invoke(connection.Value))
                    released = false;
                else
                    released =  await connection.Value.TryReleaseConnection(timeout, cancellation);
                connectionPoolReleasedSomeConnections = connectionPoolReleasedSomeConnections || released;
                connectionPoolReleasedAllConnections = connectionPoolReleasedAllConnections && released;
            }
            //in case if we know that we released within pool some connections -
            //verify if pool can be cleared from pools collection
            //acquire ConnectionPoolLock so during cleanup new connections can not be added
            if (connectionPoolConnectionsCount > 0 && connectionPoolReleasedSomeConnections)
            {
                await connectionPool.Value.ConnectionPoolLock.WaitAsync(cancellation);                
                try
                {
                    connectionPoolReleasedAllConnections = true;
                    foreach(var connection in connectionPool.Value.AllConnections)
                    {
                        connectionPoolReleasedAllConnections = connectionPoolReleasedAllConnections && 
                            (connection.Value == null || !connection.Value.Connected);
                    }
                    //remove connections pool if all connections has been released
                    if (connectionPoolReleasedAllConnections)
                        connections.TryRemove(connectionPool.Key, out _);
                }
                finally
                {
                    connectionPool.Value.ConnectionPoolLock.Release();
                }
            }
            allReleased = allReleased && connectionPoolReleasedAllConnections;
        }
        return allReleased;
    }

    private ConnectionPool<T> CreateConnectionPool<T>(string pipeName)
            => new()
            {
                Name = pipeName,
                Instances = 0,
                MaxInstances = Instances,
                FreeConnections = new ConcurrentStack<T>(),
                AllConnections = new ConcurrentDictionary<int, T>()
            };

    public async ValueTask DisposeAsync()
    {
        _expiryTimer?.Dispose();
        _expiryTimer = null;

        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.CancelAfter(ConnectionDisposeTimeout);
        
        await _connectionCleanLock.WaitAsync(cancellationSource.Token);
        //acquire connectionCleanLock here so that we don't interfere with cleanup call
        try
        {
            while (
                (
                    await ReleaseConnections(_connectionsClient, c => true, _expiryCleanupTimeout, CancellationToken.None) != true ||
                    await ReleaseConnections(_connectionsServer, c => true, _expiryCleanupTimeout, CancellationToken.None) != true
                )
                && !cancellationSource.IsCancellationRequested
            ) {};
        }
        finally
        {
            _connectionCleanLock.Release();
        }
    }
}