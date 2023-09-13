using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public class PipeConnectionPool : IAsyncDisposable
{
    class ConnectionPool<T>
    {
        public SemaphoreSlim ConnectionPoolLock = new SemaphoreSlim(1);
        public int CurrentInstance;

        public string Name { get; set; }
        public int Instances { get; set; }
        public ConcurrentStack<T> FreeConnections { get; set; }
        public ConcurrentDictionary<int, T> ConnectionQueue { get; set; }
    }

    private ILogger _logger;
    private Meter _meter;

    private Timer _expiryTimer;
    private int _expiryCheckIntervalMilliseconds = 500;
    private int _expiryCleanupTimeout = 50;

    //prevents running connections cleanup concurrently
    private SemaphoreSlim _connectionCleanLock = new SemaphoreSlim(1);

    private CancellationToken _cancellation;
    private ConcurrentDictionary<string, ConnectionPool<PipeServerConnection>> _serverConnections = new();
    private ConcurrentDictionary<string, ConnectionPool<PipeClientConnection>> _clientConnections = new();

    public int Instances { get; }
    public int Buffer { get; }

    public TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(3);
    public TimeSpan ConnectionExpiryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionRetryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionDisposeTimeout = TimeSpan.FromSeconds(10);

    public IEnumerable<IPipeConnection<NamedPipeServerStream>> ServerConnections =>
        _serverConnections.Values.SelectMany(pool => pool.ConnectionQueue.Values);
    public IEnumerable<IPipeConnection<NamedPipeClientStream>> ClientConnections =>
        _clientConnections.Values.SelectMany(pool => pool.ConnectionQueue.Values);

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

    public async Task<(bool Used, Exception Error)> UseClientConnection(string pipeName, Func<NamedPipeClientStream, Task> connectionActivityFunc)
    {
        var (connection, connectionPool) = await GetOrCreateConnection(pipeName, _clientConnections, CreateNewConnectionPool, CreateNewConnection);
        var useResult = await connection.UseConnection(connectionActivityFunc, _cancellation);
        await PutOrReleaseConnection(connectionPool, connection, useResult);
        return useResult;

        ConnectionPool<PipeClientConnection> CreateNewConnectionPool(string key)
            => new()
            {
                Name = pipeName,
                CurrentInstance = 0,
                Instances = Instances,
                FreeConnections = new ConcurrentStack<PipeClientConnection>(),
                ConnectionQueue = new ConcurrentDictionary<int, PipeClientConnection>()
            };

        PipeClientConnection CreateNewConnection(int key)
            => new(_logger, _meter, key, pipeName, ConnectionTimeout, ConnectionRetryTimeout);
    }

    public async Task<(bool Used, Exception Error)> UseServerConnection(string pipeName, Func<NamedPipeServerStream, Task> connectionActivityFunc)
    {
        var (connection, connectionPool) = await GetOrCreateConnection(pipeName, _serverConnections, CreateNewConnectionPool, CreateNewConnection);
        var useResult = await connection.UseConnection(connectionActivityFunc, _cancellation);
        await PutOrReleaseConnection(connectionPool, connection, useResult);
        return useResult;

        ConnectionPool<PipeServerConnection> CreateNewConnectionPool(string key)
            => new()
            {
                Name = pipeName,
                CurrentInstance = 0,
                Instances = Instances,
                FreeConnections = new ConcurrentStack<PipeServerConnection>(),
                ConnectionQueue = new ConcurrentDictionary<int, PipeServerConnection>()
            };

        PipeServerConnection CreateNewConnection(int key)
            => new(_logger, _meter, key, pipeName, Instances, Buffer, ConnectionRetryTimeout);
    }

    public async Task<bool> CleanupExpiredConnections()
    {
        if (!await _connectionCleanLock.WaitAsync(_expiryCleanupTimeout))
            return false;
        try
        {
            var currentTime = DateTime.UtcNow;

            var client = await ReleaseConnections(
                _clientConnections, ShouldCleanup, _expiryCleanupTimeout, CancellationToken.None);
            var server = await ReleaseConnections(
                _serverConnections, ShouldCleanup, _expiryCleanupTimeout, CancellationToken.None);

            return client && server;

            bool ShouldCleanup<TStream>(IPipeConnection<TStream> connection) where TStream : Stream
                => !connection.Connected && connection.LastUsedAt + ConnectionExpiryTimeout < currentTime;
        }
        finally
        {
            _connectionCleanLock.Release();
        }
    }

    private async Task<(T Connection, ConnectionPool<T> ConnectionPool)> GetOrCreateConnection<T>(
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
                    //no free connections available, check if we can create new one
                    if (connectionPool.CurrentInstance < connectionPool.Instances)
                        connection = connectionPool.ConnectionQueue.GetOrAdd(connectionPool.CurrentInstance++, connectionCreateFunc);
                    else
                        throw new IndexOutOfRangeException($"Run out of available connections on the pool {connectionPool.Name}");
                }
            }
            finally
            {
                connectionPool.ConnectionPoolLock.Release();
            }
        }
        return (connection, connectionPool);
    }

    private async Task PutOrReleaseConnection<T>(ConnectionPool<T> connectionPool, T connection, (bool Used, Exception Error) useResult)
        where T : class, IPipeConnection
    {
        if (!useResult.Used && useResult.Error != null)
        {
            //acquire lock on connection pool so we can release connection without race conditions
            await connectionPool.ConnectionPoolLock.WaitAsync(_cancellation);
            try
            {
                var currentConnections = connectionPool.ConnectionQueue;
                var rebalancedConnections = new ConcurrentDictionary<int, T>();
                connectionPool.CurrentInstance = 0;
                foreach(var connectionFromPool in currentConnections.Values)
                {
                    if (connectionFromPool.Id != connection.Id)
                        rebalancedConnections.GetOrAdd(connectionPool.CurrentInstance++, connectionFromPool);
                }
                connectionPool.ConnectionQueue = rebalancedConnections;
                currentConnections.Clear();
            }
            finally
            {
                connectionPool.ConnectionPoolLock.Release();
            }
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
            var connectionPoolConnectionsCount = connectionPool.Value.ConnectionQueue.Count;
            var connectionPoolReleasedSomeConnections = false;
            var connectionPoolReleasedAllConnections = true;
            foreach(var connection in connectionPool.Value.ConnectionQueue)
            {
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
                connectionPoolReleasedAllConnections = true;
                try
                {
                    foreach(var connection in connectionPool.Value.ConnectionQueue)
                    {
                        connectionPoolReleasedAllConnections = connectionPoolReleasedAllConnections && !connection.Value.Connected;
                    }
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
                    await ReleaseConnections(_clientConnections, c => true, _expiryCleanupTimeout, CancellationToken.None) != true ||
                    await ReleaseConnections(_serverConnections, c => true, _expiryCleanupTimeout, CancellationToken.None) != true
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