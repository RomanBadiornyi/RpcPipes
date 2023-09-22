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

    private readonly CancellationToken _cancellation;
    private readonly ConcurrentDictionary<string, PipeConnectionGroup<PipeServerConnection>> _connectionsServer = new();
    private readonly ConcurrentDictionary<string, PipeConnectionGroup<PipeClientConnection>> _connectionsClient = new();

    public int Instances { get; }
    public int Buffer { get; }

    public TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(3);
    public TimeSpan ConnectionExpiryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionRetryTimeout = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionDisposeTimeout = TimeSpan.FromSeconds(10);   
    public TimeSpan ConnectionReleaseTimeout = TimeSpan.FromMilliseconds(500); 

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

        _expiryTimer = new Timer(_ => Cleanup(), this, 0, (int)ConnectionReleaseTimeout.TotalMilliseconds);
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
{
            _logger.LogInformation("created connection pool {PoolName} of type {Type}", name, "client");
            return new(name, Instances, CreateNewConnection, ConnectionRetryTimeout, ConnectionRetryTimeout);
        }

        PipeClientConnection CreateNewConnection(int id, string name)
            => new(_logger, _meter, id, name, ConnectionTimeout, ConnectionExpiryTimeout);
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
        {
            _logger.LogInformation("created connection pool {PoolName} of type {Type}", name, "server");
            return new(name, Instances, CreateNewConnection, ConnectionRetryTimeout, ConnectionRetryTimeout);
        }

        PipeServerConnection CreateNewConnection(int id, string name)
            => new(_logger, _meter, id, name, Instances, Buffer, ConnectionExpiryTimeout);
    }

    public void Cleanup()
    {
        var currentTime = DateTime.UtcNow;

        Release(_connectionsClient, ShouldCleanupClient, "client", "expired");
        Release(_connectionsServer, ShouldCleanupServer, "server", "expired");

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
        var connectionPool = connections.GetOrAdd(pipeName, poolCreateFunc);
        var (connection, errors) = await connectionPool.BorrowConnection(100);        
        if (connection == null)
            return (null, null, new IndexOutOfRangeException($"Run out of available connections on the pool {pipeName}", errors));
        return (connection, connectionPool, null);
    }

    private void Put<T>(
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

    private void Release<T>(
        ConcurrentDictionary<string, PipeConnectionGroup<T>> connections,
        Func<IPipeConnection, bool> releaseCondition,
        string type,
        string reason)
            where T: IPipeConnection
    {
        foreach(var connectionPool in connections.Values)
        {            
            if (connectionPool.DisableConnections(releaseCondition, reason) && 
                connectionPool.IsUnusedPool(ConnectionDisposeTimeout.TotalMilliseconds))
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

    public async ValueTask DisposeAsync()
    {
        _expiryTimer?.Dispose();
        _expiryTimer = null;
        Release(_connectionsClient, _ => true, "client", "disposed");
        Release(_connectionsServer, _ => true, "server", "disposed");
        await Task.CompletedTask;
    }
}