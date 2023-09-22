using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public class PipeConnectionPoolServer : PipeConnectionPoolBase
{
    private readonly ILogger _logger;
    private readonly Meter _meter;
    private readonly ConcurrentDictionary<string, PipeConnectionGroup<PipeServerConnection>> _connections = new();

    public IEnumerable<PipeConnection<NamedPipeServerStream>> Connections =>
        _connections.Values.SelectMany(pool => pool.Connections);

    public PipeConnectionPoolServer(ILogger logger, Meter meter, IPipeConnectionSettings settings, CancellationToken cancellation)
        : base(logger, cancellation, settings)
    {
        _logger = logger;
        _meter = meter;
    }

    public async ValueTask<(bool Connected, bool Dispatched, Exception Error)> UseConnection(
        string pipeName,
        Func<IPipeConnection, bool> activityPredicateFunc,
        Func<NamedPipeServerStream, Task<bool>> activityFunc)
    {
        var (connection, connectionPool, error) = await Get(_connections, pipeName, CreateNewConnectionPool);
        if (error != null)
            return (false, false, error);

        var useResult = await connection.UseConnection(UseConnection, activityPredicateFunc, Cancellation);
        Put(_connections, connectionPool, connection, useResult);
        return useResult;

        async Task UseConnection(NamedPipeServerStream stream)
        {
            var completed = await activityFunc.Invoke(stream);
            //if server message handler indicate that message handling completed - we stop this connection
            //this will happen when client disconnects
            if (completed)
                connection.Disconnect($"connection interaction completed");
        }

        PipeConnectionGroup<PipeServerConnection> CreateNewConnectionPool(string name)
        {
            _logger.LogInformation("created connection pool {PoolName} of type {Type}", name, "server");
            return new(_logger, name, Settings.Instances, CreateNewConnection, Settings.ConnectionRetryTimeout, Settings.ConnectionRetryTimeout);
        }

        PipeServerConnection CreateNewConnection(int id, string name)
            => new(_logger, _meter, id, name, Settings.Instances, Settings.Buffer, Settings.ConnectionExpiryTimeout);
    }

    public override void Cleanup()
    {
        var currentTime = DateTime.UtcNow;
        Release(_connections, ShouldCleanupServer, "server", "expired");

        bool ShouldCleanupServer(IPipeConnection connection)
            => !connection.VerifyIfConnected() && connection.VerifyIfExpired(currentTime);
    }

    public override void Dispose()
    {
        base.Dispose();
        Stop(_connections, "server");
    }
}