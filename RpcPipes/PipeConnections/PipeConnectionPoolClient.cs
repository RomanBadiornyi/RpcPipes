using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public class PipeConnectionPoolClient : PipeConnectionPoolBase
{
    private readonly ILogger _logger;
    private readonly Meter _meter;
    private readonly ConcurrentDictionary<string, PipeConnectionGroup<PipeClientConnection>> _connections = new();

    public IEnumerable<PipeConnection<NamedPipeClientStream>> Connections =>
        _connections.Values.SelectMany(pool => pool.Connections);

    public PipeConnectionPoolClient(ILogger logger, Meter meter, IPipeConnectionSettings settings, CancellationToken cancellation)
        : base(logger, cancellation, settings)
    {
        _logger = logger;
        _meter = meter;
    }

    public async ValueTask<(bool Connected, bool Dispatched, Exception Error)> UseConnection(
        string pipeName,
        Func<IPipeConnection, bool> activityPredicateFunc,
        Func<NamedPipeClientStream, Task> activityFunc)
    {
        var (connection, connectionPool, error) = await Get(_connections, pipeName, CreateNewConnectionPool);
        if (error != null)
            return (false, false, error);
        var useResult = await connection.UseConnection(activityFunc, activityPredicateFunc, Cancellation);
        Put(_connections, connectionPool, connection, useResult);
        return useResult;

        PipeConnectionGroup<PipeClientConnection> CreateNewConnectionPool(string name)
        {
            _logger.LogInformation("created connection pool {PoolName} of type {Type}", name, "client");
            return new(_logger, name, Settings.Instances, CreateNewConnection, Settings.ConnectionRetryTimeout, Settings.ConnectionRetryTimeout);
        }

        PipeClientConnection CreateNewConnection(int id, string name)
            => new(_logger, _meter, id, name, Settings.ConnectionTimeout, Settings.ConnectionExpiryTimeout);
    }

    public override void Cleanup()
    {
        var currentTime = DateTime.UtcNow;
        Release(_connections, ShouldCleanupClient, "client", "expired");

        bool ShouldCleanupClient(IPipeConnection connection)
            => !connection.InUse && connection.VerifyIfExpired(currentTime);
    }    

    public override void Dispose()
    {
        base.Dispose();
        Stop(_connections, "client");
    }
}