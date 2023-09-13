using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

internal class PipeClientConnection : IPipeConnection<NamedPipeClientStream>
{
    private ILogger _logger;
    private Counter<int> _clientConnectionsCounter;

    private SemaphoreSlim _connectionLock = new SemaphoreSlim(1);

    private NamedPipeClientStream _connection;
    private volatile bool _connected;
    private volatile bool _inUse;
    private volatile bool _isReleased;

    public int Id { get; }
    public string Name { get; }
    public bool Connected => _connected;
    public bool InUse => _inUse;
    public bool Released => _isReleased;

    public DateTime LastUsedAt { get; private set; }
    public TimeSpan ConnectionTimeout { get; }
    public TimeSpan ConnectionRetryTimeout { get; }
    public PipeOptions Options { get; set; } = PipeOptions.Asynchronous | PipeOptions.WriteThrough;
    public PipeDirection Direction => PipeDirection.InOut;

    public PipeClientConnection(ILogger logger, Meter meter, int id, string name, TimeSpan connectionTimeout, TimeSpan connectionRetryTimeout)
    {
        _logger = logger;
        _clientConnectionsCounter = meter.CreateCounter<int>("client-connections");

        Id = id;
        Name = name;
        LastUsedAt = DateTime.UtcNow;
        ConnectionTimeout = connectionTimeout;
        ConnectionRetryTimeout = connectionRetryTimeout;
    }

    public async Task<(bool Used, Exception Error)> UseConnection(Func<NamedPipeClientStream, Task> useFunc, CancellationToken cancellation)
    {
        await _connectionLock.WaitAsync(cancellation);
        try
        {
            _inUse = true;
            if (_connection == null || !_connected)
                _connection = new NamedPipeClientStream(".", Name, Direction, Options);

            var (connected, error) = await TryConnect(ConnectionTimeout, cancellation);            
            if (connected)
            {
                LastUsedAt = DateTime.UtcNow;
                await useFunc.Invoke(_connection);
                LastUsedAt = DateTime.UtcNow;
                return (true, error);
            }
            //probably server run out of available connections
            //hence why apply delay here
            await Task.Delay(ConnectionRetryTimeout, cancellation);
            return (false, error);
        }
        catch (Exception ex)
        {                 
            Disconnect();
            return (true, ex);
        }
        finally
        {            
            _connectionLock.Release();
            _inUse = false;
        }
    }

    public async Task<bool> TryReleaseConnection(int timeoutMilliseconds, CancellationToken cancellation)
    {
        var acquired = await _connectionLock.WaitAsync(timeoutMilliseconds, cancellation);
        if (!acquired)
            return false;
        try
        {
            Disconnect();
            _isReleased = !_connected;
            return !_connected;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    private async Task<(bool Connected, Exception Error)> TryConnect(TimeSpan timeout, CancellationToken cancellation)
    {
        if (_connected)
            return (_connected, null);

        using var connectionCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        connectionCancellationSource.CancelAfter(timeout);
        try
        {
            _connected = true;
            await _connection.ConnectAsync(Timeout.Infinite, connectionCancellationSource.Token);
            _clientConnectionsCounter.Add(1);
            _logger.LogDebug("connected {Type} pipe of stream pipe {Pipe}", "client", Name);
            return (_connected, null);
        }
        catch (OperationCanceledException ex)
        {
            _connected = false;
            return (_connected, ex);
        }
        catch (IOException ex)
        {
            _connected = false;
            _logger.LogDebug(ex, "connection to {Type} stream pipe {Pipe} got interrupted", "client", Name);
            return (_connected, ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            _connected = false;
            _logger.LogDebug(ex, "connection to {Type} stream pipe {Pipe} got Unauthorized error", "client", Name);
            return (_connected, ex);
        }
        catch (Exception ex)
        {
            _connected = false;
            _logger.LogError(ex, "connection to {Type} stream pipe {Pipe} got unhandled error", "client", Name);
            return (_connected, ex);
        }
    }

    private void Disconnect()
    {
        if (_connection == null)
            return;

        try
        {
            try
            {
                if (_connected)
                {
                    _clientConnectionsCounter.Add(-1);
                    _logger.LogDebug("disconnected {Type} pipe of stream pipe {Pipe}", "client", Name);
                    _connection.Close();
                }
            }
            finally
            {
                _connected = false;
                _connection.Dispose();
                _connection = null;
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred when handling {Type} stream pipe {Pipe} got unhandled error", "client", Name);
        }
    }
}
