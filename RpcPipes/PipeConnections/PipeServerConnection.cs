using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

internal class PipeServerConnection : IPipeConnection<NamedPipeServerStream>
{
    private ILogger _logger;
    private int _instances;
    private int _buffer;
    private Counter<int> _serverConnectionsCounter;

    private SemaphoreSlim _connectionLock = new SemaphoreSlim(1);

    private NamedPipeServerStream _connection { get; set; }
    private volatile bool _connected;
    private volatile bool _inUse;
    private volatile bool _isReleased;

    public int Id { get; }
    public string Name { get; }
    public bool Connected => _connected;
    public bool InUse => _inUse;
    public bool Released => _isReleased;

    public DateTime LastUsedAt { get; private set; }
    public TimeSpan ConnectionRetryTimeout { get; }
    public PipeOptions Options { get; set; } = PipeOptions.Asynchronous | PipeOptions.WriteThrough;
    public PipeDirection Direction => PipeDirection.InOut;
    public PipeTransmissionMode Transmission => PipeTransmissionMode.Byte;

    public PipeServerConnection(ILogger logger, Meter meter, int id, string name, int instances, int buffer, TimeSpan connectionRetryTimeout)
    {
        _logger = logger;
        _instances = instances;
        _buffer = buffer;
        _serverConnectionsCounter = meter.CreateCounter<int>("server-connections");

        Id = id;
        Name = name;
        LastUsedAt = DateTime.UtcNow;
        ConnectionRetryTimeout = connectionRetryTimeout;
    }

    public async Task<(bool Used, Exception Error)> UseConnection(Func<NamedPipeServerStream, Task> useFunc, CancellationToken cancellation)
    {
        await _connectionLock.WaitAsync(cancellation);
        try
        {
            _inUse = true;
            if (_connection == null || !_connected)
                _connection = new NamedPipeServerStream(Name, Direction, _instances, Transmission, Options, _buffer, _buffer);

            var (connected, error) = await TryConnect(cancellation);
            if  (connected)
            {
                LastUsedAt = DateTime.UtcNow;
                await useFunc.Invoke(_connection);
                LastUsedAt = DateTime.UtcNow;
                return (true, error);
            }
            return (false, error);
        }
        catch (Exception ex)
        {
            //possibly we run out of available pipe instances as some other process took them away - 
            //hence why apply retry delay logic
            if (_connection == null && ex is not OperationCanceledException)
                await Task.Delay(ConnectionRetryTimeout, cancellation);                            
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

    private async Task<(bool Connected, Exception error)> TryConnect(CancellationToken cancellation)
    {
        if (_connected)
            return (_connected, null);

        try
        {
            _connected = true;
            await _connection.WaitForConnectionAsync(cancellation);
            _serverConnectionsCounter.Add(1);
            _logger.LogDebug("connected {Type} pipe of stream pipe {Pipe}", "server", Name);
            return (_connected, null);
        }
        catch (OperationCanceledException ex)
        {
            _connected = false;
            _logger.LogDebug("connection to {Type} stream pipe {Pipe} closed", "server", Name);
            return (_connected, ex);

        }
        catch (IOException ex)
        {
            _connected = false;
            _logger.LogDebug(ex, "connection to {Type} stream pipe {Pipe} got interrupted", "server", Name);
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
            _logger.LogError(ex, "connection to {Type} stream pipe {Pipe} got unhandled error", "server", Name);
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
                    _serverConnectionsCounter.Add(-1);
                    _logger.LogDebug("disconnected {Type} pipe of stream pipe {Pipe}", "server", Name);
                    if (_connection.IsConnected)
                        _connection.Disconnect();
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
            _logger.LogError(e, "unhandled error occurred when handling {Type} stream pipe {Pipe} got unhandled error", "server", Name);
        }
    }
}
