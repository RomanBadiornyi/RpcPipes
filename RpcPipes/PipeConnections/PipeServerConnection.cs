using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

internal class PipeServerConnection : PipeConnection<NamedPipeServerStream>
{
    private ILogger _logger;
    private int _instances;
    private int _buffer;
    private Counter<int> _serverConnectionsCounter;

    public PipeOptions Options { get; set; } = PipeOptions.Asynchronous | PipeOptions.WriteThrough;
    public PipeDirection Direction => PipeDirection.InOut;
    public PipeTransmissionMode Transmission => PipeTransmissionMode.Byte;

    public PipeServerConnection(ILogger logger, Meter meter, int id, string name, int instances, int buffer, TimeSpan connectionRetryTimeout, TimeSpan connectionExpiryTime)
        : base(logger, id, name, connectionRetryTimeout, connectionExpiryTime)
    {
        _logger = logger;
        _instances = instances;
        _buffer = buffer;
        _serverConnectionsCounter = meter.CreateCounter<int>("server-connections");        
    }

    protected override async Task<(bool Ok, Exception Error)> TryConnect(CancellationToken cancellation)
    {
        if (_connected && _connection != null && _connection.IsConnected)
            return (_connected, null);
        else
        {
            Disconnect("connection dropped");
            _connection = new NamedPipeServerStream(Name, Direction, _instances, Transmission, Options, _buffer, _buffer);
        }

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

    public override void Disconnect(string reason)
    {
        if (_connection == null)
        {
            _connected = false;
            return;
        }

        try
        {
            try
            {
                if (_connected)
                {
                    _logger.LogDebug("disconnected {Type} pipe of stream pipe {Pipe}, reason '{Reason}'",
                        "server", Name, reason);
                    _serverConnectionsCounter.Add(-1);
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