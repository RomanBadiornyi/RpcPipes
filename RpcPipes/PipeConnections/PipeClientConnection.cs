using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

internal class PipeClientConnection : PipeConnection<NamedPipeClientStream>
{
    private ILogger _logger;
    private Counter<int> _clientConnectionsCounter;

    public TimeSpan ConnectionTimeout { get; }
    
    public PipeOptions Options { get; set; } = PipeOptions.Asynchronous | PipeOptions.WriteThrough;
    public PipeDirection Direction => PipeDirection.InOut;

    public PipeClientConnection(ILogger logger, Meter meter, int id, string name, TimeSpan connectionTimeout, TimeSpan connectionRetryTimeout, TimeSpan connectionExpiryTime)
        : base(logger, id, name, connectionRetryTimeout, connectionExpiryTime)
    {
        _logger = logger;
        _clientConnectionsCounter = meter.CreateCounter<int>("client-connections");
        ConnectionTimeout = connectionTimeout;
    }

    protected override async Task<(bool Ok, Exception Error)> TryConnect(CancellationToken cancellation)
    {
        if (_connected && _connection != null && _connection.IsConnected)
            return (_connected, null);
        else
        {
            Disconnect("connection dropped");
            _connection = new NamedPipeClientStream(".", Name, Direction, Options);
        }

        using var connectionCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        connectionCancellationSource.CancelAfter(ConnectionTimeout);
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
            return (_connected, new TimeoutException("client connection timeout", ex));
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
                        "client", Name, reason);
                    _clientConnectionsCounter.Add(-1);
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