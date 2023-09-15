using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

internal class PipeClientConnection : PipeConnection<NamedPipeClientStream>
{
    private readonly ILogger _logger;
    private readonly Counter<int> _clientConnectionsCounter;

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
        if (Connected && Connection is { IsConnected: true })
            return (Connected, null);
        else
        {
            Disconnect("connection dropped");
            Connection = new NamedPipeClientStream(".", Name, Direction, Options);
        }

        using var connectionCancellationSource = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        connectionCancellationSource.CancelAfter(ConnectionTimeout);
        try
        {
            Connected = true;
            await Connection.ConnectAsync(Timeout.Infinite, connectionCancellationSource.Token);
            _clientConnectionsCounter.Add(1);
            _logger.LogDebug("connected {Type} pipe of stream pipe {Pipe}", "client", Name);
            return (Connected, null);
        }
        catch (OperationCanceledException ex)
        {
            Connected = false;
            return (Connected, new TimeoutException("client connection timeout", ex));
        }
        catch (IOException ex)
        {
            Connected = false;
            _logger.LogDebug(ex, "connection to {Type} stream pipe {Pipe} got interrupted", "client", Name);
            return (Connected, ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            Connected = false;
            _logger.LogDebug(ex, "connection to {Type} stream pipe {Pipe} got Unauthorized error", "client", Name);
            return (Connected, ex);
        }
        catch (Exception ex)
        {
            Connected = false;
            _logger.LogError(ex, "connection to {Type} stream pipe {Pipe} got unhandled error", "client", Name);
            return (Connected, ex);
        }
    }

    public override void Disconnect(string reason)
    {
        if (Connection == null)
        {
            Connected = false;
            return;
        }

        try
        {
            try
            {
                if (Connected)
                {
                    _logger.LogDebug("disconnected {Type} pipe of stream pipe {Pipe}, reason '{Reason}'",
                        "client", Name, reason);
                    _clientConnectionsCounter.Add(-1);
                    Connection.Close();
                }                
            }
            finally
            {
                Connected = false;
                Connection.Dispose();
                Connection = null;
            }
        }
        catch (Exception e)
        {
            _logger.LogError(e, "unhandled error occurred when handling {Type} stream pipe {Pipe} got unhandled error", "client", Name);
        }
    }
}