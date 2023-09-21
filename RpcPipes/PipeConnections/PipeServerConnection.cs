using System.Diagnostics.Metrics;
using System.IO.Pipes;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public class PipeServerConnection : PipeConnection<NamedPipeServerStream>
{
    private readonly ILogger _logger;
    private readonly int _instances;
    private readonly int _buffer;
    private readonly Counter<int> _serverConnectionsCounter;

    public PipeOptions Options { get; set; } = PipeOptions.Asynchronous | PipeOptions.WriteThrough;
    public PipeDirection Direction => PipeDirection.InOut;
    public PipeTransmissionMode Transmission => PipeTransmissionMode.Byte;

    public PipeServerConnection(ILogger logger, Meter meter, int id, string name, int instances, int buffer, TimeSpan connectionExpiryTime)
        : base(id, name, connectionExpiryTime)
    {
        _logger = logger;
        _instances = instances;
        _buffer = buffer;
        _serverConnectionsCounter = meter.CreateCounter<int>("server-connections");        
    }

    protected override async Task<(bool Ok, Exception Error)> TryConnect(CancellationToken cancellation)
    {
        if (Connected && Connection is { IsConnected: true })
            return (Connected, null);
        else
        {
            Disconnect("connection dropped");
            Connection = new NamedPipeServerStream(Name, Direction, _instances, Transmission, Options, _buffer, _buffer);
        }

        try
        {
            Connected = true;
            await Connection.WaitForConnectionAsync(cancellation);
            _serverConnectionsCounter.Add(1);
            _logger.LogDebug("connected {Type} pipe of stream pipe {Pipe}", "server", Name);
            return (Connected, null);
        }
        catch (OperationCanceledException ex)
        {
            Connected = false;
            _logger.LogDebug("connection to {Type} stream pipe {Pipe} closed", "server", Name);
            return (Connected, ex);

        }
        catch (IOException ex)
        {
            Connected = false;
            _logger.LogDebug(ex, "connection to {Type} stream pipe {Pipe} got interrupted", "server", Name);
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
            _logger.LogError(ex, "connection to {Type} stream pipe {Pipe} got unhandled error", "server", Name);
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
                        "server", Name, reason);
                    _serverConnectionsCounter.Add(-1);
                    if (Connection.IsConnected)
                        Connection.Disconnect();
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
            _logger.LogError(e, "unhandled error occurred when handling {Type} stream pipe {Pipe} got unhandled error", "server", Name);
        }
    }
}