using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public interface IPipeConnectionSettings
{
    int Instances { get; }
    int Buffer { get; }

    TimeSpan ConnectionTimeout { get; set; }
    TimeSpan ConnectionExpiryTimeout { get; set; }
    TimeSpan ConnectionRetryTimeout { get; set; }    
    TimeSpan ConnectionReleaseTimeout { get; set; }
    TimeSpan ConnectionDisposeTimeout { get; set; }
}
public class PipeConnectionPool : IPipeConnectionSettings
{
    public int Instances { get; }
    public int Buffer { get; }

    public TimeSpan ConnectionTimeout {get; set; } = TimeSpan.FromSeconds(3);
    public TimeSpan ConnectionExpiryTimeout {get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionRetryTimeout {get; set; } = TimeSpan.FromSeconds(5);
    public TimeSpan ConnectionDisposeTimeout {get; set; } = TimeSpan.FromSeconds(10);
    public TimeSpan ConnectionReleaseTimeout {get; set; } = TimeSpan.FromMilliseconds(500);    

    public PipeConnectionPoolClient Client { get; }
    public PipeConnectionPoolServer Server { get; }     

    public PipeConnectionPool(ILogger logger, Meter meter, int instances, int buffer, CancellationToken cancellation)        
    {
        Instances = instances;
        Buffer = buffer;
        Client = new PipeConnectionPoolClient(logger, meter, this, cancellation);
        Server = new PipeConnectionPoolServer(logger, meter, this, cancellation);
    }
}