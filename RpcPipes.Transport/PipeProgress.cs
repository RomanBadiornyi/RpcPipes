namespace RpcPipes.Transport;

public interface IPipeProgress
{
    double Progress { get; }
}

public class ProgressToken
{
    public Guid Id { get; set; }
    public bool Active { get; set; }
}