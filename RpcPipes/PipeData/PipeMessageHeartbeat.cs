namespace RpcPipes.PipeHeartbeat;

public class PipeMessageHeartbeat<T>
{
    public double Progress { get; set; }
    public T RequestState { get; set; }
}
