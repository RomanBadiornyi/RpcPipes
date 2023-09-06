namespace RpcPipes.PipeData;

public class PipeMessageRequest<T>
{
    public T Request { get; set; }
    public TimeSpan Heartbeat { get; set; }
    public TimeSpan Deadline { get; set; } 
}