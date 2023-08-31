namespace RpcPipes;

public class PipeMessageRequest<T>
{
    public T Request { get; set; }
    public TimeSpan ProgressFrequency { get; set; }
    public TimeSpan Deadline { get; set; } 
}