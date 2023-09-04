namespace RpcPipes;

public class PipeRequestContext
{
    public TimeSpan ProgressFrequency = TimeSpan.FromSeconds(5);
    public TimeSpan Deadline = TimeSpan.FromHours(1);
}