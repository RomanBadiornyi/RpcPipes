namespace RpcPipes.Models.PipeProgress;

public class PipeProgressMessageHandler : PipeProgressHandler<ProgressMessage>
{
    protected override ProgressMessage GetNotStartedProgress()
    {
        return new ProgressMessage(0, string.Empty);
    }

    protected override ProgressMessage GetCompletedProgress()
    {
        return new ProgressMessage(1, string.Empty);
    }    
}