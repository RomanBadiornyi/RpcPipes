using System.Collections.Concurrent;

namespace RpcPipes.Models.PipeProgress;

public class PipeProgressReceiver : IPipeProgressReceiver<ProgressMessage>
{
    public ConcurrentBag<ProgressMessage> ProgressMessages { get; }

    public PipeProgressReceiver(ConcurrentBag<ProgressMessage> progressMessages)
    {
        ProgressMessages = progressMessages;
    }
    
    public Task ReceiveProgress(ProgressMessage progress)
    {
        ProgressMessages.Add(progress);
        return Task.CompletedTask;
    }
}