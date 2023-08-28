using System.Collections.Concurrent;
using RpcPipes;

namespace RpcPipes.Models;

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