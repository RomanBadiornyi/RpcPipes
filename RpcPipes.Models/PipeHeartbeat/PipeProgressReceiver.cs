using System.Collections.Concurrent;
using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeHeartbeat;

public class PipeHeartbeatReceiver : IPipeHeartbeatReceiver<PipeState>
{
    public ConcurrentBag<PipeMessageHeartbeat<PipeState>> ProgressMessages { get; }

    public PipeHeartbeatReceiver(ConcurrentBag<PipeMessageHeartbeat<PipeState>> progressMessages)
    {
        ProgressMessages = progressMessages;
    }
    
    public Task OnHeartbeatMessage(PipeMessageHeartbeat<PipeState> progress)
    {
        ProgressMessages.Add(progress);
        return Task.CompletedTask;
    }
}