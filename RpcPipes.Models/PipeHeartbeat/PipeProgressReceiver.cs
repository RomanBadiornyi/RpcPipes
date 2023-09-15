using System.Collections.Concurrent;
using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeHeartbeat;

public class PipeHeartbeatReceiver : IPipeHeartbeatReceiver<PipeHeartbeatMessage>
{
    public ConcurrentBag<PipeHeartbeatMessage> ProgressMessages { get; }

    public PipeHeartbeatReceiver(ConcurrentBag<PipeHeartbeatMessage> progressMessages)
    {
        ProgressMessages = progressMessages;
    }
    
    public Task OnHeartbeatMessage(PipeHeartbeatMessage progress)
    {
        ProgressMessages.Add(progress);
        return Task.CompletedTask;
    }
}