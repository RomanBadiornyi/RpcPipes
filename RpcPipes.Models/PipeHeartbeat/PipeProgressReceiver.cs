using System.Collections.Concurrent;

namespace RpcPipes.Models.PipeHeartbeat;

public class PipeHeartbeatReceiver : IPipeHeartbeatReceiver<HeartbeatMessage>
{
    public ConcurrentBag<HeartbeatMessage> ProgressMessages { get; }

    public PipeHeartbeatReceiver(ConcurrentBag<HeartbeatMessage> progressMessages)
    {
        ProgressMessages = progressMessages;
    }
    
    public Task OnHeartbeatMessage(HeartbeatMessage progress)
    {
        ProgressMessages.Add(progress);
        return Task.CompletedTask;
    }
}