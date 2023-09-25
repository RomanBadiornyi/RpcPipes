using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeHeartbeat;

public class PipeHeartbeatReceiverCounter : IPipeHeartbeatReceiver<PipeHeartbeatMessage>
{
    private int _progressMessages;
    public int ProgressMessages => _progressMessages;

    public Task OnHeartbeatMessage(PipeHeartbeatMessage progress)
    {
        Interlocked.Increment(ref _progressMessages);
        return Task.CompletedTask;
    }
}