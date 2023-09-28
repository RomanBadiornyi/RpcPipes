using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeHeartbeat;

public class PipeHeartbeatReceiverCounter : IPipeHeartbeatReceiver<PipeState>
{
    private int _progressMessages;
    public int ProgressMessages => _progressMessages;

    public Task OnHeartbeatMessage(PipeMessageHeartbeat<PipeState> progress)
    {
        Interlocked.Increment(ref _progressMessages);
        return Task.CompletedTask;
    }
}