using System.Collections.Concurrent;
using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeMessageHandlers;

public class PipeMessageHandler :
    IPipeMessageHandler<PipeRequest, PipeReply>,
    IPipeHeartbeatReporter<PipeState>
{
    private readonly ConcurrentDictionary<object, (DateTime Started, TimeSpan Delay)> _handlingMessages = new();
    
    public async Task<PipeReply> HandleRequest(PipeRequest message, CancellationToken cancellation)
    {
        _handlingMessages.TryAdd(message, (DateTime.Now, TimeSpan.FromSeconds(message.DelaySeconds)));
        if (message.DelaySeconds > 0)
            await Task.Delay(TimeSpan.FromSeconds(message.DelaySeconds), cancellation);
        var reply = new PipeReply(message.Message);
        _handlingMessages.TryRemove(message, out _);
        return reply;
    }

    public virtual PipeMessageHeartbeat<PipeState> HeartbeatMessage(object message)
    {
        if (_handlingMessages.TryGetValue(message, out var handle))
        {
            var runningTime = (DateTime.Now - handle.Started).TotalSeconds;
            return new PipeMessageHeartbeat<PipeState> 
            { 
                Progress = runningTime / handle.Delay.TotalSeconds,
                RequestState = new PipeState("", "")
            };
        }
        return null;
    }    
}