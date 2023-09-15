using System.Collections.Concurrent;
using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeMessageHandlers;

public class PipeMessageHandler :
    IPipeMessageHandler<PipeRequestMessage, PipeReplyMessage>,
    IPipeHeartbeatReporter<PipeHeartbeatMessage>
{
    private readonly ConcurrentDictionary<object, (DateTime Started, TimeSpan Delay)> _handlingMessages = new();
    
    public async Task<PipeReplyMessage> HandleRequest(PipeRequestMessage message, CancellationToken cancellation)
    {
        _handlingMessages.TryAdd(message, (DateTime.Now, TimeSpan.FromSeconds(message.DelaySeconds)));
        if (message.DelaySeconds > 0)
            await Task.Delay(TimeSpan.FromSeconds(message.DelaySeconds), cancellation);
        var reply = new PipeReplyMessage(message.Message);
        _handlingMessages.TryRemove(message, out _);
        return reply;
    }

    public virtual PipeHeartbeatMessage HeartbeatMessage(object message)
    {
        if (_handlingMessages.TryGetValue(message, out var handle))
        {
            var runningTime = (DateTime.Now - handle.Started).TotalSeconds;
            return new PipeHeartbeatMessage(runningTime / handle.Delay.TotalSeconds, string.Empty);
        }
        return null;
    }    
}