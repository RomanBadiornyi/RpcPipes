using System.Collections.Concurrent;
using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeMessageHandlers;

public class PipeMessageHandler :
    IPipeMessageHandler<RequestMessage, ReplyMessage>,
    IPipeHeartbeatReporter<HeartbeatMessage>
{
    private readonly ConcurrentDictionary<object, (DateTime Started, TimeSpan Delay)> _handlingMessages = new();
    
    public async Task<ReplyMessage> HandleRequest(RequestMessage message, CancellationToken token)
    {
        _handlingMessages.TryAdd(message, (DateTime.Now, TimeSpan.FromSeconds(message.DelaySeconds)));
        if (message.DelaySeconds > 0)
            await Task.Delay(TimeSpan.FromSeconds(message.DelaySeconds), token);
        var reply = new ReplyMessage(message.Message);
        _handlingMessages.TryRemove(message, out _);
        return reply;
    }

    public virtual HeartbeatMessage HeartbeatMessage(object message)
    {
        if (_handlingMessages.TryGetValue(message, out var handle))
        {
            var runningTime = (DateTime.Now - handle.Started).TotalSeconds;
            return new HeartbeatMessage(runningTime / handle.Delay.TotalSeconds, string.Empty);
        }
        return null;
    }    
}