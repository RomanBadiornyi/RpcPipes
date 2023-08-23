using System.Collections.Concurrent;
using RpcPipes.Transport;

namespace RpcPipes.Models;

public class PipeMessageHandler :
    IPipeMessageHandler<RequestMessage, ReplyMessage>,
    IPipeProgressHandler<ProgressMessage>
{
    private readonly ConcurrentDictionary<Guid, (DateTime Started, TimeSpan Delay)> _handlingMessages = new();
    
    public async Task<ReplyMessage> HandleRequest(Guid messageId, RequestMessage message, CancellationToken token)
    {
        _handlingMessages.TryAdd(messageId, (DateTime.Now, TimeSpan.FromSeconds(message.DelatySeconds)));
        if (message.DelatySeconds > 0)
        {
            await Task.Delay(TimeSpan.FromSeconds(message.DelatySeconds), token);
        }
        var reply = new ReplyMessage(message.Message);
        _handlingMessages.TryRemove(messageId, out _);
        return reply;
    }

    public ProgressMessage GetProgress(ProgressToken token, bool isCompleted)
    {
        if (!_handlingMessages.TryGetValue(token.Id, out var handle))
        {
            return new ProgressMessage(isCompleted ? 1 : 0, string.Empty);            
        }
        var runningTime = (DateTime.Now - handle.Started).TotalSeconds;
        return new ProgressMessage(runningTime / handle.Delay.TotalSeconds, string.Empty);
    }    
}