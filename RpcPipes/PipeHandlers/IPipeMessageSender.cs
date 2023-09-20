using RpcPipes.PipeMessages;
using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

public interface IPipeMessageSender<T> 
    where T : IPipeMessage
{
    Task ClientTask { get; }
    ValueTask Publish(T message);

    string TargetPipe(T message);    
    Task HandleMessage(T message, PipeProtocol protocol, CancellationToken cancellation);
    ValueTask HandleError(T message, Exception error);
}