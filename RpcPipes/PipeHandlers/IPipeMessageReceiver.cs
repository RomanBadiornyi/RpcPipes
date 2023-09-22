using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHandlers;

public interface IPipeMessageReceiver
{
    string Pipe { get; }
    Task ServerTask { get; }
    Task<bool> ReceiveMessage(PipeProtocol protocol, CancellationToken cancellation);
}