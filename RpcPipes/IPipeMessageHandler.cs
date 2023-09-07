namespace RpcPipes;

public interface IPipeMessageHandler<in T, TOut>
{
    Task<TOut> HandleRequest(T message, CancellationToken cancellation);
}