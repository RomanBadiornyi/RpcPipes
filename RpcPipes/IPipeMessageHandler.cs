namespace RpcPipes.Transport;

public interface IPipeMessageHandler<in T, TOut>
{
    Task<TOut> HandleRequest(Guid messageId, T message, CancellationToken token);
}

public interface IPipeProgressHandler<out TOut>
    where TOut : IPipeProgress
{
    TOut GetProgress(ProgressToken token, bool isCompleted);    
}

public interface IPipeProgressReceiver<in TP>
    where TP : IPipeProgress
{
    Task ReceiveProgress(TP progress);
}

public interface IPipeMessageSerializer
{
    Task Serialize<T>(T message, Stream stream, CancellationToken token);
    ValueTask<T> Deserialize<T>(Stream stream, CancellationToken token);
}