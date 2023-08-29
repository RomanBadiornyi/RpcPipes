namespace RpcPipes;

public interface IPipeMessageHandler<in T, TOut>
{
    Task<TOut> HandleRequest(T message, CancellationToken token);
}

public interface IPipeProgressReporter<out TOut>
    where TOut : IPipeProgress
{
    TOut GetProgress(object message);
}

public interface IPipeProgressHandler<TOut>
    where TOut : IPipeProgress
{
    void StartMessageHandling(Guid messageId, IPipeProgressReporter<TOut> progressReporter);    
    void StartMessageExecute(Guid messageId, object message);
    TOut GetProgress(Guid messageId);
    void EndMessageExecute(Guid messageId);
    void EndMessageHandling(Guid messageId);
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