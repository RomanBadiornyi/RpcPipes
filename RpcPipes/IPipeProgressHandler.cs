namespace RpcPipes;

public interface IPipeProgressHandler<TOut>
    where TOut : IPipeProgress
{
    void StartMessageHandling(Guid messageId, IPipeProgressReporter<TOut> progressReporter);    
    void StartMessageExecute(Guid messageId, object message);
    TOut GetProgress(Guid messageId);
    void EndMessageExecute(Guid messageId);
    void EndMessageHandling(Guid messageId);
}
