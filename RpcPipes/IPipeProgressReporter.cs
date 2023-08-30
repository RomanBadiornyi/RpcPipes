namespace RpcPipes;

public interface IPipeProgressReporter<out TOut>
    where TOut : IPipeProgress
{
    TOut GetProgress(object message);
}