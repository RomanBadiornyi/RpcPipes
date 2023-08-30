namespace RpcPipes;

public interface IPipeProgressReceiver<in TP>
    where TP : IPipeProgress
{
    Task ReceiveProgress(TP progress);
}