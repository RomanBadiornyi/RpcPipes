namespace RpcPipes.PipeConnections;

public interface IPipeConnection
{    
    string Name { get; }
    bool InUse { get; }

    int ConnectionErrors { get; }

    bool VerifyIfConnected();
    bool VerifyIfExpired(DateTime currentTime);
    
    void Disconnect(string reason);
}