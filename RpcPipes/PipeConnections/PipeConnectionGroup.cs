using System.Collections.Concurrent;

namespace RpcPipes.PipeConnections;

public class PipeConnectionGroup<T> where T: IPipeConnection
{
    public string Name { get; }
    public int Instances { get; }    
    public ConcurrentDictionary<int, T> BusyConnections { get; }
    public ConcurrentStack<T> FreeConnections { get; }
    public ConcurrentQueue<T> DisabledConnections { get; }

    public int IncorrectConnectionsDetected;

    public PipeConnectionGroup(string name, int instances, Func<int, string, T> connectionFunc)
    {
        Name = name;
        Instances = instances;
        FreeConnections = new ConcurrentStack<T>();
        DisabledConnections = new ConcurrentQueue<T>();
        BusyConnections = new ConcurrentDictionary<int, T>();
        for (var i = 0; i < instances; i++)
        {
            FreeConnections.Push(connectionFunc.Invoke(i, name));
        }
    }
}