using System.Collections.Concurrent;

namespace RpcPipes.PipeConnections;

public class PipeConnectionGroup<T> where T: IPipeConnection
{
    public string Name { get; }
    public int Instances { get; }    

    public ConcurrentStack<T> FreeConnections { get; }
    public ConcurrentQueue<T> DisabledConnections { get; }

    public PipeConnectionGroup(string name, int instances, Func<int, string, T> connectionFunc)
    {
        Name = name;
        Instances = instances;
        FreeConnections = new ConcurrentStack<T>();
        DisabledConnections = new ConcurrentQueue<T>();
        for (var i = 0; i < instances; i++)
        {
            FreeConnections.Push(connectionFunc.Invoke(i, name));
        }
    }
}