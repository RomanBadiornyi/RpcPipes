using System.Collections.Concurrent;

namespace RpcPipes.PipeConnections;

internal class PipeConnectionGroup<T> where T: IPipeConnection
{
    public string Name { get; }
    private ConcurrentStack<T> FreeConnections { get; }
    private ConcurrentQueue<T> DisabledConnections { get; }
    private ConcurrentDictionary<int, T> AllConnections { get; }

    public IEnumerable<T> Free => FreeConnections;
    public IEnumerable<T> Disabled => DisabledConnections;
    public IEnumerable<T> Connections => AllConnections.Values;

    public PipeConnectionGroup(string name, int instances, Func<int, string, T> connectionFunc)
    {
        Name = name;
        FreeConnections = new ConcurrentStack<T>();
        DisabledConnections = new ConcurrentQueue<T>();
        AllConnections = new ConcurrentDictionary<int, T>();
        for (var i = 0; i < instances; i++)
        {
            AllConnections.TryAdd(i, connectionFunc(i, name));
        }
        FreeConnections.PushRange(AllConnections.Values.ToArray());
    }

    public bool Unused()
        => FreeConnections.Count == 0 && DisabledConnections.Count == AllConnections.Count;

    public bool BorrowConnection(out T connection)
    {
        if (!FreeConnections.TryPop(out connection))
        {
            if (!DisabledConnections.TryDequeue(out connection))
                return false;
        }
        return true;
    }

    public void ReturnConnection(T connection, bool disabled)
    {
        if (disabled)
            DisabledConnections.Enqueue(connection);
        else
            FreeConnections.Push(connection);
    }

    public void DisableConnections()
    {
        while (FreeConnections.TryPop(out var connection))
            DisabledConnections.Enqueue(connection);
    }
}