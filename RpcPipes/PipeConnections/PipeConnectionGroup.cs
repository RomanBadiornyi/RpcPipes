using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace RpcPipes.PipeConnections;

public class PipeConnectionGroup<T> : IDisposable
    where T: IPipeConnection
{
    private class BrokenConnection
    {
        public T Connection { get; set; }
        public DateTime DisableTime { get; set; }
        public Exception Error { get; set; }
    }

    private class ConnectionWaitHandle
    {
        public int IsActive = 1;
        public EventWaitHandle WaitHandle = new(false, EventResetMode.ManualReset);
    }

    private ILogger _logger;
    private bool _completed;

    public string Name { get; }

    private ConcurrentQueue<ConnectionWaitHandle> WaitHandles { get; }
    private ConcurrentStack<T> FreeConnections { get; }
    private ConcurrentQueue<T> DisabledConnections { get; }
    private ConcurrentDictionary<T, BrokenConnection> BrokenConnections { get; }
    private ConcurrentDictionary<int, T> AllConnections { get; }

    public IEnumerable<T> Free => FreeConnections;
    public IEnumerable<T> Connections => AllConnections.Values;

    public int Instances { get; }
    public DateTime LastUsed { get; private set; }
    public TimeSpan ConnectionRetryTime { get; }
    public TimeSpan ConnectionResumeTime { get; }    

    public PipeConnectionGroup(
        ILogger logger, string name, int instances, Func<int, string, T> connectionFunc, TimeSpan connectionRetryTime, TimeSpan connectionResumeTime)
    {
        _logger = logger;
        _completed = false;

        Name = name;

        WaitHandles = new ConcurrentQueue<ConnectionWaitHandle>();
        FreeConnections = new ConcurrentStack<T>();
        DisabledConnections = new ConcurrentQueue<T>();
        AllConnections = new ConcurrentDictionary<int, T>();
        BrokenConnections = new ConcurrentDictionary<T, BrokenConnection>();

        ConnectionRetryTime = connectionRetryTime;
        ConnectionResumeTime = connectionResumeTime;

        Instances = instances;
        for (var i = 0; i < instances; i++)
        {
            AllConnections.TryAdd(i, connectionFunc(i, name));
        }
        FreeConnections.PushRange(AllConnections.Values.ToArray());
        SignalNewConnection();
    }

    public bool IsUnusedPool(double usageTimeoutMilliseconds)
        => FreeConnections.Count == 0 && IsNotActive() && IsExpired(usageTimeoutMilliseconds);

    public bool IsExpired(double usageTimeoutMilliseconds)
        => (DateTime.UtcNow - LastUsed).TotalMilliseconds > usageTimeoutMilliseconds;

    public bool IsNotActive()
        => WaitHandles.Count == 0;

    public async ValueTask<(T Connection, AggregateException Error)> BorrowConnection(int timeout)
    {
        //asynchronously wait for connection to appear in free connection stack (most recently used) or disabled connection
        T connection = default;
        ConnectionWaitHandle connectionHandle = null;
        while (connection == null)
        {
            if (!_completed &&
                !FreeConnections.TryPop(out connection) &&
                !DisabledConnections.TryDequeue(out connection))
            {
                if (connectionHandle == null || connectionHandle.IsActive == 0)
                {
                    connectionHandle = new ConnectionWaitHandle();
                    WaitHandles.Enqueue(connectionHandle);
                }                    

                var tcs = new TaskCompletionSource<bool>();
                var waitRegister = ThreadPool.RegisterWaitForSingleObject(connectionHandle.WaitHandle,
                    (state, timedOut) => ((TaskCompletionSource<bool>)state).TrySetResult(!timedOut), tcs, timeout, true);
                    
                var success = await tcs.Task;                                        
                if (!success && DateTime.UtcNow - LastUsed > ConnectionRetryTime)
                {
                    //if no connections has been updated within timeout - return null
                    break;
                }                    
            }
        }
        AggregateException errors = null;
        if (connection == null)
            errors = new AggregateException(BrokenConnections.Values.Select(v => v.Error));
        return (connection, errors);
    }

    public void ReturnConnection(T connection, Exception error)
    {
        //connections with errors go to broken connection pool and will become available once restored
        if (error != null)
        {
            var reason = error is OperationCanceledException ? "cancelled" : $"error: {error.Message}";
            connection.Disconnect(reason);
            if (connection.ConnectionErrors > 1)
            {
                _logger.LogInformation("paused connection {Pipe} due to connection error {Reason}", connection.Name, reason);
                var brokenConnection = new BrokenConnection { Connection = connection, DisableTime = DateTime.UtcNow, Error = error };
                BrokenConnections.TryAdd(connection, brokenConnection);
            }
            else
            {
                _logger.LogInformation("disabled connection {Pipe} due to connection error {Reason}", connection.Name, reason);
                DisabledConnections.Enqueue(connection);
                SignalNewConnection();
            }
        }
        else
        {
            FreeConnections.Push(connection);
            SignalNewConnection();
        }
    }

    public void RestoreConnections()
    {
        //exponential wait for broken connection to restore based on number of it's connection errors
        var current = DateTime.UtcNow;
        foreach (var connection in BrokenConnections.Keys.ToList())
        {
            if (BrokenConnections.TryRemove(connection, out var brokenConnection))
            {
                var errorsCount = Math.Max(brokenConnection.Connection.ConnectionErrors, 10);
                var disabledTime = TimeSpan.FromMilliseconds(ConnectionResumeTime.TotalMilliseconds * Math.Pow(2, errorsCount));
                if (current > brokenConnection.DisableTime + disabledTime)
                {
                    _logger.LogInformation("restored connection {Pipe} with number of errors {ErrorsCount}", connection.Name, connection.ConnectionErrors);
                    DisabledConnections.Enqueue(brokenConnection.Connection);
                    SignalNewConnection();
                }
                else
                {
                    BrokenConnections.TryAdd(connection, brokenConnection);
                }
            }
        }
    }

    public bool DisableConnections(Func<IPipeConnection, bool> releaseCondition, string reason)
    {
        var freeConnections = new List<T>();
        while (FreeConnections.TryPop(out var connection))
        {
            if (releaseCondition.Invoke(connection))
            {
                connection.Disconnect(reason);
                DisabledConnections.Enqueue(connection);
                SignalNewConnection();
            }
            else
            {
                freeConnections.Add(connection);
            }
        }
        foreach(var connection in freeConnections)
        {
            FreeConnections.Push(connection);
            SignalNewConnection();
        }
        return freeConnections.Count == 0;
    }

    private void SignalNewConnection()
    {
        LastUsed = DateTime.UtcNow;

        var signaled = false;
        while (!signaled)
        {
            if (WaitHandles.TryDequeue(out var connectionHandle))
            {
                if (connectionHandle.IsActive == 1 && 
                    Interlocked.CompareExchange(ref connectionHandle.IsActive, 0, 1) == 1)
                {
                    connectionHandle.WaitHandle.Set();
                    signaled = true;
                }
            }
            else 
            {
                signaled = true;
            }
        }
    }

    public void Dispose()
    {
        _completed = true;
        while (WaitHandles.TryDequeue(out var connectionHandle))
        {
            Interlocked.CompareExchange(ref connectionHandle.IsActive, 0, 1);
            connectionHandle.WaitHandle.Set();
        }
    }
}