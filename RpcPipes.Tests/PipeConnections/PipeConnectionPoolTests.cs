using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;
using NUnit.Logger;
using RpcPipes.PipeConnections;

namespace RpcPipes.Tests.PipeConnections;

[TestFixture]
public class PipeConnectionPoolTests
{
    private readonly ILogger _logger = new NUnitLogger(nameof(PipeConnectionPoolTests));
    private readonly Meter _meter = new(nameof(PipeConnectionPoolTests));    

    private PipeConnectionPool _connectionPool;
    private CancellationTokenSource _cancellationSource;

    [SetUp]
    public void SetUp()
    {
        _cancellationSource = new CancellationTokenSource();
        if (!Debugger.IsAttached)
        {
            _cancellationSource.CancelAfter(TimeSpan.FromSeconds(30));
        }
    }

    [TearDown]
    public void ClearConnectionPool()
    {
        _connectionPool?.Client.Dispose();
        _connectionPool?.Server.Dispose();
    }

    [Test]
    public async Task UseClientServerConnection_WhenConnectionsDoesNotExist_NewConnectionsCreated()
    {
        _connectionPool = new PipeConnectionPool(_logger, _meter, 1, 1024, _cancellationSource.Token)
        {
            ConnectionExpiryTimeout = TimeSpan.FromSeconds(600)
        };
        var serverTask = _connectionPool.Server.UseConnection("PipeConnectionPoolTests", null, async stream => {
            await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
            return false;
        }).AsTask();
        var receivedBuffer = new byte[10];
        var clientTask = _connectionPool.Client.UseConnection("PipeConnectionPoolTests", null, async stream => {
            using var readWithTimeout = new CancellationTokenSource();
            var _ = await stream.ReadAsync(receivedBuffer, 0, 10, readWithTimeout.Token);
        }).AsTask();
        await Task.WhenAll(serverTask, clientTask);
        Assert.That(_connectionPool.Client.Connections.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
        Assert.That(_connectionPool.Server.Connections.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));

        _connectionPool.Client.Dispose();
        _connectionPool.Server.Dispose();

        Assert.That(receivedBuffer[5], Is.EqualTo(5));
        Assert.That(_connectionPool.Client.Connections.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(0));
        Assert.That(_connectionPool.Server.Connections.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(0));
    }

    [Test]
    public async Task UseClientServerConnection_WhenServerDisconnects_ClientShowsDisconnected()
    {
        _connectionPool = new PipeConnectionPool(_logger, _meter, 1, 1024, _cancellationSource.Token)
        {
            ConnectionExpiryTimeout = TimeSpan.FromSeconds(600)
        };
        var serverTask = _connectionPool.Server.UseConnection("PipeConnectionPoolTests", null, async stream => {
            await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
            return false;
        }).AsTask();
        var receivedBuffer = new byte[10];
        var clientTask = _connectionPool.Client.UseConnection("PipeConnectionPoolTests", null, async stream => {
            using var readWithTimeout = new CancellationTokenSource();
            var _ = await stream.ReadAsync(receivedBuffer, 0, 10, readWithTimeout.Token);
        }).AsTask();
        await Task.WhenAll(serverTask, clientTask);

        foreach (var connection in _connectionPool.Client.Connections)
        {
            connection.Disconnect("test");
        }
        foreach (var connection in _connectionPool.Server.Connections)
        {
            var useConnection = await _connectionPool.Server.UseConnection("PipeConnectionPoolTests", null, async stream => {                
                await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
                return false;
            });
            Assert.That(useConnection.Error, Is.Not.Null);
            Assert.That(connection.VerifyIfConnected(), Is.False);
        }
    }

    [Test]
    public async Task UseClientServerConnection_WhenMultipleUseCalls_ConnectionSelectedViaRoundRobin()
    {
        _connectionPool = new PipeConnectionPool(_logger, _meter, 3, 1024, _cancellationSource.Token)
        {
            ConnectionExpiryTimeout = TimeSpan.FromSeconds(600)
        };
        var connections = new ConcurrentDictionary<Stream, int>();
        for (var i = 0; i < 3; i++)
        {
            var readWriteTasks = new List<Task>();
            var readWriteHandle = new ManualResetEventSlim(false); 
            var connectedServer = 0;

            lock (readWriteHandle)
            {                
                for (var j = 0; j < _connectionPool.Instances; j++)
                {
                    var writeHandle = new ManualResetEventSlim(false);
                    var serverTask = Task.Run(async () => 
                    {
                        await _connectionPool.Server.UseConnection("PipeConnectionPoolTests", null, async stream => 
                        {
                            var connected = Interlocked.Increment(ref connectedServer);
                            if (connected == _connectionPool.Instances)
                                lock (readWriteHandle)
                                    Monitor.Pulse(readWriteHandle);
                            if (!readWriteHandle.Wait(TimeSpan.FromSeconds(30)))
                                throw new InvalidOperationException("no connection signal in server");                
                            connections.AddOrUpdate(stream, 1, (_, c) => c + 1);
                            await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
                            //do not exit from function until read notifies that it received message
                            if (!writeHandle.Wait(TimeSpan.FromSeconds(30)))
                                throw new InvalidOperationException("no client write signal");                        
                            return false;
                        });
                    });
                    var receivedBuffer = new byte[10];
                    var clientTask = Task.Run(async () => 
                    {
                        await _connectionPool.Client.UseConnection("PipeConnectionPoolTests", null, async stream => 
                        {                   
                            if (!readWriteHandle.Wait(TimeSpan.FromSeconds(30)))
                                throw new InvalidOperationException("no connection signal in client");                
                            _ = await stream.ReadAsync(receivedBuffer, 0, 5, _cancellationSource.Token);
                            //notify server that message has been received 
                            writeHandle.Set();                        
                        });
                    });                                
                    readWriteTasks.Add(Task.WhenAll(serverTask, clientTask));
                }
                if (!Monitor.Wait(readWriteHandle, TimeSpan.FromSeconds(30)))
                    throw new InvalidOperationException("no connection signal");                                
                //once we receive signal that all connections got connected and ready to start handling messages
                //signal them to proceed
                readWriteHandle.Set();
            }            
            await Task.WhenAll(readWriteTasks);
        }

        Assert.That(_connectionPool.Client.Connections.ToList(), Has.Count.EqualTo(_connectionPool.Instances));
        Assert.That(_connectionPool.Server.Connections.ToList(), Has.Count.EqualTo(_connectionPool.Instances));
        //verify all connections have been used equal number of times
        Assert.That(connections.Values.Distinct().ToList(), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task UseClientServerConnection_WhenAttemptsToClearConnectionsInUse_NotCleaned()
    {
        _connectionPool = new PipeConnectionPool(_logger, _meter, 3, 1024, _cancellationSource.Token)
        {
            ConnectionExpiryTimeout = TimeSpan.FromSeconds(600)
        };
        var serverTask = _connectionPool.Server.UseConnection("PipeConnectionPoolTests", null, async stream => {
            await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
            return false;
        }).AsTask();
        var receivedBuffer = new byte[10];
        var clientTask = _connectionPool.Client.UseConnection("PipeConnectionPoolTests", null, async stream => {
            using var readWithTimeout = new CancellationTokenSource();
            var _ = await stream.ReadAsync(receivedBuffer, 0, 10, readWithTimeout.Token);
        }).AsTask();
        await Task.WhenAll(serverTask, clientTask);
        Assert.That(_connectionPool.Client.Connections.Where(c => c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
        Assert.That(_connectionPool.Server.Connections.Where(c => c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
        _connectionPool.Server.Cleanup();
        _connectionPool.Client.Cleanup();
        Assert.That(_connectionPool.Client.Connections.Where(c => c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
        Assert.That(_connectionPool.Server.Connections.Where(c => c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
    }

    [Test]
    public async Task UseClientServerConnection_WhenServerThrowsException_AllConnectionsDisconnected()
    {
        _connectionPool = new PipeConnectionPool(_logger, _meter, 1, 1024, _cancellationSource.Token)
        {
            ConnectionExpiryTimeout = TimeSpan.FromSeconds(600)
        };
        var receiveSync = new ManualResetEventSlim(false);
        var serverTask = _connectionPool.Server.UseConnection("PipeConnectionPoolTests", null, async stream => {
            await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
            receiveSync.Wait(_cancellationSource.Token);
            throw new InvalidOperationException();
        }).AsTask();
        var receivedBuffer = new byte[10];
        var clientTask = _connectionPool.Client.UseConnection("PipeConnectionPoolTests", null, async stream => {
            using var readWithTimeout = new CancellationTokenSource();
            var _ = await stream.ReadAsync(receivedBuffer, 0, 10, readWithTimeout.Token);
            receiveSync.Set();
            throw new InvalidOperationException();
        }).AsTask();
        await Task.WhenAll(serverTask, clientTask);
        Assert.That(_connectionPool.Client.Connections.ToList(), Has.Count.EqualTo(1));
        Assert.That(_connectionPool.Server.Connections.ToList(), Has.Count.EqualTo(1));
        Assert.That(_connectionPool.Client.Connections.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(0));
        Assert.That(_connectionPool.Server.Connections.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(0));
    }

    [Test]
    public async Task UseClientServerConnection_WhenSomeConnectionsBusy_FreeConnectionsPicked()
    {
        _connectionPool = new PipeConnectionPool(_logger, _meter, 4, 1024, _cancellationSource.Token)
        {
            ConnectionExpiryTimeout = TimeSpan.FromSeconds(600)
        };
        using var testTimeout = new CancellationTokenSource();
        var clientWaitHandles = new List<AutoResetEvent>();
        var serverReceive = new AutoResetEvent(false);
        var serverTasks = new List<Task>();
        var clientTasks = new List<Task>();
        var receivedMessages = 0;
        for (var i = 0; i < 4; i++)
        {
            var timeout = testTimeout;
            var clientRelease = new AutoResetEvent(false);
            serverTasks.Add(Task.Run(async () => {
                var receivedBuffer = new byte[10];
                while (!timeout.IsCancellationRequested)
                {
                    await _connectionPool.Server.UseConnection("PipeConnectionPoolTests", null, async stream => {
                        var _ = await stream.ReadAsync(receivedBuffer, 0, 10, timeout.Token);
                        return false;
                    });
                    Interlocked.Increment(ref receivedMessages);
                    serverReceive.Set();
                }
            }, CancellationToken.None));
            clientTasks.Add(Task.Run(async () => {
                while (!timeout.IsCancellationRequested)
                {
                    await _connectionPool.Client.UseConnection("PipeConnectionPoolTests", null, async stream => {
                        WaitHandleWithCancellation(clientRelease, timeout.Token);
                        await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), timeout.Token);
                    });
                }
            }, CancellationToken.None));
            clientWaitHandles.Add(clientRelease);
        }
        testTimeout.CancelAfter(TimeSpan.FromSeconds(30));

        clientWaitHandles[0].Set();
        WaitHandleWithCancellation(serverReceive, testTimeout.Token);
        clientWaitHandles[1].Set();
        WaitHandleWithCancellation(serverReceive, testTimeout.Token);
        clientWaitHandles[2].Set();
        WaitHandleWithCancellation(serverReceive, testTimeout.Token);
        clientWaitHandles[3].Set();
        WaitHandleWithCancellation(serverReceive, testTimeout.Token);
        Assert.That(receivedMessages, Is.EqualTo(4));

        //now simulate situation where client 1 and 3 slow in communication, but 0 and 2 are fast
        //server connections should be used accordingly and all messages should be processed
        clientWaitHandles[0].Set();
        WaitHandleWithCancellation(serverReceive, testTimeout.Token);
        clientWaitHandles[2].Set();
        WaitHandleWithCancellation(serverReceive, testTimeout.Token);
        clientWaitHandles[0].Set();
        WaitHandleWithCancellation(serverReceive, testTimeout.Token);
        clientWaitHandles[2].Set();
        WaitHandleWithCancellation(serverReceive, testTimeout.Token);
        Assert.That(receivedMessages, Is.EqualTo(8));

        testTimeout.Cancel();
        _connectionPool.Client.Dispose();
        _connectionPool.Server.Dispose();
        await Task.WhenAll(clientTasks.Concat(serverTasks));
    }

    private void WaitHandleWithCancellation(AutoResetEvent eventHandle, CancellationToken token)
    {
        while (!eventHandle.WaitOne(TimeSpan.FromMilliseconds(100)))
        {
            token.ThrowIfCancellationRequested();
        }
    }
}