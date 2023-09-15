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
    private readonly Meter _meter = new(nameof(PipeConnectionPoolTests));
    private readonly ILogger _logger = new NUnitLogger(nameof(PipeConnectionPoolTests));

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
        _connectionPool?.DisposeAsync().AsTask().Wait();
    }

    [Test]
    public async Task UseClientServerConnection_WhenConnectionsDoesNotExist_NewConnectionsCreated()
    {
        _connectionPool = new PipeConnectionPool(_logger, _meter, 1, 1024, _cancellationSource.Token)
        {
            ConnectionExpiryTimeout = TimeSpan.FromSeconds(600)
        };
        var serverTask = _connectionPool.UseServerConnection("PipeConnectionPoolTests", null, async stream => {
            await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
        }).AsTask();
        var receivedBuffer = new byte[10];
        var clientTask = _connectionPool.UseClientConnection("PipeConnectionPoolTests", null, async stream => {
            using var readWithTimeout = new CancellationTokenSource();
            var _ = await stream.ReadAsync(receivedBuffer, 0, 10, readWithTimeout.Token);
        }).AsTask();
        await Task.WhenAll(serverTask, clientTask);
        Assert.That(_connectionPool.ConnectionsClient.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
        Assert.That(_connectionPool.ConnectionsServer.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
        await _connectionPool.DisposeAsync();

        Assert.That(receivedBuffer[5], Is.EqualTo(5));
        Assert.That(_connectionPool.ConnectionsClient.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(0));
        Assert.That(_connectionPool.ConnectionsServer.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(0));
    }

    [Test]
    public async Task UseClientServerConnection_WhenServerDisconnects_ClientShowsDisconnected()
    {
        _connectionPool = new PipeConnectionPool(_logger, _meter, 1, 1024, _cancellationSource.Token)
        {
            ConnectionExpiryTimeout = TimeSpan.FromSeconds(600)
        };
        var serverTask = _connectionPool.UseServerConnection("PipeConnectionPoolTests", null, async stream => {
            await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
        }).AsTask();
        var receivedBuffer = new byte[10];
        var clientTask = _connectionPool.UseClientConnection("PipeConnectionPoolTests", null, async stream => {
            using var readWithTimeout = new CancellationTokenSource();
            var _ = await stream.ReadAsync(receivedBuffer, 0, 10, readWithTimeout.Token);
        }).AsTask();
        await Task.WhenAll(serverTask, clientTask);

        foreach (var connection in _connectionPool.ConnectionsClient)
        {
            connection.Disconnect("test");
        }
        foreach (var connection in _connectionPool.ConnectionsServer)
        {
            var useConnection = await _connectionPool.UseServerConnection("PipeConnectionPoolTests", null, async stream => {
                await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
            });
            Assert.That(useConnection.Error, Is.Not.Null);
            Assert.That(connection.VerifyIfConnected(), Is.False);
        }
        await _connectionPool.DisposeAsync();
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
            //use wait handles to ensure that on each iteration all 3 connections from pool will be active
            var readWriteStartHandle = new ManualResetEventSlim(false);
            var readWriteTaskHandles = new List<ManualResetEventSlim>();

            for (var j = 0; j < _connectionPool.Instances; j++)
            {
                var readWriteHandle = new ManualResetEventSlim(false);
                readWriteTaskHandles.Add(readWriteHandle);

                var readWriteTask = Task.Run(() => {
                    var serverTask = _connectionPool.UseServerConnection("PipeConnectionPoolTests", null, async stream => {
                        connections.AddOrUpdate(stream, 1, (_, c) => c + 1);
                        await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
                        readWriteHandle.Set();
                        readWriteStartHandle.Wait(TimeSpan.FromSeconds(5));
                    }).AsTask();
                    var receivedBuffer = new byte[10];
                    var clientTask = _connectionPool.UseClientConnection("PipeConnectionPoolTests", null, async stream => {
                        await stream.ReadAsync(receivedBuffer, 0, 5, _cancellationSource.Token);
                        var _ = readWriteStartHandle.Wait(TimeSpan.FromSeconds(5));
                    }).AsTask();
                    return Task.WhenAll(serverTask, clientTask);
                });
                readWriteTasks.Add(readWriteTask);
            }
            WaitHandle.WaitAll(readWriteTaskHandles.Select(h => h.WaitHandle).ToArray(), TimeSpan.FromSeconds(5));
            readWriteStartHandle.Set();
            await Task.WhenAll(readWriteTasks);
        }

        Assert.That(_connectionPool.ConnectionsClient.ToList(), Has.Count.EqualTo(_connectionPool.Instances));
        Assert.That(_connectionPool.ConnectionsServer.ToList(), Has.Count.EqualTo(_connectionPool.Instances));
        //verify all connections have been used equal number of times
        Assert.That(connections.Values.Distinct().ToList(), Has.Count.EqualTo(1));

        await _connectionPool.DisposeAsync();
    }

    [Test]
    public async Task UseClientServerConnection_WhenAttemptsToClearConnectionsInUse_NotCleaned()
    {
        _connectionPool = new PipeConnectionPool(_logger, _meter, 3, 1024, _cancellationSource.Token)
        {
            ConnectionExpiryTimeout = TimeSpan.FromSeconds(600)
        };
        var serverTask = _connectionPool.UseServerConnection("PipeConnectionPoolTests", null, async stream => {
            await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
        }).AsTask();
        var receivedBuffer = new byte[10];
        var clientTask = _connectionPool.UseClientConnection("PipeConnectionPoolTests", null, async stream => {
            using var readWithTimeout = new CancellationTokenSource();
            var _ = await stream.ReadAsync(receivedBuffer, 0, 10, readWithTimeout.Token);
        }).AsTask();
        await Task.WhenAll(serverTask, clientTask);
        Assert.That(_connectionPool.ConnectionsClient.Where(c => c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
        Assert.That(_connectionPool.ConnectionsServer.Where(c => c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
        _connectionPool.Cleanup();
        Assert.That(_connectionPool.ConnectionsClient.Where(c => c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
        Assert.That(_connectionPool.ConnectionsServer.Where(c => c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(1));
        await _connectionPool.DisposeAsync();
    }

    [Test]
    public async Task UseClientServerConnection_WhenServerThrowsException_AllConnectionsDisconnected()
    {
        _connectionPool = new PipeConnectionPool(_logger, _meter, 1, 1024, _cancellationSource.Token)
        {
            ConnectionExpiryTimeout = TimeSpan.FromSeconds(600)
        };
        var receiveSync = new ManualResetEventSlim(false);
        var serverTask = _connectionPool.UseServerConnection("PipeConnectionPoolTests", null, async stream => {
            await stream.WriteAsync(Enumerable.Range(0, 10).Select(x => (byte)x).ToArray(), _cancellationSource.Token);
            receiveSync.Wait(_cancellationSource.Token);
            throw new InvalidOperationException();
        }).AsTask();
        var receivedBuffer = new byte[10];
        var clientTask = _connectionPool.UseClientConnection("PipeConnectionPoolTests", null, async stream => {
            using var readWithTimeout = new CancellationTokenSource();
            var _ = await stream.ReadAsync(receivedBuffer, 0, 10, readWithTimeout.Token);
            receiveSync.Set();
            throw new InvalidOperationException();
        }).AsTask();
        await Task.WhenAll(serverTask, clientTask);
        Assert.That(_connectionPool.ConnectionsClient.ToList(), Has.Count.EqualTo(1));
        Assert.That(_connectionPool.ConnectionsServer.ToList(), Has.Count.EqualTo(1));
        Assert.That(_connectionPool.ConnectionsClient.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(0));
        Assert.That(_connectionPool.ConnectionsServer.Where(c => c != null && c.VerifyIfConnected()).ToList(), Has.Count.EqualTo(0));
        await _connectionPool.DisposeAsync();
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
                    await _connectionPool.UseServerConnection("PipeConnectionPoolTests", null, async stream => {
                        var _ = await stream.ReadAsync(receivedBuffer, 0, 10, timeout.Token);
                    });
                    Interlocked.Increment(ref receivedMessages);
                    serverReceive.Set();
                }
            }, CancellationToken.None));
            clientTasks.Add(Task.Run(async () => {
                while (!timeout.IsCancellationRequested)
                {
                    await _connectionPool.UseClientConnection("PipeConnectionPoolTests", null, async stream => {
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
        await _connectionPool.DisposeAsync();
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