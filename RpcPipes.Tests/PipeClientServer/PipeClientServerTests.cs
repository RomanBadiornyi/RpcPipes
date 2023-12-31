using System.Collections.Concurrent;
using NSubstitute;
using RpcPipes.Models;
using RpcPipes.Models.PipeMessageHandlers;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.PipeData;
using RpcPipes.PipeHeartbeat;
using RpcPipes.Models.PipeHeartbeat;
using RpcPipes.PipeExceptions;

namespace RpcPipes.Tests.PipeClientServer;

[TestFixture]
public class PipeClientServerTests : BasePipeClientServerTests
{
    [Test]
    public async Task RequestReply_ReplyReceived()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequest, PipeReply>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequest>(), Arg.Any<CancellationToken>())
            .Returns(new PipeReply("hi"));

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);

        //test that client can be dispose after creation without sending any requests
        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
        }

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_WhenNoConnectionWithServer_ReplyError()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 10);
            var requestContext = new PipeRequestContext();
            var exception = Assert.ThrowsAsync<PipeConnectionsExhausted>(() =>
                pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("run out of available connections"));
            Assert.That(exception.InnerException.Message, Does.Contain("timeout"));
        }
    }

    [Test]
    public async Task RequestReply_WhenPassedWithCancelled_ReplyCancelled()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 10);
            var requestContext = new PipeRequestContext { Deadline = TimeSpan.FromMilliseconds(10) };
            var cancellation = new CancellationTokenSource();
            cancellation.Cancel();
            var exception = Assert.ThrowsAsync<TaskCanceledException>(() =>
                pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, cancellation.Token));
            Assert.That(exception, Is.Not.Null);
        }
    }

    [Test]
    public async Task RequestReply_OnDeadline_ReplyCancelled()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(MessageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 10);
            var requestContext = new PipeRequestContext { Deadline = TimeSpan.FromMilliseconds(10) };
            var exception = Assert.ThrowsAsync<PipeServerException>(() =>
                pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("A task was canceled"));
        }
        Assert.That(Messages["active-messages"], Is.EqualTo(0));
        Assert.That(Messages["handled-messages"], Is.EqualTo(1));

        Assert.That(Messages["sent-messages"], Is.EqualTo(1));
        Assert.That(Messages["received-messages"], Is.EqualTo(1));

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_OnTimeout_ReplyCancelled()
    {
        var receiveEventHandle = new ManualResetEventSlim(false);
        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequest, PipeReply>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequest>(), Arg.Any<CancellationToken>())
            .Returns(async args =>
            {
                receiveEventHandle.Set();
                var wait = 0;
                while (!((CancellationToken)args[1]).IsCancellationRequested && wait++ < 32)
                    await Task.Delay(TimeSpan.FromMilliseconds(8));
                return new PipeReply("hi");
            });

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 0);

            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };

            var cts = new CancellationTokenSource();
            var sendTask = pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, cts.Token);
            //wait for message to arrive to server
            receiveEventHandle.Wait(TimeSpan.FromSeconds(30));
            //cancel request execution from the client side
            cts.Cancel();

            var exception = Assert.ThrowsAsync<PipeServerException>(() => sendTask);

            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("The operation was canceled"));
        }

        Assert.That(Messages["active-messages"], Is.EqualTo(0));
        Assert.That(Messages["handled-messages"], Is.EqualTo(1));
        Assert.That(Messages["sent-messages"], Is.EqualTo(1));
        Assert.That(Messages["received-messages"], Is.EqualTo(1));

        ServerStop.Cancel();
        await ServerTask;
    }

    [TestCase(5, 10)]
    [TestCase(10, 10)]
    public async Task RequestReply_OnTimeout_RequestsInBatchCancelled(int tasksReachingServerCount, int tasksCount)
    {
        var receivedTasksCount = 0;
        var receiveEventHandle = new EventWaitHandle(false, EventResetMode.ManualReset);
        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequest, PipeReply>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequest>(), Arg.Any<CancellationToken>())
            .Returns(async args =>
            {
                var received = Interlocked.Increment(ref receivedTasksCount);
                //signal that expected number of tasks reached server side so we can stop client
                if (received == tasksReachingServerCount)
                    receiveEventHandle.Set();                
                await Task.Delay(TimeSpan.FromSeconds(30), (CancellationToken)args[1]);
                return new PipeReply("hi");
            });

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);

        Task<PipeReply>[] requests = null;
        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 0);
            var requestContext = new PipeRequestContext { Heartbeat = TimeSpan.FromMilliseconds(10) };
            var cts = new CancellationTokenSource();
            requests = Enumerable.Range(0, tasksCount)
                .Select(async i => 
                {
                    //if we want to cancel some of the tasks before they reach server side - apply wait logic here
                    if (tasksCount - tasksReachingServerCount > 0 && i > (tasksCount - tasksReachingServerCount / 2))
                    {
                        receiveEventHandle.WaitOne(TimeSpan.FromSeconds(10));
                        await Task.Delay(TimeSpan.FromMilliseconds(i - tasksReachingServerCount));
                    }
                    return await pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, cts.Token);
                })
                .ToArray();
            //wait for message to arrive to server
            receiveEventHandle.WaitOne(TimeSpan.FromSeconds(10));
        }

        var exceptions = new List<Exception>();
        foreach (var request in requests)
        {
            try
            {
                var reply = await request;
                Assert.That(reply, Is.Null);
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }            
        }
        Assert.That(exceptions, Has.Count.EqualTo(tasksCount));
        var serverExceptions = exceptions.OfType<PipeServerException>().ToList();
        var clientExceptions = exceptions.OfType<OperationCanceledException>().ToList();
        Assert.That(serverExceptions, Has.Count.GreaterThanOrEqualTo(tasksReachingServerCount));
        Assert.That(clientExceptions, Has.Count.LessThanOrEqualTo(tasksCount - tasksReachingServerCount));

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_WhenServerDropsMessage_ReplyCancelled()
    {
        var receiveEventHandle = new ManualResetEventSlim(false);
        var proceedEventHandle = new ManualResetEventSlim(false);

        var pipeServer1MessageHandler = Substitute.For<IPipeMessageHandler<PipeRequest, PipeReply>>();
        pipeServer1MessageHandler.HandleRequest(Arg.Any<PipeRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                receiveEventHandle.Set();
                proceedEventHandle.Wait(TimeSpan.FromSeconds(30));
                return new PipeReply("hi");
            });

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer1HeartbeatHandler = new PipeHeartbeatMessageHandler();
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(pipeServer1MessageHandler, pipeServer1HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 1);
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            var requestTask = pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None);
            receiveEventHandle.Wait(TimeSpan.FromSeconds(30));

            ServerStop.Cancel();
            await ServerTask;

            proceedEventHandle.Set();
            SetupServer();

            var pipeServer2MessageHandler = new PipeMessageHandler();
            var pipeServer2HeartbeatHandler = new PipeHeartbeatMessageHandler();
            pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
            ServerTask = pipeServer.Start(pipeServer2MessageHandler, pipeServer2HeartbeatHandler, ServerStop.Token);
            var exception = Assert.ThrowsAsync<TaskCanceledException>(() => requestTask);
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("Request cancelled due to failed heartbeat"));
        }

        Assert.That(Messages["active-messages"], Is.EqualTo(0));
        Assert.That(Messages["handled-messages"], Is.EqualTo(1));

        Assert.That(Messages["sent-messages"], Is.EqualTo(1));
        Assert.That(Messages["received-messages"], Is.EqualTo(0));

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_WhenClientDisposed_ReplyCancelled()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(MessageHandler, HeartbeatHandler, ServerStop.Token);

        await using var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer);
        SetupClient(pipeClient);

        await pipeClient.DisposeAsync();

        var request = new PipeRequest("hello world", 10);
        var requestContext = new PipeRequestContext();
        var exception = Assert.ThrowsAsync<TaskCanceledException>(() =>
            pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None));
        Assert.That(exception, Is.Not.Null);
        Assert.That(exception.Message, Does.Contain("Request cancelled due to dispose of client"));

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_AllConnectedOnMessageSendAndDisconnectedOnDispose()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(MessageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            pipeClient.ConnectionPool.ConnectionExpiryTimeout = TimeSpan.FromSeconds(600);
            pipeServer.ConnectionPool.ConnectionExpiryTimeout = TimeSpan.FromSeconds(600);
            Assert.Multiple(() =>
            {
                Assert.That(Connections["PipeTransportClient.server-connections"], Is.EqualTo(0));
                Assert.That(Connections["PipeTransportClient.client-connections"], Is.EqualTo(0));
                Assert.That(Connections["PipeTransportServer.server-connections"], Is.EqualTo(0));
                Assert.That(Connections["PipeTransportServer.client-connections"], Is.EqualTo(0));
            });
            var request = new PipeRequest("hello world", 0.1);
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            _ = await pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None);
            Assert.Multiple(() =>
            {
                //1 connections to accept response from server
                Assert.That(Connections["PipeTransportClient.server-connections"], Is.GreaterThanOrEqualTo(0).And.LessThanOrEqualTo(1),
                    "incorrect server connections on client");
                //1 for client requests and 1 for heartbeat requests
                Assert.That(Connections["PipeTransportClient.client-connections"], Is.EqualTo(2),
                    "incorrect client connections on client");
                //1 connections to accept requests from client and 1 to receive heartbeat
                Assert.That(Connections["PipeTransportServer.server-connections"], Is.EqualTo(2),
                    "incorrect server connections on server");
                //1 client connections to send reply back to client
                Assert.That(Connections["PipeTransportServer.client-connections"], Is.EqualTo(1),
                    "incorrect server connections on server");
            });
        }

        Assert.That(Connections["PipeTransportClient.server-connections"], Is.EqualTo(0),
            "incorrect server connections on client");
        Assert.That(Connections["PipeTransportClient.client-connections"], Is.EqualTo(0),
            "incorrect client connections on client");

        Assert.That(Connections["PipeTransportServer.server-connections"], Is.GreaterThanOrEqualTo(0).And.LessThanOrEqualTo(2),
            "incorrect server connections on server");
        //at this point client connections will still be active as it does not receive disconnect signal and we didn't dispose server
        Assert.That(Connections["PipeTransportServer.client-connections"], Is.GreaterThanOrEqualTo(0).And.LessThanOrEqualTo(1),
            "incorrect server connections on server");

        ServerStop.Cancel();
        await ServerTask;

        Assert.That(Connections["PipeTransportServer.server-connections"], Is.EqualTo(0),
            "incorrect server connections on server");
        Assert.That(Connections["PipeTransportServer.client-connections"], Is.EqualTo(0),
            "incorrect server connections on server");
    }

    [Test]
    public async Task RequestReply_SameClientIdAndMultipleConnections_AllResponsesReceived()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequest, PipeReply>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequest>(), Arg.Any<CancellationToken>())
            .Returns(new PipeReply("hi"));
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 2, Serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }
        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_MultipleClients_AllResponsesReceived()
    {
        const int clientsCount = 4;
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", clientsCount, Serializer);
        ServerTask = pipeServer.Start(MessageHandler, HeartbeatHandler, ServerStop.Token);

        var replies = new ConcurrentBag<PipeReply>();
        var clientTasks = Enumerable.Range(0, clientsCount).Select(i => RunRequestOnClient(i, replies)).ToArray();
        await Task.WhenAll(clientTasks);

        var replyMessages = replies.Select(r => r.Reply).ToList();
        foreach (var index in Enumerable.Range(0, clientsCount))
        {
            Assert.That(replyMessages, Has.Member($"{index}"));
        }

        async Task RunRequestOnClient(int clientNum, ConcurrentBag<PipeReply> results)
        {
            var clientId = $"{TestContext.CurrentContext.Test.Name}.{clientNum}";
            await using var pipeClient = new PipeTransportClient<PipeState>(
                ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer);
            SetupClient(pipeClient);

            var request = new PipeRequest($"{clientNum}", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None);
            results.Add(reply);
        }
    }

    [Test]
    public async Task RequestReply_ProgressUpdated()
    {
        var heartbeatSync = new object();
        var heartbeatMessages = new ConcurrentBag<PipeMessageHeartbeat<PipeState>>();
        var heartbeatMessageReceiver = Substitute.For<IPipeHeartbeatReceiver<PipeState>>();
        heartbeatMessageReceiver.OnHeartbeatMessage(Arg.Any<PipeMessageHeartbeat<PipeState>>())
            .Returns(args =>
        {
            heartbeatMessages.Add((PipeMessageHeartbeat<PipeState>)args[0]);
            lock (heartbeatSync) { Monitor.Pulse(heartbeatSync); }
            return Task.CompletedTask;
        });

        var heartbeatHandler = Substitute.ForPartsOf<PipeHeartbeatHandler<PipeState>>();
        heartbeatHandler.HeartbeatMessage(Arg.Any<Guid>())
            .Returns(
                _ => new PipeMessageHeartbeat<PipeState> { Progress = 0.1 },
                _ => new PipeMessageHeartbeat<PipeState> { Progress = 0.5 },
                _ => new PipeMessageHeartbeat<PipeState> { Progress = 1.0 });

        var receiveEventHandle = new ManualResetEventSlim(false);
        var proceedEventHandle = new ManualResetEventSlim(false);

        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequest, PipeReply>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequest>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                receiveEventHandle.Set();
                proceedEventHandle.Wait(TimeSpan.FromSeconds(30));
                return new PipeReply("hi");
            });

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(messageHandler, heartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, heartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var requestContext = new PipeRequestContext { Heartbeat = TimeSpan.FromMilliseconds(5) };
            var request = new PipeRequest("hello world", 1);
            var clientTask = pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None);

            receiveEventHandle.Wait(TimeSpan.FromSeconds(30));
            await Task.Run(() => {
                lock (heartbeatSync)
                {
                    while (heartbeatMessages.Count < 3)
                    {
                        Monitor.Wait(heartbeatSync, TimeSpan.FromSeconds(30));
                    }
                }
            });
            //as soon as we received 3 heartbeat messages - release message handler to complete request
            proceedEventHandle.Set();
            await clientTask;
        }

        Assert.That(heartbeatMessages, Has.Some.Matches<PipeMessageHeartbeat<PipeState>>(m => CompareDouble(m.Progress, 0.1, 0.01)));
        Assert.That(heartbeatMessages, Has.Some.Matches<PipeMessageHeartbeat<PipeState>>(m => CompareDouble(m.Progress, 0.5, 0.01)));
        Assert.That(heartbeatMessages, Has.Some.Matches<PipeMessageHeartbeat<PipeState>>(m => CompareDouble(m.Progress, 1.0, 0.01)));

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_IntermediateMessageStages_ProgressUpdated()
    {
        var messageHandler = Substitute.ForPartsOf<PipeMessageHandler>();
        messageHandler.HeartbeatMessage(Arg.Any<object>())
            .Returns(_ => new PipeMessageHeartbeat<PipeState> { Progress = 0.5 });
        //should be no progress updates when serializer starts
        var serializer = Substitute.ForPartsOf<PipeJsonSerializer>();
        serializer.ReadRequest<PipeRequest>(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(args => Serializer.ReadRequest<PipeRequest>((Stream)args[0], (CancellationToken)args[1]))
            .AndDoes(_ => Task.Delay(TimeSpan.FromMilliseconds(50)).Wait());
        serializer.WriteResponse(Arg.Any<PipeMessageResponse<PipeReply>>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(args => Serializer.WriteResponse((PipeMessageResponse<PipeReply>)args[0], (Stream)args[1], (CancellationToken)args[2]))
            .AndDoes(_ => Task.Delay(TimeSpan.FromMilliseconds(50)).Wait());

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);
        HeartbeatReplies.Clear();
        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            var request = new PipeRequest("hello world", 0.1);
            _ = await pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None);
        }

        Assert.That(HeartbeatReplies, Has.All.Matches<PipeMessageHeartbeat<PipeState>>(m => 
            CompareDouble(m.Progress, 0.5, 0.01) || 
            CompareDouble(m.Progress, 1.0, 0.01)));

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_WhenHandlerThrows_ErrorReturned()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequest, PipeReply>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequest>(), Arg.Any<CancellationToken>())
            .Returns<PipeReply>(_ => throw new InvalidOperationException("handler error"));

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 0);
            var requestContext = new PipeRequestContext();
            var exception = Assert.ThrowsAsync<PipeServerException>(
                () => pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("handler error"));
        }

        ServerStop.Cancel();
        await ServerTask;
    }

    bool CompareDouble(double actual, double expected, double precision)
        => Math.Abs(actual - expected) < precision;
}