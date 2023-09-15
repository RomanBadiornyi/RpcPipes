using System.Collections.Concurrent;
using NSubstitute;
using RpcPipes.Models;
using RpcPipes.Models.PipeMessageHandlers;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.PipeData;
using RpcPipes.PipeHeartbeat;
using RpcPipes.Models.PipeHeartbeat;

namespace RpcPipes.Tests.PipeClientServer;

[TestFixture]
public class PipeClientServerTests : BasePipeClientServerTests
{
    [Test]
    public async Task RequestReply_ReplyReceived()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequestMessage, PipeReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(new PipeReplyMessage("hi"));

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            var request = new PipeRequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_OnDeadline_ReplyCancelled()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(MessageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            var request = new PipeRequestMessage("hello world", 10);
            var requestContext = new PipeRequestContext { Deadline = TimeSpan.FromMilliseconds(10) };
            var exception = Assert.ThrowsAsync<PipeServerException>(() =>
                pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None));
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
        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequestMessage, PipeReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(async args =>
            {
                receiveEventHandle.Set();
                var wait = 0;
                while (!((CancellationToken)args[1]).IsCancellationRequested && wait++ < 10)
                    await Task.Delay(TimeSpan.FromMilliseconds(5));
                return new PipeReplyMessage("hi");
            });

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            var request = new PipeRequestMessage("hello world", 0);
            
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            
            var cts = new CancellationTokenSource();
            var sendTask = pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, cts.Token);
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

    [Test]
    public async Task RequestReply_WhenServerDropsMessage_ReplyCancelled()
    {
        var receiveEventHandle = new ManualResetEventSlim(false);
        var proceedEventHandle = new ManualResetEventSlim(false);

        var pipeServer1MessageHandler = Substitute.For<IPipeMessageHandler<PipeRequestMessage, PipeReplyMessage>>();
        pipeServer1MessageHandler.HandleRequest(Arg.Any<PipeRequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                receiveEventHandle.Set();
                proceedEventHandle.Wait(TimeSpan.FromSeconds(30));
                return new PipeReplyMessage("hi");
            });

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer1HeartbeatHandler = new PipeHeartbeatMessageHandler();
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(pipeServer1MessageHandler, pipeServer1HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            var request = new PipeRequestMessage("hello world", 1);
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            var requestTask = pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None);
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

        await using var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer);
        pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);

        await pipeClient.DisposeAsync();

        var request = new PipeRequestMessage("hello world", 10);
        var requestContext = new PipeRequestContext();
        var exception = Assert.ThrowsAsync<TaskCanceledException>(() =>
            pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None));
        Assert.That(exception, Is.Not.Null);
        Assert.That(exception.Message, Does.Contain("Request cancelled due to cancellation of client"));

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_AllConnectedOnMessageSendAndDisconnectedOnDispose()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 4, Serializer);
        ServerTask = pipeServer.Start(MessageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 4, HeartbeatMessageReceiver, Serializer))
        {
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            pipeClient.ConnectionPool.ConnectionExpiryTimeout = TimeSpan.FromSeconds(600);
            pipeServer.ConnectionPool.ConnectionExpiryTimeout = TimeSpan.FromSeconds(600);
            Assert.Multiple(() =>
            {
                Assert.That(Connections["PipeTransportClient.server-connections"], Is.EqualTo(0));
                Assert.That(Connections["PipeTransportClient.client-connections"], Is.EqualTo(0));
                Assert.That(Connections["PipeTransportServer.server-connections"], Is.EqualTo(0));
                Assert.That(Connections["PipeTransportServer.client-connections"], Is.EqualTo(0));
            });
            var request = new PipeRequestMessage("hello world", 0.1);
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            _ = await pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.Multiple(() =>
            {
                //4 connections to accept response from server
                Assert.That(Connections["PipeTransportClient.server-connections"], Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(4),
                    "incorrect server connections on client");
                //4 connections to send requests (4 for client requests and 4 for heartbeat requests)
                Assert.That(Connections["PipeTransportClient.client-connections"], Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(8),
                    "incorrect client connections on client");
                //8 connections to accept requests from client (4 for requests and 4 for heartbeat)
                Assert.That(Connections["PipeTransportServer.server-connections"], Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(8),
                    "incorrect server connections on server");
                //4 client connections to send reply back to client
                Assert.That(Connections["PipeTransportServer.client-connections"], Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(4),
                    "incorrect server connections on server");
            });
        }

        Assert.That(Connections["PipeTransportClient.server-connections"], Is.EqualTo(0),
            "incorrect server connections on client");
        Assert.That(Connections["PipeTransportClient.client-connections"], Is.EqualTo(0),
            "incorrect client connections on client");
        Assert.That(Connections["PipeTransportServer.server-connections"], Is.GreaterThanOrEqualTo(0).And.LessThanOrEqualTo(8),
            "incorrect server connections on server");
        //at this point client connections will still be active as it does not receive disconnect signal and we didn't dispose server
        Assert.That(Connections["PipeTransportServer.client-connections"], Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(4),
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
        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequestMessage, PipeReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(new PipeReplyMessage("hi"));
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 2, Serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            var request = new PipeRequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }
        await using (var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            var request = new PipeRequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_MultipleClients_AllResponsesReceived()
    {
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 4, Serializer);
        ServerTask = pipeServer.Start(MessageHandler, HeartbeatHandler, ServerStop.Token);

        var replies = new ConcurrentBag<PipeReplyMessage>();
        var clientTasks = Enumerable.Range(0, 4).Select(async i => {
            var clientId = $"{TestContext.CurrentContext.Test.Name}.{i}";
            await using var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
                ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer);
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            var request = new PipeRequestMessage($"{i}", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None);
            replies.Add(reply);
        }).ToArray();

        await Task.WhenAll(clientTasks);

        var replyMessages = replies.Select(r => r.Reply).ToList();
        foreach (var index in Enumerable.Range(0, 4))
        {
            Assert.That(replyMessages, Has.Member($"{index}"));
        }
    }

    [Test]
    public async Task RequestReply_ProgressUpdated()
    {
        var heartbeatSync = new object();
        var heartbeatMessages = new ConcurrentBag<PipeHeartbeatMessage>();
        var heartbeatMessageReceiver = Substitute.For<IPipeHeartbeatReceiver<PipeHeartbeatMessage>>();
        heartbeatMessageReceiver.OnHeartbeatMessage(Arg.Any<PipeHeartbeatMessage>())
            .Returns(args =>
        {
            heartbeatMessages.Add((PipeHeartbeatMessage)args[0]);
            lock (heartbeatSync) { Monitor.Pulse(heartbeatSync); }
            return Task.CompletedTask;
        });

        var heartbeatHandler = Substitute.ForPartsOf<PipeHeartbeatHandler<PipeHeartbeatMessage>>();
        heartbeatHandler.HeartbeatMessage(Arg.Any<Guid>())
            .Returns(
                _ => new PipeHeartbeatMessage(0.1, ""),
                _ => new PipeHeartbeatMessage(0.5, ""),
                _ => new PipeHeartbeatMessage(1.0, ""));

        var receiveEventHandle = new ManualResetEventSlim(false);
        var proceedEventHandle = new ManualResetEventSlim(false);

        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequestMessage, PipeReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                receiveEventHandle.Set();
                proceedEventHandle.Wait(TimeSpan.FromSeconds(30));
                return new PipeReplyMessage("hi");
            });

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(messageHandler, heartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 1, heartbeatMessageReceiver, Serializer))
        {
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            var requestContext = new PipeRequestContext { Heartbeat = TimeSpan.FromMilliseconds(5) };
            var request = new PipeRequestMessage("hello world", 1);
            var clientTask = pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None);

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

        Assert.That(heartbeatMessages, Has.Some.Matches<PipeHeartbeatMessage>(m => CompareDouble(m.Progress, 0.1, 0.01)));
        Assert.That(heartbeatMessages, Has.Some.Matches<PipeHeartbeatMessage>(m => CompareDouble(m.Progress, 0.5, 0.01)));
        Assert.That(heartbeatMessages, Has.Some.Matches<PipeHeartbeatMessage>(m => CompareDouble(m.Progress, 1.0, 0.01)));

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_IntermediateMessageStages_ProgressUpdated()
    {
        var messageHandler = Substitute.ForPartsOf<PipeMessageHandler>();
        messageHandler.HeartbeatMessage(Arg.Any<object>())
            .Returns(_ => new PipeHeartbeatMessage(0.5, ""));
        var serializer = Substitute.ForPartsOf<PipeSerializer>();
        serializer.ReadRequest<PipeRequestMessage>(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(args => Serializer.ReadRequest<PipeRequestMessage>((Stream)args[0], (CancellationToken)args[1]))
            .AndDoes(_ => Task.Delay(TimeSpan.FromMilliseconds(50)).Wait());
        serializer.WriteResponse(Arg.Any<PipeMessageResponse<PipeReplyMessage>>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(args => Serializer.WriteResponse((PipeMessageResponse<PipeReplyMessage>)args[0], (Stream)args[1], (CancellationToken)args[2]))
            .AndDoes(_ => Task.Delay(TimeSpan.FromMilliseconds(50)).Wait());

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            var request = new PipeRequestMessage("hello world", 0.1);
            _ = await pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None);
        }

        Assert.That(HeartbeatReplies, Has.All.Matches<PipeHeartbeatMessage>(m => CompareDouble(m.Progress, 0.5, 0.01)));

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReplyWhenHandlerThrows_ErrorReturned()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<PipeRequestMessage, PipeReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<PipeRequestMessage>(), Arg.Any<CancellationToken>())
            .Returns<PipeReplyMessage>(_ => throw new InvalidOperationException("handler error"));

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(messageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeHeartbeatMessage>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            pipeClient.Cancellation.CancelAfter(ClientRequestTimeout);
            var request = new PipeRequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var exception = Assert.ThrowsAsync<PipeServerException>(
                () => pipeClient.SendRequest<PipeRequestMessage, PipeReplyMessage>(request, requestContext, CancellationToken.None));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("handler error"));
        }

        ServerStop.Cancel();
        await ServerTask;
    }

    bool CompareDouble(double actual, double expected, double precision)
        => Math.Abs(actual - expected) < precision;    
}