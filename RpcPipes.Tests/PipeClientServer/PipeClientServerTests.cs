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
        var messageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(new ReplyMessage("hi"));

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 1, _serializer);
        _serverTask = pipeServer.Start(messageHandler, _heartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
            var request = new RequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }

        _serverStop.Cancel();
        await _serverTask;
    }

    [Test]
    public async Task RequestReply_OnDeadline_ReplyCancelled()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 1, _serializer);
        _serverTask = pipeServer.Start(_messageHandler, _heartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
            var request = new RequestMessage("hello world", 10);
            var requestContext = new PipeRequestContext { Deadline = TimeSpan.FromMilliseconds(10) };
            var exception = Assert.ThrowsAsync<PipeServerException>(() =>
                pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("A task was canceled"));
        }
        Assert.That(_messages["active-messages"], Is.EqualTo(0));
        Assert.That(_messages["handled-messages"], Is.EqualTo(1));

        Assert.That(_messages["sent-messages"], Is.EqualTo(1));
        Assert.That(_messages["received-messages"], Is.EqualTo(1));

        _serverStop.Cancel();
        await _serverTask;
    }

    [Test]
    public async Task RequestReply_OnTimeout_ReplyCancelled()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 1, _serializer);
        _serverTask = pipeServer.Start(_messageHandler, _heartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
            var request = new RequestMessage("hello world", 10);
            var cts = new CancellationTokenSource();
            cts.CancelAfter(TimeSpan.FromMilliseconds(10));
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            var exception = Assert.ThrowsAsync<PipeServerException>(() =>
                pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, cts.Token));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("A task was canceled"));
        }

        Assert.That(_messages["active-messages"], Is.EqualTo(0));
        Assert.That(_messages["handled-messages"], Is.EqualTo(1));

        Assert.That(_messages["sent-messages"], Is.EqualTo(1));
        Assert.That(_messages["received-messages"], Is.EqualTo(1));

        _serverStop.Cancel();
        await _serverTask;
    }

    [Test]
    public async Task RequestReply_WhenServerDropsMessage_ReplyCancelled()
    {
        var receiveEventHandle = new ManualResetEventSlim(false);
        var proceedEventHandle = new ManualResetEventSlim(false);

        var pipeServer1MessageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        pipeServer1MessageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(args =>
            {
                receiveEventHandle.Set();
                proceedEventHandle.Wait(TimeSpan.FromSeconds(30));
                return new ReplyMessage("hi");
            });

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer1HeartbeatHandler = new PipeHeartbeatMessageHandler();
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 1, _serializer);
        _serverTask = pipeServer.Start(pipeServer1MessageHandler, pipeServer1HeartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
            var request = new RequestMessage("hello world", 1);
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            var requestTask = pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
            receiveEventHandle.Wait(TimeSpan.FromSeconds(30));

            _serverStop.Cancel();
            await _serverTask;

            proceedEventHandle.Set();
            SetupServer();

            var pipeServer2MessageHandler = new PipeMessageHandler();
            var pipeServer2HeartbeatHandler = new PipeHeartbeatMessageHandler();
            pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 1, _serializer);
            _serverTask = pipeServer.Start(pipeServer2MessageHandler, pipeServer2HeartbeatHandler, _serverStop.Token);
            var exception = Assert.ThrowsAsync<TaskCanceledException>(() => requestTask);
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("Request cancelled due to failed heartbeat"));
        }

        Assert.That(_messages["active-messages"], Is.EqualTo(0));
        Assert.That(_messages["handled-messages"], Is.EqualTo(1));

        Assert.That(_messages["sent-messages"], Is.EqualTo(1));
        Assert.That(_messages["received-messages"], Is.EqualTo(0));

        _serverStop.Cancel();
        await _serverTask;
    }

    [Test]
    public async Task RequestReply_WhenClientDisposed_ReplyCancelled()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 1, _serializer);
        _serverTask = pipeServer.Start(_messageHandler, _heartbeatHandler, _serverStop.Token);

        using var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 1, _heartbeatMessageReceiver, _serializer);
        pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);

        await pipeClient.DisposeAsync();

        var request = new RequestMessage("hello world", 10);
        var requestContext = new PipeRequestContext();
        var exception = Assert.ThrowsAsync<TaskCanceledException>(() =>
            pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None));
        Assert.That(exception, Is.Not.Null);
        Assert.That(exception.Message, Does.Contain("Request cancelled due to cancellation of client"));

        _serverStop.Cancel();
        await _serverTask;
    }

    [Test]
    public async Task RequestReply_AllConnectedOnMessageSendAndDisconnectedOnDispose()
    {
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 4, _serializer);
        _serverTask = pipeServer.Start(_messageHandler, _heartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 4, _heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
            pipeClient.ConnectionPool.ClientConnectionExpiryTimeout = TimeSpan.FromSeconds(600);
            pipeServer.ConnectionPool.ClientConnectionExpiryTimeout = TimeSpan.FromSeconds(600);

            Assert.Multiple(() =>
            {
                Assert.That(_connections["PipeTransportClient.server-connections"], Is.EqualTo(0));
                Assert.That(_connections["PipeTransportClient.client-connections"], Is.EqualTo(0));
                Assert.That(_connections["PipeTransportServer.server-connections"], Is.EqualTo(0));
                Assert.That(_connections["PipeTransportServer.client-connections"], Is.EqualTo(0));
            });
            var request = new RequestMessage("hello world", 0.1);
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            _ = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.Multiple(() =>
            {
                //4 connections to accept response from server
                Assert.That(_connections["PipeTransportClient.server-connections"], Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(4),
                    "incorrect server connections on client");
                //4 connections to send requests (4 for client requests and 4 for heartbeat requests)
                Assert.That(_connections["PipeTransportClient.client-connections"], Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(8),
                    "incorrect client connections on client");
                //8 connections to accept requests from client (4 for requests and 4 for heartbeat)
                Assert.That(_connections["PipeTransportServer.server-connections"], Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(8),
                    "incorrect server connections on server");
                //4 client connections to send reply back to client
                Assert.That(_connections["PipeTransportServer.client-connections"], Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(4),
                    "incorrect server connections on server");
            });
        }

        Assert.That(_connections["PipeTransportClient.server-connections"], Is.EqualTo(0),
            "incorrect server connections on client");
        Assert.That(_connections["PipeTransportClient.client-connections"], Is.EqualTo(0),
            "incorrect client connections on client");
        Assert.That(_connections["PipeTransportServer.server-connections"], Is.GreaterThanOrEqualTo(0).And.LessThanOrEqualTo(8),
            "incorrect server connections on server");
        //at this point client connections will still be active as it does not receive disconnect signal and we didn't dispose server
        Assert.That(_connections["PipeTransportServer.client-connections"], Is.GreaterThanOrEqualTo(1).And.LessThanOrEqualTo(4),
            "incorrect server connections on server");

        _serverStop.Cancel();
        await _serverTask;

        Assert.That(_connections["PipeTransportServer.server-connections"], Is.EqualTo(0),
            "incorrect server connections on server");
        Assert.That(_connections["PipeTransportServer.client-connections"], Is.EqualTo(0),
            "incorrect server connections on server");
    }

    [Test]
    public async Task RequestReply_SameClientIdAndMultipleConnections_AllResponsesReceived()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(new ReplyMessage("hi"));
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 1, _serializer);
        _serverTask = pipeServer.Start(messageHandler, _heartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
            var request = new RequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }
        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
            var request = new RequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }

        _serverStop.Cancel();
        await _serverTask;
    }

    [Test]
    public async Task RequestReply_MultipleClients_AllResponsesReceived()
    {
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 4, _serializer);
        _serverTask = pipeServer.Start(_messageHandler, _heartbeatHandler, _serverStop.Token);

        var replies = new ConcurrentBag<ReplyMessage>();
        var clientTasks = Enumerable.Range(0, 4).Select(async i => {
            var clientId = $"{TestContext.CurrentContext.Test.Name}.{i}";
            await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
                _clientLogger, "rpc.pipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
            {
                pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
                var request = new RequestMessage($"{i}", 0);
                var requestContext = new PipeRequestContext();
                var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
                replies.Add(reply);
            }
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
        var heartbeatMessages = new ConcurrentBag<HeartbeatMessage>();
        var heartbeatMessageReceiver = Substitute.For<IPipeHeartbeatReceiver<HeartbeatMessage>>();
        heartbeatMessageReceiver.OnHeartbeatMessage(Arg.Any<HeartbeatMessage>())
            .Returns(args =>
        {
            heartbeatMessages.Add((HeartbeatMessage)args[0]);
            lock (heartbeatSync) { Monitor.Pulse(heartbeatSync); }
            return Task.CompletedTask;
        });

        var heartbeatHandler = Substitute.ForPartsOf<PipeHeartbeatHandler<HeartbeatMessage>>();
        heartbeatHandler.HeartbeatMessage(Arg.Any<Guid>())
            .Returns(
                args => new HeartbeatMessage(0.1, ""),
                args => new HeartbeatMessage(0.5, ""),
                args => new HeartbeatMessage(1.0, ""));

        var receiveEventHandle = new ManualResetEventSlim(false);
        var proceedEventHandle = new ManualResetEventSlim(false);

        var messageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(args =>
            {
                receiveEventHandle.Set();
                proceedEventHandle.Wait(TimeSpan.FromSeconds(30));
                return new ReplyMessage("hi");
            });

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 1, _serializer);
        _serverTask = pipeServer.Start(messageHandler, heartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 1, heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
            var requestContext = new PipeRequestContext { Heartbeat = TimeSpan.FromMilliseconds(5) };
            var request = new RequestMessage("hello world", 1);
            var clientTask = pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);

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

        Assert.That(heartbeatMessages, Has.Some.Matches<HeartbeatMessage>(m => m.Progress == 0.1));
        Assert.That(heartbeatMessages, Has.Some.Matches<HeartbeatMessage>(m => m.Progress == 0.5));
        Assert.That(heartbeatMessages, Has.Some.Matches<HeartbeatMessage>(m => m.Progress == 1.0));

        _serverStop.Cancel();
        await _serverTask;
    }

    [Test]
    public async Task RequestReply_IntermediateMessageStages_ProgressUpdated()
    {
        var messageHandler = Substitute.ForPartsOf<PipeMessageHandler>();
        messageHandler.HeartbeatMessage(Arg.Any<object>())
            .Returns(args => new HeartbeatMessage(0.5, ""));
        var serializer = Substitute.ForPartsOf<PipeSerializer>();
        serializer.ReadRequest<RequestMessage>(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(args => _serializer.ReadRequest<RequestMessage>((Stream)args[0], (CancellationToken)args[1]))
            .AndDoes(x => Task.Delay(TimeSpan.FromMilliseconds(50)).Wait());
        serializer.WriteResponse(Arg.Any<PipeMessageResponse<ReplyMessage>>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(args => _serializer.WriteResponse((PipeMessageResponse<ReplyMessage>)args[0], (Stream)args[1], (CancellationToken)args[2]))
            .AndDoes(x => Task.Delay(TimeSpan.FromMilliseconds(50)).Wait());

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 1, serializer);
        _serverTask = pipeServer.Start(messageHandler, _heartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            var request = new RequestMessage("hello world", 0.1);
            _ = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
        }

        Assert.That(_heartbeatReplies, Has.All.Matches<HeartbeatMessage>(m => m.Progress == 0.5));

        _serverStop.Cancel();
        await _serverTask;
    }

    [Test]
    public async Task RequestReplyWhenHandlerThrows_ErrorReturned()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns<ReplyMessage>(args => throw new InvalidOperationException("handler error"));

        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(_serverLogger, "rpc.pipe", 1, _serializer);
        _serverTask = pipeServer.Start(messageHandler, _heartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "rpc.pipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(_clientRequestTimeout);
            var request = new RequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var exception = Assert.ThrowsAsync<PipeServerException>(
                () => pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("handler error"));
        }

        _serverStop.Cancel();
        await _serverTask;
    }
}