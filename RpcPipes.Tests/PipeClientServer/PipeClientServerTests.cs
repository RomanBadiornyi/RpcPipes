using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using NSubstitute;
using RpcPipes.Models;
using RpcPipes.Models.PipeMessageHandlers;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.PipeData;
using RpcPipes.PipeHeartbeat;

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
        
        var clientId = $"TestPipe.{Guid.NewGuid()}";
        var pipeServer = new PipeTransportServer(_serverLogger, "TestPipe", "Heartbeat.TestPipe", 4, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(messageHandler, _heartbeatHandler, serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "TestPipe", "Heartbeat.TestPipe", clientId, 4, _heartbeatMessageReceiver, _serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }

        serverStop.Cancel();
        await serverTask;
    }

    [Test]
    public async Task RequestReply_AllConnectedOnMessageSendAndDisconnectedOnDispose()
    {
        var connections = new Dictionary<string, int>();
        connections.TryAdd("PipeTransportClient.server-connections", 0);
        connections.TryAdd("PipeTransportClient.client-connections", 0);
        connections.TryAdd("PipeTransportServer.server-connections", 0);
        connections.TryAdd("PipeTransportServer.client-connections", 0);
        using MeterListener meterListener = new();
        meterListener.InstrumentPublished = (instrument, listener) =>
        {
            if (connections.ContainsKey($"{instrument.Meter.Name}.{instrument.Name}"))
                listener.EnableMeasurementEvents(instrument);
        };        
        meterListener.SetMeasurementEventCallback<int>(OnMeasurementRecorded);
        meterListener.Start();
        
        var clientId = $"TestPipe.{Guid.NewGuid()}";
        var pipeServer = new PipeTransportServer(
            _serverLogger, "TestPipe", "Heartbeat.TestPipe", 4, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(_messageHandler, _heartbeatHandler, serverStop.Token);

        pipeServer.ConnectionPool.ClientConnectionExpiryTimeout = TimeSpan.FromSeconds(600);
        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "TestPipe", "Heartbeat.TestPipe", clientId, 4, _heartbeatMessageReceiver, _serializer))
        {            
            pipeClient.ConnectionPool.ClientConnectionExpiryTimeout = TimeSpan.FromSeconds(600);            
            
            Assert.That(connections["PipeTransportClient.server-connections"], Is.EqualTo(0));
            Assert.That(connections["PipeTransportClient.client-connections"], Is.EqualTo(0));
            Assert.That(connections["PipeTransportServer.server-connections"], Is.EqualTo(0));
            Assert.That(connections["PipeTransportServer.client-connections"], Is.EqualTo(0));

            var request = new RequestMessage("hello world", 0.5);
            var requestContext = new PipeRequestContext 
            { 
                Heartbeat = TimeSpan.FromMilliseconds(100)
            };
            var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
            //4 connections to accept response from server
            Assert.That(connections["PipeTransportClient.server-connections"], Is.EqualTo(4));
            //4 connections to send requests (4 for client requests and 4 for heartbeat requests)
            Assert.That(connections["PipeTransportClient.client-connections"], Is.EqualTo(8));
            //8 connections to accept requests from client (4 for requests and 4 for heartbeat)
            Assert.That(connections["PipeTransportServer.server-connections"], Is.EqualTo(8));
            //4 client connections to send reply back to client
            Assert.That(connections["PipeTransportServer.client-connections"], Is.EqualTo(4));
        }

        Assert.That(connections["PipeTransportClient.server-connections"], Is.EqualTo(0));
        Assert.That(connections["PipeTransportClient.client-connections"], Is.EqualTo(0));
        Assert.That(connections["PipeTransportServer.server-connections"], Is.EqualTo(0));
        //at this point client connections will still be active as it does not receive disconnect signal and we didn't dispose server
        Assert.That(connections["PipeTransportServer.client-connections"], Is.EqualTo(4));

        serverStop.Cancel();
        await serverTask;

        Assert.That(connections["PipeTransportServer.client-connections"], Is.EqualTo(0));
       
        void OnMeasurementRecorded(Instrument instrument, int measurement, ReadOnlySpan<KeyValuePair<string, object>> tags, object state)
        {
            lock(connections) 
            {
                connections[$"{instrument.Meter.Name}.{instrument.Name}"] += measurement;
            }            
        }
    }    

    [Test]
    public async Task RequestReply_SameClientIdAndMultipleConnections_AllResponsesReceived()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(new ReplyMessage("hi"));
        
        var clientId = $"TestPipe.{Guid.NewGuid()}";
        var pipeServer = new PipeTransportServer(_serverLogger, "TestPipe", "Heartbeat.TestPipe", 2, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(messageHandler, _heartbeatHandler, serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "TestPipe", "Heartbeat.TestPipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }
        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "TestPipe", "Heartbeat.TestPipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }

        serverStop.Cancel();
        await serverTask;
    }

    [Test]
    public async Task RequestReply_MultipleClients_AllResponsesReceived()
    {
        var pipeServer = new PipeTransportServer(_serverLogger, "TestPipe", "Heartbeat.TestPipe", 4, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(_messageHandler, _heartbeatHandler, serverStop.Token);

        var replies = new ConcurrentBag<ReplyMessage>();
        var clientTasks = Enumerable.Range(0, 4).Select(async i => {
            var clientId = $"TestPipe.{Guid.NewGuid()}";
            await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
                _clientLogger, "TestPipe", "Heartbeat.TestPipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
            {
                var request = new RequestMessage($"{i}", 0);
                var requestContext = new PipeRequestContext();
                using var tokenSource = new CancellationTokenSource();
                tokenSource.CancelAfter(TimeSpan.FromSeconds(10));
                var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, tokenSource.Token);
                replies.Add(reply);
            }
        }).ToArray();

        try
        {
            await Task.WhenAll(clientTasks);

            var replyMessages = replies.Select(r => r.Reply).ToList();
            foreach (var index in Enumerable.Range(0, 4))
            {
                Assert.That(replyMessages, Has.Member($"{index}"));
            }                    
        }
        finally
        {
            serverStop.Cancel();
            await serverTask;            
        }        
    }

    [Test]
    public async Task RequestReply_ProgressUpdated()
    {
        var heartbeatHandler = Substitute.ForPartsOf<PipeHeartbeatHandler<HeartbeatMessage>>();
        heartbeatHandler.HeartbeatMessage(Arg.Any<Guid>())
            .Returns(args => new HeartbeatMessage(0.1, ""), args => new HeartbeatMessage(0.5, ""), args => new HeartbeatMessage(1.0, ""));
        
        var clientId = $"TestPipe.{Guid.NewGuid()}";
        var pipeServer = new PipeTransportServer(_serverLogger, "TestPipe", "Heartbeat.TestPipe", 1, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(_messageHandler, heartbeatHandler, serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "TestPipe", "Heartbeat.TestPipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            var requestContext = new PipeRequestContext
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            var request = new RequestMessage("hello world", 0.1);
            _ = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
        }

        Assert.That(_heartbeatReplies, Has.Some.Matches<HeartbeatMessage>(m => m.Progress == 0.1));
        Assert.That(_heartbeatReplies, Has.Some.Matches<HeartbeatMessage>(m => m.Progress == 0.5));
        Assert.That(_heartbeatReplies, Has.Some.Matches<HeartbeatMessage>(m => m.Progress == 1.0));
        
        serverStop.Cancel();
        await serverTask;
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

        var clientId = $"TestPipe.{Guid.NewGuid()}";
        var pipeServer = new PipeTransportServer(_serverLogger, "TestPipe", "Heartbeat.TestPipe", 1, serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(messageHandler, _heartbeatHandler, serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "TestPipe", "Heartbeat.TestPipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {            
            var requestContext = new PipeRequestContext 
            {
                Heartbeat = TimeSpan.FromMilliseconds(10)
            };
            var request = new RequestMessage("hello world", 0.1);
            _ = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None);
        }
        
        Assert.That(_heartbeatReplies, Has.All.Matches<HeartbeatMessage>(m => m.Progress == 0.5));

        serverStop.Cancel();
        await serverTask;
    }

    [Test]
    public async Task RequestReplyWhenHandlerThrows_ErrorReturned()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns<ReplyMessage>(args => throw new InvalidOperationException("handler error"));
        
        var clientId = $"TestPipe.{Guid.NewGuid()}";
        var pipeServer = new PipeTransportServer(_serverLogger, "TestPipe", "Heartbeat.TestPipe", 1, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(messageHandler, _heartbeatHandler, serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "TestPipe", "Heartbeat.TestPipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var exception = Assert.ThrowsAsync<PipeServerException>(
                () => pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("handler error"));
        }

        serverStop.Cancel();
        await serverTask;
    }    
}