using NSubstitute;
using RpcPipes.Models;
using RpcPipes.Models.PipeMessageHandlers;
using RpcPipes.Models.PipeSerializers;

namespace RpcPipes.PipeClientServer.Tests;

[TestFixture]
public class PipeClientServerTests : BasePipeClientServerTests
{    
    [Test]
    public async Task RequestReply_ReplyReceived()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(new ReplyMessage("hi"));
        
        var clientId = Guid.NewGuid();
        var pipeServer = new PipeServer<ProgressMessage>(
            _serverLogger, "TestPipe", "Progress.TestPipe", 1, _progressHandler, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(messageHandler, serverStop.Token);

        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Progress.TestPipe", clientId, 1, _progressMessageReceiver, _serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }

        serverStop.Cancel();
        await serverTask;
    }

    [Test]
    public async Task RequestReply_MultipleClients_BrokenPipeHandled()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns(new ReplyMessage("hi"));
        
        var clientId = Guid.NewGuid();
        var pipeServer = new PipeServer<ProgressMessage>(
            _serverLogger, "TestPipe", "Progress.TestPipe", 1, _progressHandler, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(messageHandler, serverStop.Token);

        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Progress.TestPipe", clientId, 1, _progressMessageReceiver, _serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }
        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Progress.TestPipe", clientId, 1, _progressMessageReceiver, _serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var reply = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, CancellationToken.None);
            Assert.That(reply.Reply, Is.EqualTo("hi"));
        }

        serverStop.Cancel();
        await serverTask;
    }

    [Test]
    public async Task RequestReply_ProgressUpdated()
    {
        var progressHandler = Substitute.For<IPipeProgressHandler<ProgressMessage>>();
        progressHandler.GetProgress(Arg.Any<Guid>())
            .Returns(args => new ProgressMessage(0.1, ""), args => new ProgressMessage(0.5, ""), args => new ProgressMessage(1.0, ""));
        
        var clientId = Guid.NewGuid();
        var pipeServer = new PipeServer<ProgressMessage>(
            _serverLogger, "TestPipe", "Progress.TestPipe", 1, progressHandler, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(_messageHandler, serverStop.Token);

        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Progress.TestPipe", clientId, 1, _progressMessageReceiver, _serializer))
        {
            pipeClient.ProgressFrequency = TimeSpan.FromMilliseconds(10);
            var request = new RequestMessage("hello world", 0.1);
            _ = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, CancellationToken.None);
        }

        Assert.That(_progressReplies, Has.Some.Matches<ProgressMessage>(m => m.Progress == 0.1));
        Assert.That(_progressReplies, Has.Some.Matches<ProgressMessage>(m => m.Progress == 0.5));
        Assert.That(_progressReplies, Has.Some.Matches<ProgressMessage>(m => m.Progress == 1.0));
        
        serverStop.Cancel();
        await serverTask;
    }

    [Test]
    public async Task RequestReply_IntermediateMessageStages_ProgressUpdated()
    {
        var messageHandler = Substitute.ForPartsOf<PipeMessageHandler>();
        messageHandler.GetProgress(Arg.Any<object>())
            .Returns(args => new ProgressMessage(0.5, ""));        
        var serializer = Substitute.ForPartsOf<PipeSerializer>();        
        serializer.Deserialize<RequestMessage>(Arg.Any<Stream>(), Arg.Any<CancellationToken>())            
            .Returns(args => _serializer.Deserialize<RequestMessage>((Stream)args[0], (CancellationToken)args[1]))
            .AndDoes(x => Task.Delay(TimeSpan.FromMilliseconds(50)).Wait());
        serializer.Serialize(Arg.Any<PipeMessageResponse<ReplyMessage>>(), Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns(args => _serializer.Serialize((PipeMessageResponse<ReplyMessage>)args[0], (Stream)args[1], (CancellationToken)args[2]))
            .AndDoes(x => Task.Delay(TimeSpan.FromMilliseconds(50)).Wait());

        var clientId = Guid.NewGuid();
        var pipeServer = new PipeServer<ProgressMessage>(
            _serverLogger, "TestPipe", "Progress.TestPipe", 1, _progressHandler, serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(messageHandler, serverStop.Token);

        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Progress.TestPipe", clientId, 1, _progressMessageReceiver, _serializer))
        {
            pipeClient.ProgressFrequency = TimeSpan.FromMilliseconds(10);
            var request = new RequestMessage("hello world", 0.1);
            _ = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, CancellationToken.None);
        }
        
        Assert.That(_progressReplies, Has.All.Matches<ProgressMessage>(m => m.Progress == 0.5));

        serverStop.Cancel();
        await serverTask;
    }

    [Test]
    public async Task RequestReplyWhenHandlerThrows_ErrorReturned()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns<ReplyMessage>(args => throw new InvalidOperationException("handler error"));
        
        var clientId = Guid.NewGuid();
        var pipeServer = new PipeServer<ProgressMessage>(
            _serverLogger, "TestPipe", "Progress.TestPipe", 1, _progressHandler, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(messageHandler, serverStop.Token);

        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Progress.TestPipe", clientId, 1, _progressMessageReceiver, _serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var exception = Assert.ThrowsAsync<PipeServerException>(
                () => pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, CancellationToken.None));
            Assert.That(exception.Message, Does.Contain("handler error"));
        }

        serverStop.Cancel();
        await serverTask;
    }    
}