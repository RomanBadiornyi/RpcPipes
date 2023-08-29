using NSubstitute;
using RpcPipes.Models;

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
        
        var pipeServer = new PipeServer<ProgressMessage>(
            _serverLogger, "Client.TestPipe", "TestPipe", "Progress.TestPipe", 1, _progressHandler, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(messageHandler, serverStop.Token);

        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Client.TestPipe", "Progress.TestPipe", 1, _progressMessageReceiver, _serializer))
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
        
        var pipeServer = new PipeServer<ProgressMessage>(
            _serverLogger, "Client.TestPipe", "TestPipe", "Progress.TestPipe", 1, progressHandler, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(_messageHandler, serverStop.Token);

        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Client.TestPipe", "Progress.TestPipe", 1, _progressMessageReceiver, _serializer))
        {
            pipeClient.ProgressFrequency = TimeSpan.FromMilliseconds(10);
            var request = new RequestMessage("hello world", 0.1);
            _ = await pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, CancellationToken.None);
        }

        Assert.That(_progressReplies, Has.Some.Matches<ProgressMessage>(m => m.Progress == 0.1));
        Assert.That(_progressReplies, Has.Some.Matches<ProgressMessage>(m => m.Progress == 0.5));
        Assert.That(_progressReplies, Has.Some.Matches<ProgressMessage>(m => m.Progress == 0.1));
        
        serverStop.Cancel();
        await serverTask;
    }

    [Test]
    public async Task RequestReplyWhenHandlerThrows_ErrorReturned()
    {
        var messageHandler = Substitute.For<IPipeMessageHandler<RequestMessage, ReplyMessage>>();
        messageHandler.HandleRequest(Arg.Any<RequestMessage>(), Arg.Any<CancellationToken>())
            .Returns<ReplyMessage>(args => throw new InvalidOperationException("handler error"));
        
        var pipeServer = new PipeServer<ProgressMessage>(
            _serverLogger, "Client.TestPipe", "TestPipe", "Progress.TestPipe", 1, _progressHandler, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(messageHandler, serverStop.Token);

        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Client.TestPipe", "Progress.TestPipe", 1, _progressMessageReceiver, _serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var exception = Assert.ThrowsAsync<ServiceException>(
                () => pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, CancellationToken.None));
            Assert.That(exception.Message, Does.Contain("handler error"));
        }

        serverStop.Cancel();
        await serverTask;
    }    
}