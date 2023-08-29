using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using NSubstitute;
using RpcPipes.Models;

namespace RpcPipes.PipeClientServer.Tests;

[TestFixture]
public class PipeClientServerSerializerTests : BasePipeClientServerTests
{    
    [Test]
    public async Task RequestReply_WhenServerDeserializeThrows_ErrorReturned()
    {        
        var serializer = Substitute.ForPartsOf<PipeSerializer>();        
        serializer.Deserialize<RequestMessage>(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<RequestMessage>(args => throw new InvalidOperationException("deserialize server error"));
        
        var pipeServer = new PipeServer<ProgressMessage>(
            _serverLogger, "Client.TestPipe", "TestPipe", "Progress.TestPipe", 1, serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(_messageHandler, _messageHandler, serverStop.Token);

        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Client.TestPipe", "Progress.TestPipe", 1, _progressMessageReceiver, _serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var exception = Assert.ThrowsAsync<ServiceException>(
                () => pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, CancellationToken.None));
            Assert.That(exception.Message, Does.Contain("deserialize server error"));
        }

        serverStop.Cancel();
        await serverTask;
    }

    [Test]
    public async Task RequestReply_WhenClientDeserializeThrows_ErrorReturned()
    {        
        var serializer = Substitute.ForPartsOf<PipeSerializer>();        
        serializer.Deserialize<PipeMessageResponse<ReplyMessage>>(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<PipeMessageResponse<ReplyMessage>>(args => throw new InvalidOperationException("deserialize client error"));
        
        var pipeServer = new PipeServer<ProgressMessage>(
            _serverLogger, "Client.TestPipe", "TestPipe", "Progress.TestPipe", 1, _serializer);

        var serverStop = new CancellationTokenSource();
        var serverTask = pipeServer.Start(_messageHandler, _messageHandler, serverStop.Token);

        await using (var pipeClient = new PipeClient<ProgressMessage>(
            _clientLogger, "TestPipe", "Client.TestPipe", "Progress.TestPipe", 1, _progressMessageReceiver, serializer))
        {
            var request = new RequestMessage("hello world", 0);
            var exception = Assert.ThrowsAsync<InvalidOperationException>(
                () => pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, CancellationToken.None));
            Assert.That(exception.Message, Does.Contain("deserialize client error"));
        }

        serverStop.Cancel();
        await serverTask;
    }  
}