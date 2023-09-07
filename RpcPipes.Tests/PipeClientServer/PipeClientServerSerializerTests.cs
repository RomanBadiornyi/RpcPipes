using NSubstitute;
using RpcPipes.Models;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.PipeData;

namespace RpcPipes.Tests.PipeClientServer;

[TestFixture]
public class PipeClientServerSerializerTests : BasePipeClientServerTests
{    
    [Test]
    public async Task RequestReply_WhenServerDeserializeThrows_ErrorReturned()
    {        
        var serializer = Substitute.ForPartsOf<PipeSerializer>();        
        serializer.ReadRequest<RequestMessage>(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<PipeMessageRequest<RequestMessage>>(args => throw new InvalidOperationException("deserialize server error"));
        
        var clientId = $"TestPipe.{Guid.NewGuid()}";
        var pipeServer = new PipeTransportServer(_serverLogger, "TestPipe", "Heartbeat.TestPipe", 1, serializer);
        _serverTask = pipeServer.Start(_messageHandler, _heartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "TestPipe", "Heartbeat.TestPipe", clientId, 1, _heartbeatMessageReceiver, _serializer))
        {
            pipeClient.Cancellation.CancelAfter(TimeSpan.FromSeconds(10));
            var request = new RequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var exception = Assert.ThrowsAsync<PipeServerException>(
                () => pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None));
            Assert.That(exception.Message, Does.Contain("deserialize server error"));
        }

        _serverStop.Cancel();
        await _serverTask;
    }

    [Test]
    public async Task RequestReply_WhenClientDeserializeThrows_ErrorReturned()
    {        
        var serializer = Substitute.ForPartsOf<PipeSerializer>();        
        serializer.ReadResponse<ReplyMessage>(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<PipeMessageResponse<ReplyMessage>>(args => throw new InvalidOperationException("deserialize client error"));
        
        var clientId = $"TestPipe.{Guid.NewGuid()}";
        var pipeServer = new PipeTransportServer(_serverLogger, "TestPipe", "Heartbeat.TestPipe", 1, _serializer);
        _serverTask = pipeServer.Start(_messageHandler, _heartbeatHandler, _serverStop.Token);

        await using (var pipeClient = new PipeTransportClient<HeartbeatMessage>(
            _clientLogger, "TestPipe", "Heartbeat.TestPipe", clientId, 1, _heartbeatMessageReceiver, serializer))
        {
            pipeClient.Cancellation.CancelAfter(TimeSpan.FromSeconds(10));
            var request = new RequestMessage("hello world", 0);
            var requestContext = new PipeRequestContext();
            var exception = Assert.ThrowsAsync<InvalidOperationException>(
                () => pipeClient.SendRequest<RequestMessage, ReplyMessage>(request, requestContext, CancellationToken.None));
            Assert.That(exception.Message, Does.Contain("deserialize client error"));
        }

        _serverStop.Cancel();
        await _serverTask;
    }  
}