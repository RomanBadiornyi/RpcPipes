using NSubstitute;
using RpcPipes.Models;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.PipeData;
using RpcPipes.PipeExceptions;

namespace RpcPipes.Tests.PipeClientServer;

[TestFixture]
public class PipeClientServerSerializerTests : BasePipeClientServerTests
{    
    [Test]
    public async Task RequestReply_WhenServerDeserializeThrows_ErrorReturned()
    {        
        var serializer = Substitute.ForPartsOf<PipeJsonSerializer>();        
        serializer.ReadRequest<PipeRequest>(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<PipeMessageRequest<PipeRequest>>(_ => throw new InvalidOperationException("deserialize server error"));
        
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, serializer);
        ServerTask = pipeServer.Start(MessageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, Serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 0);
            var requestContext = new PipeRequestContext();
            var exception = Assert.ThrowsAsync<PipeServerException>(
                () => pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.Message, Does.Contain("deserialize server error"));
        }

        ServerStop.Cancel();
        await ServerTask;
    }

    [Test]
    public async Task RequestReply_WhenClientDeserializeThrows_ErrorReturned()
    {        
        var serializer = Substitute.ForPartsOf<PipeJsonSerializer>();        
        serializer.ReadResponse<PipeReply>(Arg.Any<Stream>(), Arg.Any<CancellationToken>())
            .Returns<PipeMessageResponse<PipeReply>>(args => throw new InvalidOperationException("deserialize client error"));
        
        var clientId = $"{TestContext.CurrentContext.Test.Name}.0";
        var pipeServer = new PipeTransportServer(ServerLogger, "rpc.pipe", 1, Serializer);
        ServerTask = pipeServer.Start(MessageHandler, HeartbeatHandler, ServerStop.Token);

        await using (var pipeClient = new PipeTransportClient<PipeState>(
            ClientLogger, "rpc.pipe", clientId, 1, HeartbeatMessageReceiver, serializer))
        {
            SetupClient(pipeClient);
            var request = new PipeRequest("hello world", 0);
            var requestContext = new PipeRequestContext();
            var exception = Assert.ThrowsAsync<PipeDataException>(
                () => pipeClient.SendRequest<PipeRequest, PipeReply>(request, requestContext, CancellationToken.None));
            Assert.That(exception, Is.Not.Null);
            Assert.That(exception.InnerException, Is.TypeOf<InvalidOperationException>());
            Assert.That(exception.Message, Does.Contain("deserialize client error"));
        }

        ServerStop.Cancel();
        await ServerTask;
    }  
}