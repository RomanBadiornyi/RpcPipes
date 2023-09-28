using RpcPipes.Models;
using RpcPipes.Models.PipeSerializers;
using RpcPipes.PipeData;

namespace RpcPipes.Tests.PipeSerialization;

[TestFixture]
public class PipeMessagePackSerializerTests
{
    [Test]
    public async Task SerializeDeserialize_Request_Completes()
    {
        var messagePackSerializer = new PipeMessagePackSerializer();
        var r = new PipeRequest("hello", 0);
        var request = new PipeMessageRequest<PipeRequest> 
            { Request = r, Heartbeat = TimeSpan.FromSeconds(1), Deadline = TimeSpan.FromSeconds(2) };
        using var stream = new MemoryStream();
        await messagePackSerializer.WriteRequest(request, stream, CancellationToken.None);
        stream.Position = 0;
        var requestOut = await messagePackSerializer.ReadRequest<PipeRequest>(stream, CancellationToken.None);

        Assert.That(requestOut, Is.Not.Null);
        Assert.That(requestOut.Request, Is.Not.Null);
        Assert.That(requestOut.Request.Message, Is.EqualTo(request.Request.Message));
    }

    [Test]
    public async Task SerializeDeserialize_Response_Completes()
    {
        var messagePackSerializer = new PipeMessagePackSerializer();
        var r = new PipeReply("hello");
        var e = new PipeMessageException()
        {
            ClassName = "c",
            Message = "m",
            StackTrace = new List<string> { "s" },
            InnerException = new PipeMessageException 
            {
                ClassName = "inner_c",
                Message = "inner_m",
                StackTrace = new List<string> { "inner_s" }
            } 
        };
        var request = new PipeMessageResponse<PipeReply> 
            { Reply = r, ReplyError = e };
        using var stream = new MemoryStream();
        await messagePackSerializer.WriteResponse(request, stream, CancellationToken.None);
        stream.Position = 0;
        var requestOut = await messagePackSerializer.ReadResponse<PipeReply>(stream, CancellationToken.None);

        Assert.That(requestOut, Is.Not.Null);
        Assert.That(requestOut.Reply, Is.Not.Null);
        Assert.That(requestOut.Reply.Reply, Is.EqualTo(request.Reply.Reply));
    }
}
