using RpcPipes.PipeTransport;

namespace RpcPipes.PipeData;

public class PipeMessageHeader
{
    public Guid MessageId { get; set; }

    public virtual async Task WriteHeaderToStream(PipeChunkWriteStream stream, CancellationToken cancellation)
    {
        await stream.WriteGuid(MessageId, cancellation);
    }

    public virtual async Task<bool> TryReadHeaderFromStream(PipeChunkReadStream stream, CancellationToken cancellation)
    {
        return await stream.ReadTransaction(
            new Func<PipeChunkReadStream, Task<bool>>[] 
            {
                s => s.TryReadGuid(val => MessageId = val, cancellation)
            }
        );
    }
}

public class PipeAsyncMessageHeader : PipeMessageHeader
{
    public string ReplyPipe { get; set; }

    public override async Task WriteHeaderToStream(PipeChunkWriteStream stream, CancellationToken cancellation)
    {
        await stream.WriteGuid(MessageId, cancellation);
        await stream.WriteString(ReplyPipe, cancellation);
    }

    public override async Task<bool> TryReadHeaderFromStream(PipeChunkReadStream stream, CancellationToken cancellation)
    {
        return await stream.ReadTransaction(
            new Func<PipeChunkReadStream, Task<bool>>[] 
            {
                s => s.TryReadGuid(val => MessageId = val, cancellation),
                s => s.TryReadString(val => ReplyPipe = val, cancellation)
            });
    }
}