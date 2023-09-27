using RpcPipes.PipeTransport;

namespace RpcPipes.PipeData;

public class PipeMessageHeader
{
    public Guid MessageId { get; set; }
    public bool Ready { get; protected set;}    

    public virtual async Task WriteHeaderToStream(PipeChunkWriteStream stream, CancellationToken cancellation)
    {
        await stream.WriteGuid(MessageId, cancellation);
    }

    public virtual async Task<bool> TryReadHeaderFromStream(PipeChunkReadStream stream, CancellationToken cancellation)
    {
        Ready =  await stream.ReadTransaction(
            new Func<PipeChunkReadStream, Task<bool>>[] 
            {
                s => s.TryReadGuid(val => MessageId = val, cancellation)
            }
        );
        return Ready;
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
        Ready = await stream.ReadTransaction(
            new Func<PipeChunkReadStream, Task<bool>>[] 
            {
                s => s.TryReadGuid(val => MessageId = val, cancellation),
                s => s.TryReadString(val => ReplyPipe = val, cancellation)
            });
        return Ready;
    }
}

public class PipeAckMessage
{
    public Guid MessageId { get; set;}
    public bool AckReceived { get; set;}

    public PipeAckMessage()
    {
    }

    public PipeAckMessage(Guid messageId, bool ackReceived)
    {
        MessageId = messageId;
        AckReceived = ackReceived;
    }

    public async Task WriteToStream(PipeChunkWriteStream stream, CancellationToken cancellation)
    {
        await stream.WriteGuid(MessageId, cancellation);
        await stream.WriteBoolean(AckReceived, cancellation);
    }

    public async Task ReadFromStream(PipeChunkReadStream stream, CancellationToken cancellation)
    {
        var ackReceived = false;
        var messageRead = await stream.ReadTransaction(
            new Func<PipeChunkReadStream, Task<bool>>[]
            {
                s => s.TryReadGuid(val => MessageId = val, cancellation),
                s => s.TryReadBoolean(val => ackReceived = val, cancellation)
            }
        );
        AckReceived = ackReceived && messageRead;
    }
}