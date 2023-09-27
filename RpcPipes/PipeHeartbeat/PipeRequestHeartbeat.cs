using RpcPipes.PipeTransport;

namespace RpcPipes.PipeHeartbeat;

public class PipeRequestHeartbeat
{
    public Guid Id { get; private set; }
    public bool Active { get; private set; }

    public PipeRequestHeartbeat()
    {
    }

    public PipeRequestHeartbeat(Guid id, bool active)
    {
        Id = id;
        Active = active;
    }

    public async Task WriteToStream(PipeChunkWriteStream stream, CancellationToken cancellation)
    {
        await stream.WriteGuid(Id, cancellation);
        await stream.WriteBoolean(Active, cancellation);
    }

    public async ValueTask<PipeRequestHeartbeat> ReadFromStream(PipeChunkReadStream stream, CancellationToken cancellation)
    {
        var active = false;
        var messageRead = await stream.ReadTransaction(
            new Func<PipeChunkReadStream, Task<bool>>[]
            {
                s => s.TryReadGuid(val => Id = val, cancellation),
                s => s.TryReadBoolean(val => active = val, cancellation)
            }
        );
        Active = active && messageRead;
        return this;
    }
}