using System.Buffers;
using RpcPipes.PipeData;
using RpcPipes.PipeExceptions;

namespace RpcPipes.PipeTransport;

public class PipeProtocol
{
    private readonly Stream _stream;
    private readonly int _headerBuffer;
    private readonly int _contentBuffer;

    public PipeProtocol(Stream stream, int headerBuffer, int contentBuffer)
    {
        _stream = stream;
        _headerBuffer = headerBuffer;
        _contentBuffer = contentBuffer;
    }    

    public async Task<bool> BeginTransferMessage(
        PipeMessageHeader header, CancellationToken cancellation)
    {
        await SendMessageHeader(header, cancellation);
        return await WaitAcknowledge(header.MessageId, false, cancellation);
    }

    public async Task EndTransferMessage(
        Guid messageId, Func<Stream, CancellationToken, Task> writeFunc, CancellationToken cancellation)
    {
        await SendMessage(writeFunc, cancellation);
        await WaitAcknowledge(messageId, true, cancellation);
    }

    public async Task TransferMessage(PipeMessageHeader header, Func<Stream, CancellationToken, Task> writeFunc, CancellationToken cancellation)
    {
        await BeginTransferMessage(header, cancellation);
        await EndTransferMessage(header.MessageId, writeFunc, cancellation);
    }

    public async Task<bool> TryTransferMessage(PipeMessageHeader header, Func<Stream, CancellationToken, Task> writeFunc, CancellationToken cancellation)
    {
        await SendMessageHeader(header, cancellation);
        var ack = await WaitAcknowledge(header.MessageId, true, cancellation);
        if (ack)
            await EndTransferMessage(header.MessageId, writeFunc, cancellation);
        return ack;
    }

    public async Task<T> BeginReceiveMessage<T>(T messageHeader, Func<T, bool> onAcceptAction, CancellationToken cancellation)
        where T : PipeMessageHeader
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            if (await messageHeader.TryReadHeaderFromStream(pipeStream, cancellation))
            {
                var accept = onAcceptAction?.Invoke(messageHeader) ?? true;
                await SendAcknowledge(messageHeader.MessageId, accept, cancellation);
                return messageHeader;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
        return null;
    }

    public async Task<T> EndReceiveMessage<T>(Guid messageId, Func<Stream, CancellationToken, ValueTask<T>> readFunc, CancellationToken cancellation)
    {
        T message;
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_contentBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _contentBuffer, _stream, cancellation);
            message = await readFunc.Invoke(pipeStream, cancellation);
        }
        catch (Exception e) when (e is not PipeNetworkException)
        {
            throw new PipeDataException(e.Message, e);
        }         
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
            await SendAcknowledge(messageId, true, cancellation);
        }        
        return message;
    }

    public async Task<(T Message, bool Received)> ReceiveMessage<T>(Func<Stream, CancellationToken, ValueTask<T>> readFunc, CancellationToken cancellation)
    {
        var header = await BeginReceiveMessage(new PipeMessageHeader(), null, cancellation);
        if (header != null)
            return (await EndReceiveMessage(header.MessageId, readFunc, cancellation), true);
        return (default, false);
    }

    private async Task SendMessageHeader(PipeMessageHeader header, CancellationToken cancellation)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            await header.WriteHeaderToStream(pipeStream, cancellation);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
    }

    private async Task SendMessage(Func<Stream, CancellationToken, Task> writeFunc, CancellationToken cancellation)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_contentBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _contentBuffer, _stream, cancellation);
            await writeFunc.Invoke(pipeStream, cancellation);
        }
        catch (Exception e) when (e is not PipeNetworkException)
        {
            throw new PipeDataException(e.Message, e);
        } 
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
    }

    private async Task<bool> WaitAcknowledge(
        Guid messageId, bool allowConnectionDrop, CancellationToken cancellation)
    {
        Guid messageIdReceived;
        bool ackReceived;

        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            ackReceived = false;
            var messageRead = await pipeStream.ReadTransaction(
                new Func<PipeChunkReadStream, Task<bool>>[] 
                {
                    s => s.TryReadGuid(val => messageIdReceived = val, cancellation),
                    s => s.TryReadBoolean(val => ackReceived = val, cancellation)
                }
            );
            ackReceived = ackReceived && messageRead;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }        
        if (messageIdReceived == messageId)
            return ackReceived;
        //could happen that server receives message and sends ack but before we receive this ack here - 
        //server drops connection due to various reasons, in this case we will read empty stream
        //there are 2 main reasons for ack logic:
        //- is to prevent sending wrong data during BeginSend call 
        //  which is not the case here, as those pass allowConnectionDrop here)
        //- is to prevent client from dropping connection after sending data before server fully reads this data 
        //  which also not the case here as this is server who drops connection, not the client
        if (messageIdReceived == Guid.Empty && allowConnectionDrop)
            return false;
        throw new PipeProtocolException($"Server did not acknowledge receiving of request message {messageId}, received {messageIdReceived}", null);
    }

    private async Task SendAcknowledge(
        Guid messageId, bool ack, CancellationToken cancellation)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            await pipeStream.WriteGuid(messageId, cancellation);
            await pipeStream.WriteBoolean(ack, cancellation);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
    }
}