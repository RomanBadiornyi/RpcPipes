using System.Buffers;
using RpcPipes.PipeData;
using RpcPipes.PipeExceptions;

namespace RpcPipes.PipeTransport;

public class PipeProtocol
{
    private readonly Stream _stream;
    private readonly int _headerBuffer;
    private readonly int _contentBuffer;
    private readonly ArrayPool<byte> _arrayPool;

    public PipeProtocol(Stream stream, int headerBuffer, int contentBuffer)
    {
        _stream = stream;
        _headerBuffer = headerBuffer;
        _contentBuffer = contentBuffer;
        _arrayPool = ArrayPool<byte>.Shared;
    }

    public async Task<bool> BeginTransferMessage(PipeMessageHeader header, CancellationToken cancellation)
    {
        await SendMessageHeader(header, cancellation);
        return await WaitAcknowledge(header.MessageId, false, cancellation);
    }

    public async Task<bool> TryBeginTransferMessage(PipeMessageHeader header, CancellationToken cancellation)
    {
        await SendMessageHeader(header, cancellation);
        return await WaitAcknowledge(header.MessageId, true, cancellation);
    }

    public async Task<bool> EndTransferMessage(Guid messageId, Func<Stream, CancellationToken, Task> writeFunc, CancellationToken cancellation)
    {
        await SendMessage(writeFunc, cancellation);
        return await WaitAcknowledge(messageId, true, cancellation);
    }

    public async Task<bool> TransferMessage(PipeMessageHeader header, Func<Stream, CancellationToken, Task> writeFunc, CancellationToken cancellation)
    {
        await BeginTransferMessage(header, cancellation);
        return await EndTransferMessage(header.MessageId, writeFunc, cancellation);
    }

    public async Task<bool> TryTransferMessage(PipeMessageHeader header, Func<Stream, CancellationToken, Task> writeFunc, CancellationToken cancellation)
    {
        if (await BeginTransferMessage(header, cancellation))
            await EndTransferMessage(header.MessageId, writeFunc, cancellation);
        return false;
    }

    public async Task<TMessage> BeginReceiveMessage<THeader, TMessage>(Func<THeader, TMessage> messageFunc, CancellationToken cancellation)
        where THeader : PipeMessageHeader, new()
        where TMessage : class
    {
        var messageHeader = new THeader();
        var message = default(TMessage);
        var readBytes = 0L;
        var chunkBuffer = _arrayPool.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            var headerRead = await messageHeader.TryReadHeaderFromStream(pipeStream, cancellation);
            readBytes = pipeStream.Position;
            if (headerRead)
            {
                message = messageFunc.Invoke(messageHeader);
                return message;
            }
        }
        finally
        {
            _arrayPool.Return(chunkBuffer);
            if (readBytes > 0)
                await SendAcknowledge(new PipeAckMessage(messageHeader.MessageId, message != default), cancellation);
        }
        return message;
    }

    public async Task<T> EndReceiveMessage<T>(Guid messageId, Func<Stream, CancellationToken, ValueTask<T>> readFunc, CancellationToken cancellation)
    {
        T message;
        var readBytes = 0L;
        var completed = false;
        var chunkBuffer = _arrayPool.Rent(_contentBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _contentBuffer, _stream, cancellation);
            message = await readFunc.Invoke(pipeStream, cancellation);
            readBytes = pipeStream.Position;
            completed = true;
        }
        catch (Exception e) when (e is not PipeNetworkException)
        {
            throw new PipeDataException(e.Message, e);
        }
        finally
        {
            _arrayPool.Return(chunkBuffer);
            if (readBytes > 0)
                await SendAcknowledge(new PipeAckMessage(messageId, completed), cancellation);
        }
        return message;
    }

    public async Task<(T Message, bool Received)> TryReceiveMessage<T>(Func<Stream, CancellationToken, ValueTask<T>> readFunc, CancellationToken cancellation)
    {
        var message = await BeginReceiveMessage<PipeMessageHeader, PipeMessageHeader>(HeaderToMessage, cancellation);
        if (message != default && message.Ready)
            return (await EndReceiveMessage(message.MessageId, readFunc, cancellation), true);
        return (default, false);

        static PipeMessageHeader HeaderToMessage(PipeMessageHeader header)
            => header;
    }

    private async Task SendMessageHeader(PipeMessageHeader header, CancellationToken cancellation)
    {
        var chunkBuffer = _arrayPool.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            await header.WriteHeaderToStream(pipeStream, cancellation);
        }
        finally
        {
            _arrayPool.Return(chunkBuffer);
        }
    }

    private async Task SendMessage(Func<Stream, CancellationToken, Task> writeFunc, CancellationToken cancellation)
    {
        var chunkBuffer = _arrayPool.Rent(_contentBuffer);
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
            _arrayPool.Return(chunkBuffer);
        }
    }

    private async Task<bool> WaitAcknowledge(Guid messageId, bool allowConnectionDrop, CancellationToken cancellation)
    {
        var ackMessage = new PipeAckMessage();
        var chunkBuffer = _arrayPool.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            await ackMessage.ReadFromStream(pipeStream, cancellation);            
        }
        finally
        {
            _arrayPool.Return(chunkBuffer);
        }
        if (ackMessage.MessageId == messageId)
            return ackMessage.AckReceived;
        //could happen that server receives message and sends ack but before we receive this ack here -
        //server drops connection due to various reasons, in this case we will read empty stream
        //there are 2 main reasons for ack logic:
        //- is to prevent sending wrong data during BeginSend call
        //  which is not the case here, as those pass allowConnectionDrop here)
        //- is to prevent client from dropping connection after sending data before server fully reads this data
        //  which also not the case here as this is server who drops connection, not the client
        if (ackMessage.MessageId == Guid.Empty && allowConnectionDrop)
            return false;
        throw new PipeProtocolException($"Server did not acknowledge receiving of request message {messageId}, received {ackMessage.MessageId}", null);
    }

    private async Task SendAcknowledge(PipeAckMessage message, CancellationToken cancellation)
    {
        var chunkBuffer = _arrayPool.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            await message.WriteToStream(pipeStream, cancellation);
        }
        finally
        {
            _arrayPool.Return(chunkBuffer);
        }
    }    
}