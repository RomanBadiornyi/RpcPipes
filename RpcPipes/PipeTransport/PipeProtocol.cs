using System.Buffers;
using System.IO.Pipes;
using RpcPipes.PipeData;

namespace RpcPipes.PipeTransport;

public class PipeProtocol
{
    private readonly Stream _stream;
    private readonly int _headerBuffer;
    private readonly int _contentBuffer;

    public bool Connected => CheckConnection(_stream);

    public PipeProtocol(Stream stream, int headerBuffer, int contentBuffer)
    {
        _stream = stream;
        _headerBuffer = headerBuffer;
        _contentBuffer = contentBuffer;
    }    

    public async Task BeginTransferMessage(
        PipeMessageHeader header, CancellationToken cancellation)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            await pipeStream.WriteGuid(header.MessageId, cancellation);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
        await WaitAcknowledge(header.MessageId, cancellation);
    }

    public async Task BeginTransferMessageAsync(
        PipeAsyncMessageHeader header, CancellationToken cancellation)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            await pipeStream.WriteGuid(header.MessageId, cancellation);
            await pipeStream.WriteString(header.ReplyPipe, cancellation);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
        await WaitAcknowledge(header.MessageId, cancellation);
    }

    public async Task EndTransferMessage(
        Guid messageId, Func<Stream, CancellationToken, Task> writeFunc, CancellationToken cancellation)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_contentBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _contentBuffer, _stream, cancellation);
            await writeFunc.Invoke(pipeStream, cancellation);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
        await WaitAcknowledge(messageId, cancellation);
    }

    public async Task TransferMessage(
        PipeMessageHeader header, Func<Stream, CancellationToken, Task> writeFunc, CancellationToken cancellation)
    {
        await BeginTransferMessage(header, cancellation);
        await EndTransferMessage(header.MessageId, writeFunc, cancellation);
    }

    public async Task<PipeMessageHeader> BeginReceiveMessage(
        Action<Guid> onAcceptAction, CancellationToken cancellation)
    {
        var message = new PipeMessageHeader();
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            if (await pipeStream.ReadTransaction(
                new Func<PipeChunkReadStream, Task<bool>>[] 
                {
                    s => s.TryReadGuid(val => message.MessageId = val, cancellation)
                }
            ))
            {
                onAcceptAction?.Invoke(message.MessageId);
                await SendAcknowledge(message.MessageId, true, cancellation);
                return message;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
        return null;
    }

    public async Task<PipeAsyncMessageHeader> BeginReceiveMessageAsync(
        Action<Guid, string> onAcceptAction, CancellationToken cancellation)
    {
        var message = new PipeAsyncMessageHeader();
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _headerBuffer, _stream, cancellation);
            if (await pipeStream.ReadTransaction(
                new Func<PipeChunkReadStream, Task<bool>>[] 
                {
                    s => s.TryReadGuid(val => message.MessageId = val, cancellation),
                    s => s.TryReadString(val => message.ReplyPipe = val, cancellation)
                }
            ))
            {
                onAcceptAction?.Invoke(message.MessageId, message.ReplyPipe);
                await SendAcknowledge(message.MessageId, true, cancellation);
                return message;
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
        return null;
    }

    public async Task<T> EndReceiveMessage<T>(
        Guid messageId, Func<Stream, CancellationToken, ValueTask<T>> readFunc, CancellationToken cancellation)
    {
        T message;
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_contentBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _contentBuffer, _stream, cancellation);
            message = await readFunc.Invoke(pipeStream, cancellation);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
            await SendAcknowledge(messageId, true, cancellation);
        }        
        return message;
    }

    public async Task<T> ReceiveMessage<T>(
        Func<Stream, CancellationToken, ValueTask<T>> readFunc, CancellationToken cancellation)
    {
        var header = await BeginReceiveMessage(null, cancellation);
        if (header != null)
            return await EndReceiveMessage(header.MessageId, readFunc, cancellation);
        return default;
    }

    private async Task WaitAcknowledge(
        Guid messageId, CancellationToken cancellation)
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
        if (messageIdReceived == messageId && ackReceived)
            return;
        if (messageIdReceived == messageId && !ackReceived || messageIdReceived == Guid.Empty)
            throw new InvalidOperationException($"Server did not acknowledge receiving of request message {messageId}");

        throw new InvalidDataException($"Server did not acknowledge receiving of request message {messageId}, received {messageIdReceived}");
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

    private static bool CheckConnection(Stream stream)
    {
        if (stream is NamedPipeClientStream { IsConnected: true } client)
            return client.IsConnected;
        if (stream is NamedPipeClientStream { IsConnected: true }  server)
            return server.IsConnected;
        return false;
    }
}