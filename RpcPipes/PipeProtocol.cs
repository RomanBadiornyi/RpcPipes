using System.Buffers;
using System.IO.Pipes;

namespace RpcPipes;

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
        PipeMessageHeader header, CancellationToken token)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _headerBuffer, _stream, token);
            await pipeStream.WriteGuid(header.MessageId, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
        await WaitAcknowledge(header.MessageId, token);
    }

    public async Task BeginTransferMessageAsync(
        PipeAsyncMessageHeader header, CancellationToken token)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _headerBuffer, _stream, token);
            await pipeStream.WriteGuid(header.MessageId, token);
            await pipeStream.WriteString(header.ReplyPipe, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
        await WaitAcknowledge(header.MessageId, token);
    }

    public async Task EndTransferMessage(
        Guid messageId, Func<Stream, CancellationToken, Task> writeFunc, CancellationToken token)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_contentBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _contentBuffer, _stream, token);
            await writeFunc.Invoke(pipeStream, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
        await WaitAcknowledge(messageId, token);
    }

    public async Task TransferMessage(
        PipeMessageHeader header, Func<Stream, CancellationToken, Task> writeFunc, CancellationToken token)
    {
        await BeginTransferMessage(header, token);
        await EndTransferMessage(header.MessageId, writeFunc, token);
    }

    public async Task<PipeMessageHeader> BeginReceiveMessage(
        Action<Guid> onAcceptAction, CancellationToken token)
    {
        var message = new PipeMessageHeader();
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _headerBuffer, _stream, token);
            if (await ReadTransaction(
                pipeStream,
                new Func<PipeChunkReadStream, Task<bool>>[] 
                {
                    s => s.TryReadGuid(val => message.MessageId = val, token)
                }
            ))
            {
                onAcceptAction?.Invoke(message.MessageId);
                await SendAcknowledge(message.MessageId, true, token);
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
        Action<Guid> onAcceptAction, CancellationToken token)
    {
        var message = new PipeAsyncMessageHeader();
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _headerBuffer, _stream, token);
            if (await ReadTransaction(
                pipeStream,
                new Func<PipeChunkReadStream, Task<bool>>[] 
                {
                    s => s.TryReadGuid(val => message.MessageId = val, token),
                    s => s.TryReadString(val => message.ReplyPipe = val, token)
                }
            ))
            {
                onAcceptAction?.Invoke(message.MessageId);
                await SendAcknowledge(message.MessageId, true, token);
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
        Guid messageId, Func<Stream, CancellationToken, ValueTask<T>> readFunc, CancellationToken token)
    {
        T message;
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_contentBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _contentBuffer, _stream, token);
            message = await readFunc.Invoke(pipeStream, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
            await SendAcknowledge(messageId, true, token);
        }        
        return message;
    }

    public async Task<T> ReceiveMessage<T>(
        Func<Stream, CancellationToken, ValueTask<T>> readFunc, CancellationToken token)
    {
        var header = await BeginReceiveMessage(null, token);
        if (header != null)
            return await EndReceiveMessage(header.MessageId, readFunc, token);
        return default;
    }

    private async Task WaitAcknowledge(
        Guid messageId, CancellationToken token)
    {
        Guid messageIdReceived;
        bool ackReceived;

        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkReadStream(chunkBuffer, _headerBuffer, _stream, token);
            ackReceived = false;
            var messageRead = await ReadTransaction(
                pipeStream,
                new Func<PipeChunkReadStream, Task<bool>>[] 
                {
                    s => s.TryReadGuid(val => messageIdReceived = val, token),
                    s => s.TryReadBoolean(val => ackReceived = val, token)
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
        throw new InvalidOperationException($"Server did not acknowledge receiving of request message {messageId}");
    }

    private async Task SendAcknowledge(
        Guid messageId, bool ack, CancellationToken token)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(_headerBuffer);
        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, _headerBuffer, _stream, token);
            await pipeStream.WriteGuid(messageId, token);
            await pipeStream.WriteBoolean(ack, token);
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
    
    private static async Task<bool> ReadTransaction(PipeChunkReadStream stream, IEnumerable<Func<PipeChunkReadStream, Task<bool>>> reads)
    {
        var allResult = true;
        foreach (var readOperation in reads)
        {
            var result = await readOperation.Invoke(stream);
            if (!result)
            {
                allResult = false;
                break;
            }
        }
        return allResult;
    }
}