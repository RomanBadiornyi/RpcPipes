using System.Buffers;

namespace RpcPipes;

public class PipeProtocol
{
    private readonly Stream _stream;

    public PipeProtocol(Stream stream)
    {
        _stream = stream;
    }

    public async Task BeginTransferMessage(
        Guid messageId, int bufferSize, CancellationToken token)
    {
        var messageIdArray = messageId.ToByteArray();
        var bufferSizeArray = BitConverter.GetBytes(bufferSize);

        var bufferLength =
            messageIdArray.Length +
            bufferSizeArray.Length;

        var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);

        try
        {
            Array.Copy(
                messageIdArray, 0, buffer, 0, messageIdArray.Length);
            Array.Copy(
                bufferSizeArray, 0, buffer, messageIdArray.Length, bufferSizeArray.Length);
            await _stream.WriteAsync(buffer, 0, bufferLength, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        await WaitAcknowledge(messageId, token);

    }

    public async Task BeginTransferMessageAsync(
        Guid messageId, int bufferSize, Guid replyPipe, CancellationToken token)
    {
        var messageIdArray = messageId.ToByteArray();
        var replyPipeBytesArray = replyPipe != Guid.Empty
            ? replyPipe.ToByteArray()
            : Array.Empty<byte>();
        var bufferSizeArray = BitConverter.GetBytes(bufferSize);

        var bufferLength =
            messageIdArray.Length +
            bufferSizeArray.Length +
            replyPipeBytesArray.Length;

        var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);

        try
        {
            Array.Copy(
                messageIdArray, 0, buffer, 0, messageIdArray.Length);
            Array.Copy(
                replyPipeBytesArray, 0, buffer, messageIdArray.Length, replyPipeBytesArray.Length);                
            Array.Copy(
                bufferSizeArray, 0, buffer, messageIdArray.Length + replyPipeBytesArray.Length, bufferSizeArray.Length);
            await _stream.WriteAsync(buffer, 0, bufferLength, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        await WaitAcknowledge(messageId, token);
    }

    public async Task EndTransferMessage(
        Guid messageId, Func<Stream, CancellationToken, Task> writeFunc, int bufferSize, CancellationToken token)
    {
        var chunkBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);

        try
        {
            await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, bufferSize, _stream);
            await writeFunc.Invoke(pipeStream, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }
        await WaitAcknowledge(messageId, token);
    }

    public async Task TransferMessage(
        Guid messageId, Func<Stream, CancellationToken, Task> writeFunc, int bufferSize, CancellationToken token)
    {
        await BeginTransferMessage(messageId, bufferSize, token);
        await EndTransferMessage(messageId, writeFunc, bufferSize, token);
    }

    public async Task<(Guid? messageId, int bufferSize)> BeginReceiveMessage(
        Action<Guid> onAcceptAction, CancellationToken token)
    {
        const int bufferLength =
            16 +
            sizeof(int);
        var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);

        try
        {
            var bufferReadLength = await _stream.ReadAsync(buffer, 0, bufferLength, token);
            if (bufferReadLength == bufferLength)
            {
                var messageIdArray = new byte[16];
                Array.Copy(buffer, messageIdArray, 16);
                var messageId = new Guid(messageIdArray);
                var bufferSize = BitConverter.ToInt32(buffer, 16);

                onAcceptAction?.Invoke(messageId);

                await SendAcknowledge(messageId, true, token);
                return (messageId, bufferSize);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        return (null, 0);
    }

    public async Task<(Guid? messageId, Guid? replyPipe, int bufferSize)> BeginReceiveMessageAsync(
        Action<Guid> onAcceptAction, CancellationToken token)
    {
        const int bufferLength =
            16 +
            16 +
            sizeof(int);
        var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);

        try
        {
            var bufferReadLength = await _stream.ReadAsync(buffer, 0, bufferLength, token);
            if (bufferReadLength == bufferLength)
            {
                var messageIdArray = new byte[16];
                var replyPipeArray = new byte[16];
                Array.Copy(buffer, messageIdArray, 16);
                Array.Copy(buffer, 16, replyPipeArray, 0, 16);
                var messageId = new Guid(messageIdArray);
                var replyPipe = new Guid(replyPipeArray);
                var bufferSize = BitConverter.ToInt32(buffer, 16 + 16);

                onAcceptAction?.Invoke(messageId);

                await SendAcknowledge(messageId, true, token);
                return (messageId, replyPipe, bufferSize);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
        return (null, null, 0);
    }

    public async Task<T> EndReceiveMessage<T>(
        Guid messageId, Func<Stream, CancellationToken, ValueTask<T>> readFunc, int bufferSize, CancellationToken token)
    {
        T message;

        var chunkBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            using var pipeStream = new PipeChunkReadStream(chunkBuffer, bufferSize, _stream);
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
        var (messageId, bufferSize) = await BeginReceiveMessage(null, token);
        if (bufferSize > 0)
            return await EndReceiveMessage(messageId.Value, readFunc, bufferSize, token);
        return default;
    }

    private async Task WaitAcknowledge(
        Guid messageId, CancellationToken token)
    {
        const int bufferLength = 16 + sizeof(bool);
        var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);

        var messageIdReceived = Guid.Empty;
        var ackReceived = false;
        try
        {
            var bufferReadLength = await _stream.ReadAsync(buffer, 0, bufferLength, token);
            if (bufferReadLength == bufferLength)
            {
                var messageIdReceivedArray = new byte[16];
                Array.Copy(buffer, messageIdReceivedArray, 16);
                messageIdReceived = new Guid(messageIdReceivedArray);
                ackReceived = BitConverter.ToBoolean(buffer, 16);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }

        if (messageIdReceived == messageId && ackReceived)
            return;
        throw new InvalidOperationException($"Server did not acknowledge receiving of request message {messageId}");
    }

    private async Task SendAcknowledge(
        Guid messageId, bool ack, CancellationToken token)
    {
        var messageIdArray = messageId.ToByteArray();
        var ackArray = BitConverter.GetBytes(ack);
        var bufferLength = messageIdArray.Length + ackArray.Length;

        var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);

        try
        {
            Array.Copy(messageIdArray, buffer, messageIdArray.Length);
            Array.Copy(ackArray, 0, buffer, messageIdArray.Length, ackArray.Length);
            await _stream.WriteAsync(buffer, 0, bufferLength, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}