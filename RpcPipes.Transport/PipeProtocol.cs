using System.Buffers;

namespace RpcPipes.Transport;

public class PipeProtocol
{
    private readonly Stream _stream;
    private readonly IPipeMessageSerializer _serializer;

    public PipeProtocol(Stream stream, IPipeMessageSerializer serializer)
    {
        _stream = stream;
        _serializer = serializer;
    }

    public async Task TransferMessage<T>(Guid messageId, int bufferSize, T request, CancellationToken token)
    {
        await BeginTranferMessage();
        await WaitAcknowledge(messageId, token);

        await StartTransferMessage();
        await WaitAcknowledge(messageId, token);

        async Task BeginTranferMessage()
        {
            var messageIdArray = messageId.ToByteArray();
            var bufferSizeArray = BitConverter.GetBytes(bufferSize);
            var bufferLength = messageIdArray.Length + bufferSizeArray.Length;

            var buffer = ArrayPool<byte>.Shared.Rent(bufferLength);

            try
            {
                Array.Copy(messageIdArray, buffer, messageIdArray.Length);
                Array.Copy(bufferSizeArray, 0, buffer, messageIdArray.Length, bufferSizeArray.Length);
                await _stream.WriteAsync(buffer, 0, buffer.Length, token);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        async Task StartTransferMessage()
        {
            var chunkBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);

            try
            {
                await using var pipeStream = new PipeChunkWriteStream(chunkBuffer, bufferSize, _stream);
                await _serializer.Serialize(request, pipeStream, token);
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(chunkBuffer);
            }
        }
    }

    public async Task<(Guid? messageId, int bufferSize)> BeginReceiveMessage(Action<Guid> onAcceptAction, CancellationToken token)
    {
        const int bufferLength = 16 + sizeof(int);
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

    public async Task<T> EndReceiveMessage<T>(Guid messageId, int bufferSize, CancellationToken token)
    {
        T message;

        var chunkBuffer = ArrayPool<byte>.Shared.Rent(bufferSize);
        try
        {
            using var pipeStream = new PipeChunkReadStream(chunkBuffer, bufferSize, _stream);
            message = await _serializer.Deserialize<T>(pipeStream, token);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(chunkBuffer);
        }

        await SendAcknowledge(messageId, true, token);
        return message;
    }

    public async Task<T> ReceiveMessage<T>(CancellationToken token)
    {
        var (messageId, bufferSize) = await BeginReceiveMessage(null, token);
        if (bufferSize > 0)
            return await EndReceiveMessage<T>(messageId.Value, bufferSize, token);
        return default;
    }

    private async Task WaitAcknowledge(Guid messageId, CancellationToken token)
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

    private async Task SendAcknowledge(Guid messageId, bool ack, CancellationToken token)
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