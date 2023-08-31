namespace RpcPipes.Tests;

public class PipeChunkStreamTests
{
    [TestCase(16 + 5, 16, 16, 16)]
    [TestCase(16 + 5, 12, 60, 60)]
    [TestCase(16 + 5, 12, 14, 60)]
    public async Task WriteAndReadMultipleTimesContentLessThanBufferAsync_ReadExpected(int bufferLength, int chunkWriteLength, int chunkReadLength, int dataLength)
    {
        var dataInputBuffer = new byte[dataLength];
        var dataOutputBuffer = new byte[dataLength];
        Enumerable.Range(0, dataLength).ToList().ForEach(i => dataInputBuffer[i] = (byte)(i & byte.MaxValue));

        var memoryStream = new MemoryStream();

        var inputBuffer = new byte[bufferLength];
        var input = new PipeChunkWriteStream(inputBuffer, inputBuffer.Length, memoryStream, CancellationToken.None);

        var outputBuffer = new byte[bufferLength];
        var output = new PipeChunkReadStream(outputBuffer, outputBuffer.Length, memoryStream, CancellationToken.None);

        var writeLength = 0;
        while (writeLength < dataLength)
        {
            await input.WriteAsync(dataInputBuffer.AsSpan().Slice(writeLength, chunkWriteLength).ToArray(), 0, chunkWriteLength);
            writeLength += chunkWriteLength;
        }
        await input.DisposeAsync();

        memoryStream.Position = 0;

        var tempBuffer = new byte[chunkReadLength];
        var readCompleted = false;
        var totalRead = 0;
        int readBytes;
        while (!readCompleted)
        {
            readBytes = await output.ReadAsync(tempBuffer, 0, tempBuffer.Length);
            Array.Copy(tempBuffer, 0, dataOutputBuffer, totalRead, readBytes);
            totalRead += readBytes;
            readCompleted = readBytes < tempBuffer.Length;
        }
        readBytes = await output.ReadAsync(tempBuffer, 0, tempBuffer.Length);
        await output.DisposeAsync();

        Assert.That(readBytes, Is.EqualTo(0));
        Assert.That(totalRead, Is.EqualTo(dataLength));
        Assert.That(dataInputBuffer, Is.EquivalentTo(dataOutputBuffer));
    }

    [TestCase(16 + 5, 16, 16, 16)]
    [TestCase(16 + 5, 12, 60, 60)]
    [TestCase(16 + 5, 12, 14, 60)]
    public void WriteAndReadMultipleTimesContentLessThanBufferSync_ReadExpected(int bufferLength, int chunkWriteLength, int chunkReadLength, int dataLength)
    {
        var dataInputBuffer = new byte[dataLength];
        var dataOutputBuffer = new byte[dataLength];
        Enumerable.Range(0, dataLength).ToList().ForEach(i => dataInputBuffer[i] = (byte)(i & byte.MaxValue));

        var memoryStream = new MemoryStream();

        var inputBuffer = new byte[bufferLength];
        var input = new PipeChunkWriteStream(inputBuffer, inputBuffer.Length, memoryStream, CancellationToken.None);

        var outputBuffer = new byte[bufferLength];
        var output = new PipeChunkReadStream(outputBuffer, outputBuffer.Length, memoryStream, CancellationToken.None);

        var writeLength = 0;
        while (writeLength < dataLength)
        {
            input.Write(dataInputBuffer.AsSpan().Slice(writeLength, chunkWriteLength).ToArray(), 0, chunkWriteLength);
            writeLength += chunkWriteLength;
        }
        input.Dispose();

        memoryStream.Position = 0;

        var tempBuffer = new byte[chunkReadLength];
        var readCompleted = false;
        var totalRead = 0;
        int readBytes;
        while (!readCompleted)
        {
            readBytes = output.Read(tempBuffer, 0, tempBuffer.Length);
            Array.Copy(tempBuffer, 0, dataOutputBuffer, totalRead, readBytes);
            totalRead += readBytes;
            readCompleted = readBytes < tempBuffer.Length;
        }
        readBytes = output.Read(tempBuffer, 0, tempBuffer.Length);
        output.Dispose();

        Assert.That(readBytes, Is.EqualTo(0));
        Assert.That(totalRead, Is.EqualTo(dataLength));
        Assert.That(dataInputBuffer, Is.EquivalentTo(dataOutputBuffer));
    }
}