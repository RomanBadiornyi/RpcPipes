using System.Text;

namespace RpcPipes;

public class PipeChunkWriteStream : Stream, IAsyncDisposable
{
    private const int _bufferReserved = sizeof(int) + 1;
    private bool _closed;
    private bool _closing;

    private readonly byte[] _buffer;
    private readonly int _bufferLength;

    private int _bufferPosition;

    private readonly Stream _networkStream;
    private readonly CancellationToken _cancellationToken;

    public PipeChunkWriteStream(byte[] buffer, int bufferLength, Stream networkStream, CancellationToken cancellationToken)
    {
        if (buffer.Length > 0)
            buffer[0] = 0;

        _buffer = buffer;
        _bufferLength = bufferLength;
        _bufferPosition = _bufferReserved;

        _networkStream = networkStream;
        _cancellationToken = cancellationToken;
    }

    public Task WriteGuid(Guid id, CancellationToken token)
        => WriteAsync(id.ToByteArray(), 0, 16, token);

    public Task WriteBoolean(bool val, CancellationToken token)
        => WriteAsync(BitConverter.GetBytes(val), 0, 1, token);

    public async Task WriteString(string message, CancellationToken token)
    {
        var buffer = Encoding.UTF8.GetBytes(message);
        await WriteAsync(BitConverter.GetBytes(buffer.Length), 0, 4, token);
        await WriteAsync(buffer, 0, buffer.Length, token);
    }    

    public override void Write(byte[] buffer, int offset, int count)
    {
        if (_closed)
            throw new ObjectDisposedException("stream already disposed");
        
        var bufferWriteTotal = 0L;

        while (bufferWriteTotal < count)
        {
            var bufferCapacity = _bufferLength - _bufferPosition;
            var bufferWriteLeft = count - bufferWriteTotal;
            var bufferWriteCount = bufferWriteLeft < bufferCapacity ? bufferWriteLeft : bufferCapacity;
            Array.Copy(buffer, bufferWriteTotal + offset, _buffer, _bufferPosition, bufferWriteCount);

            _bufferPosition += (int)bufferWriteCount;
            bufferWriteTotal += bufferWriteCount;

            if (_bufferPosition == _bufferLength)
                Flush();
        }
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_closed)
            throw new ObjectDisposedException("stream already disposed");
        
        var bufferWriteTotal = 0L;

        while (bufferWriteTotal < count)
        {
            var bufferCapacity = _bufferLength - _bufferPosition;
            var bufferWriteLeft = count - bufferWriteTotal;
            var bufferWriteCount = bufferWriteLeft < bufferCapacity ? bufferWriteLeft : bufferCapacity;
            Array.Copy(buffer, bufferWriteTotal + offset, _buffer, _bufferPosition, bufferWriteCount);

            _bufferPosition += (int)bufferWriteCount;
            bufferWriteTotal += bufferWriteCount;

            if (_bufferPosition == _bufferLength)
                await FlushAsync(cancellationToken);
        }
    }

    public override void Flush()
    {
        if (_closed)
            return;
        var flushedBytes = _bufferPosition - _bufferReserved;
        var flushedBytesArray = BitConverter.GetBytes(flushedBytes);
        Array.Copy(flushedBytesArray, 0, _buffer, 1, sizeof(int));
        _buffer[0] = _closing ? (byte)1 : (byte)0;
        _networkStream.Write(_buffer, 0, _bufferPosition);
        _bufferPosition = _bufferReserved;
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (_closed)
            return;
        var flushedBytes = _bufferPosition - _bufferReserved;
        var flushedBytesArray = BitConverter.GetBytes(flushedBytes);
        Array.Copy(flushedBytesArray, 0, _buffer, 1, sizeof(int));
        _buffer[0] = _closing ? (byte)1 : (byte)0;
        await _networkStream.WriteAsync(_buffer, 0, _bufferPosition, cancellationToken);
        _bufferPosition = _bufferReserved;
    }

    public async ValueTask DisposeAsync()
    {
        _closing = true;
        await FlushAsync(_cancellationToken);
        _closed = true;
        _closing = false;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Read operation is not supported");
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("ReadAsync operation is not supported");        
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException("Seek operation is not supported");
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException("SetLength operation is not supported");
    }

    protected override void Dispose(bool disposing)
    {
        _closing = true;
        Flush();
        _closed = true;
        _closing = false;
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _bufferLength;
    public override long Position { get; set; } 
}