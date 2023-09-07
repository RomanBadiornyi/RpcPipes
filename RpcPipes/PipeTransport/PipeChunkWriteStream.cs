using System.Text;

namespace RpcPipes.PipeTransport;

public class PipeChunkWriteStream : Stream, IAsyncDisposable
{
    private const int _bufferReserved = sizeof(int) + 1;
    private bool _closed;
    private bool _closing;

    private readonly byte[] _buffer;
    private readonly int _bufferLength;

    private long _bufferPosition;

    private readonly Stream _networkStream;
    private readonly CancellationToken _cancellation;

    public PipeChunkWriteStream(byte[] buffer, int bufferLength, Stream networkStream, CancellationToken cancellation)
    {
        if (buffer.Length > 0)
            buffer[0] = 0;

        _buffer = buffer;
        _bufferLength = bufferLength;
        _bufferPosition = _bufferReserved;

        _networkStream = networkStream;
        _cancellation = cancellation;
    }

    public Task WriteGuid(Guid id, CancellationToken cancellation)
        => WriteAsync(id.ToByteArray(), 0, 16, cancellation);

    public Task WriteBoolean(bool val, CancellationToken cancellation)
        => WriteAsync(BitConverter.GetBytes(val), 0, sizeof(bool), cancellation);

    public Task WriteInteger32(int val, CancellationToken cancellation)
        => WriteAsync(BitConverter.GetBytes(val), 0, sizeof(int), cancellation);

    public async Task WriteString(string message, CancellationToken cancellation)
    {
        var buffer = Encoding.UTF8.GetBytes(message);
        await WriteAsync(BitConverter.GetBytes(buffer.Length), 0, 4, cancellation);
        await WriteAsync(buffer, 0, buffer.Length, cancellation);
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

            _bufferPosition += bufferWriteCount;
            Position += bufferWriteCount;
            bufferWriteTotal += bufferWriteCount;

            if (_bufferPosition == _bufferLength)
                Flush();
        }
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellation)
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

            _bufferPosition += bufferWriteCount;
            Position += bufferWriteCount;
            bufferWriteTotal += bufferWriteCount;            

            if (_bufferPosition == _bufferLength)
                await FlushAsync(cancellation);
        }
    }

    public override void Flush()
    {
        if (_closed)
            return;
        var flushedBytes = _bufferPosition - _bufferReserved;
        SetFlushedLength(_buffer, _closing, (int)flushedBytes);
        _networkStream.Write(_buffer, 0, (int)_bufferPosition);
        _bufferPosition = _bufferReserved;
    }

    public override async Task FlushAsync(CancellationToken cancellation)
    {
        if (_closed)
            return;
        var flushedBytes = _bufferPosition - _bufferReserved;
        SetFlushedLength(_buffer, _closing, (int)flushedBytes);
        await _networkStream.WriteAsync(_buffer, 0, (int)_bufferPosition, cancellation);
        _bufferPosition = _bufferReserved;
    }

    private void SetFlushedLength(byte[] buffer, bool closing, int flushedBytes)
    {
        //indicate if this is last buffer or not, so reader know if it should stop
        buffer[0] = closing ? (byte)1 : (byte)0;
        //specify what buffer length was written, so reader know how much bytes to read
        buffer[1] = (byte)flushedBytes;
        buffer[2] = (byte)(flushedBytes >> 8);
        buffer[3] = (byte)(flushedBytes >> 16);
        buffer[4] = (byte)(flushedBytes >> 24);
    }

    public async ValueTask DisposeAsync()
    {
        _closing = true;
        await FlushAsync(_cancellation);
        _closed = true;
        _closing = false;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Read operation is not supported");
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellation)
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
    public override long Length => throw new NotSupportedException("Length operation is not supported");
    public override long Position { get; set; } 
}