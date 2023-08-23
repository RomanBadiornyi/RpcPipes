namespace RpcPipes.Transport;

public class PipeChunkWriteStream : Stream, IAsyncDisposable
{
    private bool _closed;

    private readonly byte[] _buffer;
    private readonly int _bufferLength;

    private int _bufferPosition;

    private readonly Stream _networkStrem;

    public PipeChunkWriteStream(byte[] buffer, int bufferLength, Stream networkStream)
    {
        if (buffer.Length > 0)
            buffer[0] = 0;

        _buffer = buffer;
        _bufferLength = bufferLength;
        _bufferPosition = 1;

        _networkStrem = networkStream;
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
                Flush(false);
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
                await FlushAsync(false, cancellationToken);
        }
    }

    public override void Flush()
    {
        Flush(false);
    }

    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        await FlushAsync(false, cancellationToken);
    }

    public void Flush(bool isCompleted)
    {
        _buffer[0] = isCompleted ? (byte)1 : (byte)0;
        if (_bufferPosition > 0) 
        {
            _networkStrem.Write(_buffer, 0, _bufferPosition);
            _bufferPosition = isCompleted ? 0 : 1;
        }
    }

    public async Task FlushAsync(bool isCompleted, CancellationToken cancellationToken) 
    {
        _buffer[0] = isCompleted ? (byte)1 : (byte)0;
        if (_bufferPosition > 0) 
        {
            await _networkStrem.WriteAsync(_buffer, 0, _bufferPosition, cancellationToken);
            _bufferPosition = isCompleted ? 0 : 1;
        }
    }

    public async ValueTask DisposeAsync()
    {
        _closed = true;
        await FlushAsync(true, CancellationToken.None);
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
        _closed = true;
        Flush(true);
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _bufferLength;
    public override long Position { get; set; } 
}