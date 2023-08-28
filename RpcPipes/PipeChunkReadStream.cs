namespace RpcPipes;

public class PipeChunkReadStream : Stream
{
    private readonly byte[] _buffer;
    private readonly int _bufferLength;

    private int _bufferLengthCurrent;
    private int _bufferPosition;

    private readonly Stream _networkStream;

    public PipeChunkReadStream(byte[] buffer, int bufferLength, Stream networkStream)
    {
        if (buffer.Length > 0)
            buffer[0] = 0;

        _buffer = buffer;
        _bufferLength = bufferLength;
        _bufferLengthCurrent = bufferLength;
        _bufferPosition = 1;

        _networkStream = networkStream;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        var bufferReadTotal = 0L;
        var bufferClientPosition = offset;
        var bufferReadCompleted = false;

        while (bufferReadTotal < count && !bufferReadCompleted)
        {
            if (_bufferPosition == _bufferLengthCurrent || _bufferPosition == 1)
                FillBuffer();

            var bufferReadCount = Math.Min(count - bufferClientPosition, _bufferLengthCurrent - _bufferPosition);
            if (bufferReadCount > 0) 
                Array.Copy(_buffer, _bufferPosition, buffer, bufferClientPosition, bufferReadCount);
            
            _bufferPosition += bufferReadCount;
            bufferClientPosition += bufferReadCount;
            bufferReadTotal += bufferReadCount;

            bufferReadCompleted = _buffer[0] == 1;
        }
        return (int)bufferReadTotal;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) 
    {
        var bufferReadTotal = 0L;
        var bufferClientPosition = offset;
        var bufferReadCompleted = false;

        while (bufferReadTotal < count && !bufferReadCompleted)
        {
            if (_bufferPosition == _bufferLengthCurrent || _bufferPosition == 1)
                await FillBufferAsync(cancellationToken);

            var bufferReadCount = Math.Min(count - bufferClientPosition, _bufferLengthCurrent - _bufferPosition);
            if (bufferReadCount > 0) 
                Array.Copy(_buffer, _bufferPosition, buffer, bufferClientPosition, bufferReadCount);
            
            _bufferPosition += bufferReadCount;
            bufferClientPosition += bufferReadCount;
            bufferReadTotal += bufferReadCount;

            bufferReadCompleted = _buffer[0] == 1;
        }
        return (int)bufferReadTotal;
    }

    public override void Flush()
    {        
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException("Write operation is not supported");
    } 

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        throw new NotSupportedException("WriteAsync operation is not supported");
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException("Seek operation is not supported");
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException("SetLength operation is not supported");
    }
    
    private void FillBuffer()
    {
        _bufferLengthCurrent = _buffer[0] == 1 ? 1 : _networkStream.Read(_buffer, 0, _bufferLength);
        _bufferPosition = 1;
    }

    public async Task FillBufferAsync(CancellationToken cancellationToken)
    {
        _bufferLengthCurrent = _buffer[0] == 1 ? 1 : await _networkStream.ReadAsync(_buffer, 0, _bufferLength, cancellationToken);
        _bufferPosition = 1;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _bufferLength;
    public override long Position { get; set; }
}