using System.Text;

namespace RpcPipes;

public class PipeChunkReadStream : Stream, IAsyncDisposable
{
    private const int _bufferReserved = sizeof(int) + 1;
    private bool _closed;

    private readonly byte[] _buffer;
    private readonly int _bufferLength;

    private int _bufferLengthCurrent;
    private int _bufferPosition;

    private readonly Stream _networkStream;
    private readonly CancellationToken _cancellationToken;

    public PipeChunkReadStream(byte[] buffer, int bufferLength, Stream networkStream, CancellationToken cancellationToken)
    {
        if (buffer.Length > 0)
            buffer[0] = 0;

        _buffer = buffer;
        _bufferLength = bufferLength;
        _bufferLengthCurrent = bufferLength;
        _bufferPosition = _bufferReserved;

        _networkStream = networkStream;
        _cancellationToken = cancellationToken;
    }

    public async Task<bool> TryReadGuid(Action<Guid> onRead, CancellationToken token)
    {
        var buffer = new byte[16];
        var readCount = await ReadAsync(buffer, 0, buffer.Length, token);
        if (readCount != buffer.Length)
            return false;
        onRead.Invoke(new Guid(buffer));
        return true;
    }

    public async Task<bool> TryReadBoolean(Action<bool> onRead, CancellationToken token)
    {
        var buffer = new byte[1];
        var readCount = await ReadAsync(buffer, 0, sizeof(bool), token);
        if (readCount != buffer.Length)
            return false;
        onRead.Invoke(BitConverter.ToBoolean(buffer, 0));
        return true;
    }

    public async Task<bool> TryReadInteger32(Action<int> onRead, CancellationToken token)
    {
        var buffer = new byte[sizeof(int)];
        var readCount = await ReadAsync(buffer, 0, sizeof(int), token);
        if (readCount != buffer.Length)
            return false;
        onRead.Invoke(BitConverter.ToInt32(buffer, 0));
        return true;
    }

    public async Task<bool> TryReadString(Action<string> onRead, CancellationToken token)
    {
        var buffer = new byte[4];
        var readCount = await ReadAsync(buffer, 0, 4, token);
        if (readCount < 4)
            return false;
        var stringLength = BitConverter.ToInt32(buffer, 0);
        var stringBuffer = new byte[stringLength];
        readCount = await ReadAsync(stringBuffer, 0, stringBuffer.Length, token);
        if (readCount != stringBuffer.Length)
            return false;
        onRead.Invoke(Encoding.UTF8.GetString(stringBuffer));
        return true;
    }

    public async Task<bool> ReadTransaction(IEnumerable<Func<PipeChunkReadStream, Task<bool>>> reads)
    {
        var allResult = true;
        foreach (var readOperation in reads)
        {
            var result = await readOperation.Invoke(this);
            if (!result)
            {
                allResult = false;
                break;
            }
        }
        return allResult;
    }    

    public override int Read(byte[] buffer, int offset, int count)
    {
        var bufferReadTotal = 0L;
        var bufferClientPosition = offset;

        while (bufferReadTotal < count && (_bufferLengthCurrent - _bufferPosition > 0 || !_closed))
        {
            if (_bufferPosition == _bufferLengthCurrent || _bufferPosition == _bufferReserved)
                FillBuffer();

            var bufferReadCount = Math.Min(count - bufferClientPosition, _bufferLengthCurrent - _bufferPosition);
            if (bufferReadCount > 0)
                Array.Copy(_buffer, _bufferPosition, buffer, bufferClientPosition, bufferReadCount);

            _bufferPosition += bufferReadCount;
            bufferClientPosition += bufferReadCount;
            bufferReadTotal += bufferReadCount;
        }
        return (int)bufferReadTotal;
    }

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        var bufferReadTotal = 0L;
        var bufferClientPosition = offset;

        while (bufferReadTotal < count && (_bufferLengthCurrent - _bufferPosition > 0 || !_closed))
        {
            if (_bufferPosition == _bufferLengthCurrent || _bufferPosition == _bufferReserved)
                await FillBufferAsync(cancellationToken);

            var bufferReadCount = Math.Min(count - bufferClientPosition, _bufferLengthCurrent - _bufferPosition);
            if (bufferReadCount > 0)
                Array.Copy(_buffer, _bufferPosition, buffer, bufferClientPosition, bufferReadCount);

            _bufferPosition += bufferReadCount;
            bufferClientPosition += bufferReadCount;
            bufferReadTotal += bufferReadCount;
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

    public async ValueTask DisposeAsync()
    {
        while (!_closed)
            await FillBufferAsync(_cancellationToken);
    }

    protected override void Dispose(bool disposing)
    {
        while (!_closed)
            FillBuffer();
    }

    private void FillBuffer()
    {
        if (_closed)
            return;
        _bufferPosition = _bufferReserved;
        _bufferLengthCurrent = _bufferReserved;
        var readCount = _networkStream.Read(_buffer, 0, _bufferReserved);
        if (readCount != _bufferReserved)
            _closed = true;
        if (_closed)
            return;
        _closed = _buffer[0] == 1;
        var readLength = BitConverter.ToInt32(_buffer, 1);

        readCount = _networkStream.Read(_buffer, _bufferReserved, readLength);
        _bufferLengthCurrent += readCount;
        if (readCount != readLength)
            _closed = true;
    }

    private async Task FillBufferAsync(CancellationToken cancellationToken)
    {
        if (_closed)
            return;
        _bufferPosition = _bufferReserved;
        _bufferLengthCurrent = _bufferReserved;
        var readCount = await _networkStream.ReadAsync(_buffer, 0, _bufferReserved, cancellationToken);
        if (readCount != _bufferReserved)
            _closed = true;
        if (_closed)
            return;
        _closed = _buffer[0] == 1;
        var readLength = BitConverter.ToInt32(_buffer, 1);

        readCount = await _networkStream.ReadAsync(_buffer, _bufferReserved, readLength, cancellationToken);
        _bufferLengthCurrent += readCount;
        if (readCount != readLength)
            _closed = true;
    }

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => _bufferLength;
    public override long Position { get; set; }
}