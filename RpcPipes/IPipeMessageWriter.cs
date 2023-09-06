using RpcPipes.PipeData;

namespace RpcPipes;

public interface IPipeMessageWriter
{
    Task WriteData<T>(T message, Stream stream, CancellationToken token);
    ValueTask<T> ReadData<T>(Stream stream, CancellationToken token);

    Task WriteRequest<T>(PipeMessageRequest<T> message, Stream stream, CancellationToken token);
    ValueTask<PipeMessageRequest<T>> ReadRequest<T>(Stream stream, CancellationToken token);

    Task WriteResponse<T>(PipeMessageResponse<T> message, Stream stream, CancellationToken token);
    ValueTask<PipeMessageResponse<T>> ReadResponse<T>(Stream stream, CancellationToken token);
}