using RpcPipes.PipeData;

namespace RpcPipes;

public interface IPipeMessageWriter
{
    Task WriteData<T>(T message, Stream stream, CancellationToken cancellation);
    ValueTask<T> ReadData<T>(Stream stream, CancellationToken cancellation);

    Task WriteRequest<T>(PipeMessageRequest<T> message, Stream stream, CancellationToken cancellation);
    ValueTask<PipeMessageRequest<T>> ReadRequest<T>(Stream stream, CancellationToken cancellation);

    Task WriteResponse<T>(PipeMessageResponse<T> message, Stream stream, CancellationToken cancellation);
    ValueTask<PipeMessageResponse<T>> ReadResponse<T>(Stream stream, CancellationToken cancellation);
}