using RpcPipes.PipeData;
using RpcPipes.PipeHeartbeat;

namespace RpcPipes;

public interface IPipeMessageWriter
{
    Task WriteHeartbeat<T>(PipeMessageHeartbeat<T> message, Stream stream, CancellationToken cancellation);
    ValueTask<PipeMessageHeartbeat<T>> ReadHeartbeat<T>(Stream stream, CancellationToken cancellation);

    Task WriteRequest<T>(PipeMessageRequest<T> message, Stream stream, CancellationToken cancellation);
    ValueTask<PipeMessageRequest<T>> ReadRequest<T>(Stream stream, CancellationToken cancellation);

    Task WriteResponse<T>(PipeMessageResponse<T> message, Stream stream, CancellationToken cancellation);
    ValueTask<PipeMessageResponse<T>> ReadResponse<T>(Stream stream, CancellationToken cancellation);
}