using RpcPipes.PipeData;

namespace RpcPipes.Models.PipeSerializers;

public class PipeSerializer : IPipeMessageWriter
{
    public virtual Task WriteData<T>(T message, Stream stream, CancellationToken cancellation)
        => System.Text.Json.JsonSerializer.SerializeAsync(stream, message, cancellationToken: cancellation);

    public virtual ValueTask<T> ReadData<T>(Stream stream, CancellationToken cancellation)
        => System.Text.Json.JsonSerializer.DeserializeAsync<T>(stream, cancellationToken: cancellation);

    public virtual Task WriteRequest<T>(PipeMessageRequest<T> message, Stream stream, CancellationToken cancellation)
        => System.Text.Json.JsonSerializer.SerializeAsync(stream, message, cancellationToken: cancellation);

    public virtual ValueTask<PipeMessageRequest<T>> ReadRequest<T>(Stream stream, CancellationToken cancellation)
        => System.Text.Json.JsonSerializer.DeserializeAsync<PipeMessageRequest<T>>(stream, cancellationToken: cancellation);

    public virtual Task WriteResponse<T>(PipeMessageResponse<T> message, Stream stream, CancellationToken cancellation)
        => System.Text.Json.JsonSerializer.SerializeAsync(stream, message, cancellationToken: cancellation);

    public virtual ValueTask<PipeMessageResponse<T>> ReadResponse<T>(Stream stream, CancellationToken cancellation)
        => System.Text.Json.JsonSerializer.DeserializeAsync<PipeMessageResponse<T>>(stream, cancellationToken: cancellation);
}