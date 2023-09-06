using RpcPipes.PipeData;

namespace RpcPipes.Models.PipeSerializers;

public class PipeSerializer : IPipeMessageWriter
{
    public virtual Task WriteData<T>(T message, Stream stream, CancellationToken token)
        => System.Text.Json.JsonSerializer.SerializeAsync(stream, message, cancellationToken: token);

    public virtual ValueTask<T> ReadData<T>(Stream stream, CancellationToken token)
        => System.Text.Json.JsonSerializer.DeserializeAsync<T>(stream, cancellationToken: token);

    public virtual Task WriteRequest<T>(PipeMessageRequest<T> message, Stream stream, CancellationToken token)
        => System.Text.Json.JsonSerializer.SerializeAsync(stream, message, cancellationToken: token);

    public virtual ValueTask<PipeMessageRequest<T>> ReadRequest<T>(Stream stream, CancellationToken token)
        => System.Text.Json.JsonSerializer.DeserializeAsync<PipeMessageRequest<T>>(stream, cancellationToken: token);

    public virtual Task WriteResponse<T>(PipeMessageResponse<T> message, Stream stream, CancellationToken token)
        => System.Text.Json.JsonSerializer.SerializeAsync(stream, message, cancellationToken: token);

    public virtual ValueTask<PipeMessageResponse<T>> ReadResponse<T>(Stream stream, CancellationToken token)
        => System.Text.Json.JsonSerializer.DeserializeAsync<PipeMessageResponse<T>>(stream, cancellationToken: token);
}