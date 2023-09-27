using System.Text.Json;
using RpcPipes.PipeData;

namespace RpcPipes.Models.PipeSerializers;

public class PipeJsonSerializer : IPipeMessageWriter
{
    private static readonly JsonSerializerOptions _options = new()
    {
        DefaultBufferSize = 4096            
    };
        
    public virtual async Task WriteData<T>(T message, Stream stream, CancellationToken cancellation)
        => await JsonSerializer.SerializeAsync(stream, message, options: _options, cancellationToken: cancellation);

    public virtual async ValueTask<T> ReadData<T>(Stream stream, CancellationToken cancellation)
        => await JsonSerializer.DeserializeAsync<T>(stream, options: _options, cancellationToken: cancellation);

    public virtual async Task WriteRequest<T>(PipeMessageRequest<T> message, Stream stream, CancellationToken cancellation)
        => await JsonSerializer.SerializeAsync(stream, message, options: _options, cancellationToken: cancellation);

    public virtual async ValueTask<PipeMessageRequest<T>> ReadRequest<T>(Stream stream, CancellationToken cancellation)
        => await JsonSerializer.DeserializeAsync<PipeMessageRequest<T>>(stream, options: _options, cancellationToken: cancellation);

    public virtual async Task WriteResponse<T>(PipeMessageResponse<T> message, Stream stream, CancellationToken cancellation)
        => await JsonSerializer.SerializeAsync(stream, message, options: _options, cancellationToken: cancellation);

    public virtual async ValueTask<PipeMessageResponse<T>> ReadResponse<T>(Stream stream, CancellationToken cancellation)
        => await JsonSerializer.DeserializeAsync<PipeMessageResponse<T>>(stream, options: _options, cancellationToken: cancellation);
}
