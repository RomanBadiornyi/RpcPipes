using System.Text.Json;
using RpcPipes.PipeData;
using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models.PipeSerializers;

public class PipeJsonSerializer : IPipeMessageWriter
{
    private static readonly JsonSerializerOptions _options = new()
    {
        DefaultBufferSize = 4096            
    };
        
    public virtual async Task WriteHeartbeat<T>(PipeMessageHeartbeat<T> message, Stream stream, CancellationToken cancellation)
        => await JsonSerializer.SerializeAsync(stream, message, options: _options, cancellationToken: cancellation);

    public virtual async ValueTask<PipeMessageHeartbeat<T>> ReadHeartbeat<T>(Stream stream, CancellationToken cancellation)
        => await JsonSerializer.DeserializeAsync<PipeMessageHeartbeat<T>>(stream, options: _options, cancellationToken: cancellation);

    public virtual async Task WriteRequest<T>(PipeMessageRequest<T> message, Stream stream, CancellationToken cancellation)
        => await JsonSerializer.SerializeAsync(stream, message, options: _options, cancellationToken: cancellation);

    public virtual async ValueTask<PipeMessageRequest<T>> ReadRequest<T>(Stream stream, CancellationToken cancellation)
        => await JsonSerializer.DeserializeAsync<PipeMessageRequest<T>>(stream, options: _options, cancellationToken: cancellation);

    public virtual async Task WriteResponse<T>(PipeMessageResponse<T> message, Stream stream, CancellationToken cancellation)
        => await JsonSerializer.SerializeAsync(stream, message, options: _options, cancellationToken: cancellation);

    public virtual async ValueTask<PipeMessageResponse<T>> ReadResponse<T>(Stream stream, CancellationToken cancellation)
        => await JsonSerializer.DeserializeAsync<PipeMessageResponse<T>>(stream, options: _options, cancellationToken: cancellation);
}
