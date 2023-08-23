using RpcPipes.Transport;

namespace RpcPipes.Models;

public class PipeSerializer : IPipeMessageSerializer
{
    public Task Serialize<T>(T message, Stream stream, CancellationToken token)
        => System.Text.Json.JsonSerializer.SerializeAsync(stream, message, cancellationToken: token);
    
    public ValueTask<T> Deserialize<T>(Stream stream, CancellationToken token)
        => System.Text.Json.JsonSerializer.DeserializeAsync<T>(stream, cancellationToken: token);
}