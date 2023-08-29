namespace RpcPipes.Models.PipeSerializers;

public class PipeSerializer : IPipeMessageSerializer
{
    public virtual Task Serialize<T>(T message, Stream stream, CancellationToken token)
        => System.Text.Json.JsonSerializer.SerializeAsync(stream, message, cancellationToken: token);
    
    public virtual ValueTask<T> Deserialize<T>(Stream stream, CancellationToken token)
        => System.Text.Json.JsonSerializer.DeserializeAsync<T>(stream, cancellationToken: token);
}