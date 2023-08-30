namespace RpcPipes;

public interface IPipeMessageWriter
{
    PipeMessageResponse<T> CreateResponseContainer<T>();
    Task Serialize<T>(T message, Stream stream, CancellationToken token);
    ValueTask<T> Deserialize<T>(Stream stream, CancellationToken token);
}