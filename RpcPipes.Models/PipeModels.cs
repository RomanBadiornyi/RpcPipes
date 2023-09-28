using MessagePack;

namespace RpcPipes.Models;

[MessagePackObject(true)]
public record PipeRequest(string Message, double DelaySeconds);

[MessagePackObject(true)]
public record PipeReply(string Reply);

[MessagePackObject(true)]
public record PipeState(string Stage, string Message);