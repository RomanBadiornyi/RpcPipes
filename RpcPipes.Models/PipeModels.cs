using MessagePack;
using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models;

[MessagePackObject(true)]
public record PipeRequestMessage(string Message, double DelaySeconds);
[MessagePackObject(true)]
public record PipeReplyMessage(string Reply);
[MessagePackObject(true)]
public record PipeHeartbeatMessage(double Progress, string Stage) : IPipeHeartbeat;