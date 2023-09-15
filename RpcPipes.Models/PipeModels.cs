using RpcPipes.PipeHeartbeat;

namespace RpcPipes.Models;

public record PipeRequestMessage(string Message, double DelaySeconds);
public record PipeReplyMessage(string Reply);
public record PipeHeartbeatMessage(double Progress, string Stage) : IPipeHeartbeat;