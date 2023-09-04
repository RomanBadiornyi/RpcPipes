namespace RpcPipes.Models;

public record RequestMessage(string Message, double DelaySeconds);
public record ReplyMessage(string Reply);
public record HeartbeatMessage(double Progress, string Stage) : IPipeHeartbeat;