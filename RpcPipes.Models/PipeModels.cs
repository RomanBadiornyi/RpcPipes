using RpcPipes.Transport;

namespace RpcPipes.Models;

public record RequestMessage(string Message, int DelatySeconds);
public record ReplyMessage(string Reply);
public record ProgressMessage(double Progress, string Stage) : IPipeProgress;