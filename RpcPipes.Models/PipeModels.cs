namespace RpcPipes.Models;

public record RequestMessage(string Message, double DelaySeconds);
public record ReplyMessage(string Reply);
public record ProgressMessage(double Progress, string Stage) : IPipeProgress;

public class MessageException : PipeMessageException
{
}

public class MessageResponse<T> : PipeMessageResponse<T>
{
}