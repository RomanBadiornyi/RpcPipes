namespace RpcPipes.PipeData;

public class PipeMessageHeader
{
    public Guid MessageId { get; set; }
}

public class PipeAsyncMessageHeader : PipeMessageHeader
{
    public string ReplyPipe { get; set; }
}