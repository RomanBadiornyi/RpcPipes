using MessagePack;
using RpcPipes.PipeData;

namespace RpcPipes.Models.PipeSerializers;

public class PipeMessagePackSerializer : IPipeMessageWriter
{
    private static readonly MessagePackSerializerOptions _options = MessagePackSerializerOptions.Standard
        .WithCompression(MessagePackCompression.None);

    public async Task WriteData<T>(T message, Stream stream, CancellationToken cancellation)
        => await MessagePackSerializer.SerializeAsync(stream, message, _options, cancellation);

    public async ValueTask<T> ReadData<T>(Stream stream, CancellationToken cancellation)
        => await MessagePackSerializer.DeserializeAsync<T>(stream, _options, cancellation);

    public async Task WriteRequest<T>(PipeMessageRequest<T> message, Stream stream, CancellationToken cancellation)
        => await MessagePackSerializer.SerializeAsync(stream, message.FromRequest(), _options, cancellation);

    public async ValueTask<PipeMessageRequest<T>> ReadRequest<T>(Stream stream, CancellationToken cancellation)
        => (await MessagePackSerializer.DeserializeAsync<MpPipeMessageRequest<T>>(stream, _options, cancellation)).ToRequest();

    public async Task WriteResponse<T>(PipeMessageResponse<T> message, Stream stream, CancellationToken cancellation)
        => await MessagePackSerializer.SerializeAsync(stream, message.FromResponse(), _options, cancellation);

    public async ValueTask<PipeMessageResponse<T>> ReadResponse<T>(Stream stream, CancellationToken cancellation)
        => (await MessagePackSerializer.DeserializeAsync<MpPipeMessageResponse<T>>(stream, _options, cancellation)).ToResponse();    

    [MessagePackObject(true)]
    public class MpPipeMessageRequest<T>
    {
        public T Request { get; set; }
        public TimeSpan Heartbeat { get; set; }
        public TimeSpan Deadline { get; set; } 
    }

    [MessagePackObject(true)]
    public class MpPipeMessageResponse<T>
    {
        public T Reply { get; set; }
        public MpPipeMessageException ReplyError {get; set; }         
    }

    [MessagePackObject(true)]
    public class MpPipeMessageException
    {
        public string ClassName { get; set; }
        public string Message { get; set; }
        public MpPipeMessageException InnerException { get; set; }
        public List<string> StackTrace { get; set; }
   }
}

internal static class PipeModelConverters
{
    public static PipeMessagePackSerializer.MpPipeMessageRequest<T> FromRequest<T>(this PipeMessageRequest<T> r)
        => r != null 
        ? new() 
            { 
                Request = r.Request, 
                Heartbeat = r.Heartbeat, 
                Deadline = r.Deadline 
            }
        : null;

    public static PipeMessageRequest<T> ToRequest<T>(this PipeMessagePackSerializer.MpPipeMessageRequest<T> r)
        => r != null 
        ? new() 
            { 
                Request = r.Request, 
                Heartbeat = r.Heartbeat, 
                Deadline = r.Deadline 
            }
        : null;

    public static PipeMessagePackSerializer.MpPipeMessageResponse<T> FromResponse<T>(this PipeMessageResponse<T> r)
        => r != null 
        ? new()
            { 
                Reply = r.Reply, 
                ReplyError = r.ReplyError.FromError()
            }
        : null; 
    
    public static PipeMessageResponse<T> ToResponse<T>(this PipeMessagePackSerializer.MpPipeMessageResponse<T> r)
        => r != null 
        ? new() 
            { 
                Reply = r.Reply, 
                ReplyError = r.ReplyError.ToError() 
            }
        : null;

    public static PipeMessagePackSerializer.MpPipeMessageException FromError(this PipeMessageException e)
        => e != null 
            ? new() 
            { 
                ClassName = e.ClassName, 
                Message = e.Message, 
                StackTrace = e.StackTrace, 
                InnerException = e.InnerException.FromError()
            }
            : null; 

    public static PipeMessageException ToError(this PipeMessagePackSerializer.MpPipeMessageException e)
        => e != null 
        ? new() 
            { 
                ClassName = e.ClassName, 
                Message = e.Message, 
                StackTrace = e.StackTrace, 
                InnerException = e.InnerException.ToError() 
            }
        : null;

}