namespace RpcPipes;

public class PipeMessageResponse<T>
{
    public T Reply { get; set; }
    public PipeMessageException ReplyError { get; set; }

    public virtual void SetRequestException(Exception ex)
    {
        ReplyError = CreateRequestException(ex);
    }

    private PipeMessageException CreateRequestException(Exception ex)
    {
        var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        var isDevelopment = environment == null || environment?.ToLower() == "development";

        return new PipeMessageException
        {
            ClassName = !isDevelopment ? 
                "Exception" : ex.GetType().Name,
            Message = !isDevelopment ? 
                "Internal Server Error" : ex.Message,
            StackTrace = !isDevelopment || ex.StackTrace == null ? 
                new List<string>() : ex.StackTrace.Split(Environment.NewLine.ToCharArray()).ToList(),
            InnerException = ex.InnerException != null ? CreateRequestException(ex.InnerException) : null            
        };      
    }
}