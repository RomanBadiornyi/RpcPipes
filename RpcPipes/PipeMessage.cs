namespace RpcPipes.Transport;

public class RequestException
{
    public string ClassName { get; set; }
    public string Message { get; set; }
    public RequestException InnerException { get; set; }
    public List<string> StackTrace { get; set; }

    public Exception ToException()
      => new ServiceException(this);

    public static RequestException CreateRequestException(Exception ex)
    {
        var environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        var isDevelopment = environment?.ToLower() == "development";

        return new RequestException
        {
            ClassName = !isDevelopment ? "Exception" : ex.GetType().Name,
            Message = !isDevelopment ? "Internal Server Error" : ex.Message,
            StackTrace = !isDevelopment && ex.StackTrace != null ? ex.StackTrace.Split(Environment.NewLine.ToCharArray()).ToList() : new List<string>(),
            InnerException = ex.InnerException != null ? CreateRequestException(ex.InnerException) : null            
        };
    }
}

public class ServiceException : Exception
{
    public string ClassName { get; }
    public override string StackTrace { get; }

    public ServiceException(RequestException exception) : base(exception.Message, exception.InnerException != null ? new ServiceException(exception.InnerException) : null)
    {
        ClassName = exception.ClassName;
        StackTrace = string.Join(Environment.NewLine, exception.StackTrace);
    }
}

public class PipeMessageResponse<T>
{
    public T Reply { get; set; }
    public RequestException Exception { get; set; }
}