using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace NUnit.Logger;

public sealed class NUnitLogger : ILogger
{
    private readonly string _name;
    public NUnitLogger(string name) =>
        _name = name;

    public IDisposable BeginScope<TState>(TState state) where TState : notnull => default;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception exception,
        Func<TState, Exception, string> formatter)
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }
        
        var message = $"{formatter(state, null)}";
        if (message == null && exception != null)
            message = exception.Message;
        if (message == null)
            message = "";

        var level = logLevel.ToString().ToLower();
        TestContext.Out.WriteLine($"[{_name}]: {level} - {message}");
        if (exception != null)
            TestContext.Out.WriteLine(exception.ToString());        
    }
}