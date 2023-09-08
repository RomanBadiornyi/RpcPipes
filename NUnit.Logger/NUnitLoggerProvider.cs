using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace NUnit.Logger;

public sealed class NUnitLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentDictionary<string, NUnitLogger> _loggers =
        new(StringComparer.OrdinalIgnoreCase);

    public NUnitLoggerProvider()
    {
    }

    public ILogger CreateLogger(string categoryName) =>
        _loggers.GetOrAdd(categoryName, name => new NUnitLogger(name));

    public void Dispose()
    {
        _loggers.Clear();
    }
}