using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace NUnit.Logger;

public static class NUnitLoggerExtensions
{
    public static ILoggingBuilder AddNUnitLogger(
        this ILoggingBuilder builder)
    {
        builder.Services.TryAddEnumerable(
            ServiceDescriptor.Singleton<ILoggerProvider, NUnitLoggerProvider>());

        return builder;
    }
}