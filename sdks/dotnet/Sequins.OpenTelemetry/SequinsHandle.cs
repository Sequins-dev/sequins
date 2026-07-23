using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

namespace Sequins.OpenTelemetry;

/// <summary>
/// Handle returned by <see cref="SequinsExtensions.AddSequins"/> or <see cref="SequinsBuilder.Build"/>.
/// Provides access to all configured OpenTelemetry providers.
/// </summary>
public sealed class SequinsHandle : IDisposable
{
    /// <summary>Gets the configured TracerProvider.</summary>
    public TracerProvider TracerProvider { get; }

    /// <summary>Gets the configured MeterProvider.</summary>
    public MeterProvider MeterProvider { get; }

    /// <summary>Gets the configured LoggerProvider.</summary>
    public LoggerProvider LoggerProvider { get; }

    internal SequinsHandle(TracerProvider tracerProvider, MeterProvider meterProvider, LoggerProvider loggerProvider)
    {
        TracerProvider = tracerProvider;
        MeterProvider = meterProvider;
        LoggerProvider = loggerProvider;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        TracerProvider.Dispose();
        MeterProvider.Dispose();
        LoggerProvider.Dispose();
    }
}
