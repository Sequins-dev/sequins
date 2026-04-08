using OpenTelemetry;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Sequins.OpenTelemetry;

/// <summary>
/// Builds and configures the Sequins OpenTelemetry SDK for use in non-DI applications.
/// </summary>
/// <example>
/// <code>
/// using var sequins = new SequinsBuilder("my-app").Build();
/// var tracer = sequins.TracerProvider.GetTracer("my-module");
/// </code>
/// </example>
public sealed class SequinsBuilder
{
    private readonly SequinsOptions _options;

    /// <summary>Initializes a new builder with the given service name.</summary>
    public SequinsBuilder(string serviceName) : this(new SequinsOptions { ServiceName = serviceName }) { }

    /// <summary>Initializes a new builder with the given options.</summary>
    public SequinsBuilder(SequinsOptions options) => _options = options;

    /// <summary>Builds and starts the Sequins SDK, returning a handle to all providers.</summary>
    public SequinsHandle Build()
    {
        var serviceName = ResolveServiceName();
        var endpoint = ResolveEndpoint();
        var resource = ResourceBuilder.CreateDefault()
            .AddService(serviceName);

        var tracerProvider = Sdk.CreateTracerProviderBuilder()
            .SetResourceBuilder(resource)
            .AddHttpClientInstrumentation()
            .AddOtlpExporter(o => o.Endpoint = new Uri(endpoint))
            .Build()!;

        var meterProvider = Sdk.CreateMeterProviderBuilder()
            .SetResourceBuilder(resource)
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddOtlpExporter(o => o.Endpoint = new Uri(endpoint))
            .Build()!;

        var loggerProvider = Sdk.CreateLoggerProviderBuilder()
            .SetResourceBuilder(resource)
            .AddOtlpExporter(o => o.Endpoint = new Uri(endpoint))
            .Build();

        return new SequinsHandle(tracerProvider, meterProvider, loggerProvider);
    }

    private string ResolveServiceName()
    {
        if (!string.IsNullOrEmpty(_options.ServiceName)) return _options.ServiceName!;
        var envName = Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME");
        return string.IsNullOrEmpty(envName) ? "unknown_service" : envName;
    }

    private string ResolveEndpoint()
    {
        var envEndpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");
        return string.IsNullOrEmpty(envEndpoint) ? _options.Endpoint : envEndpoint;
    }
}
