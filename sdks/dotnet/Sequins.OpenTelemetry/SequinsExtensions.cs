using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry.Logs;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Sequins.OpenTelemetry;

/// <summary>
/// Extension methods for integrating Sequins with ASP.NET Core / generic host.
/// </summary>
public static class SequinsExtensions
{
    /// <summary>
    /// Adds Sequins OpenTelemetry instrumentation to the service collection.
    /// Configures traces, metrics, and logs to export to a local Sequins instance.
    /// </summary>
    /// <example>
    /// <code>
    /// builder.Services.AddSequins("my-app");
    /// </code>
    /// </example>
    public static IServiceCollection AddSequins(
        this IServiceCollection services,
        string serviceName,
        Action<SequinsOptions>? configure = null)
    {
        var options = new SequinsOptions { ServiceName = serviceName };
        configure?.Invoke(options);

        var resolvedServiceName = !string.IsNullOrEmpty(options.ServiceName)
            ? options.ServiceName!
            : Environment.GetEnvironmentVariable("OTEL_SERVICE_NAME") ?? "unknown_service";

        var envEndpoint = Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT");
        var endpoint = string.IsNullOrEmpty(envEndpoint) ? options.Endpoint : envEndpoint;
        var endpointUri = new Uri(endpoint);

        var resource = ResourceBuilder.CreateDefault().AddService(resolvedServiceName);

        services.AddOpenTelemetry()
            .WithTracing(builder => builder
                .SetResourceBuilder(resource)
                .AddAspNetCoreInstrumentation()
                .AddHttpClientInstrumentation()
                .AddOtlpExporter(o => o.Endpoint = endpointUri))
            .WithMetrics(builder => builder
                .SetResourceBuilder(resource)
                .AddAspNetCoreInstrumentation()
                .AddHttpClientInstrumentation()
                .AddRuntimeInstrumentation()
                .AddOtlpExporter(o => o.Endpoint = endpointUri));

        services.AddLogging(logging => logging
            .AddOpenTelemetry(o =>
            {
                o.SetResourceBuilder(resource);
                o.AddOtlpExporter(otlp => otlp.Endpoint = endpointUri);
            }));

        return services;
    }
}
