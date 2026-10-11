namespace Sequins.OpenTelemetry;

/// <summary>
/// Configuration options for the Sequins OpenTelemetry SDK.
/// </summary>
public sealed class SequinsOptions
{
    /// <summary>
    /// The name of your service. Defaults to <c>OTEL_SERVICE_NAME</c> env var, or <c>"unknown_service"</c>.
    /// </summary>
    public string? ServiceName { get; set; }

    /// <summary>
    /// The OTLP gRPC endpoint. Defaults to <c>OTEL_EXPORTER_OTLP_ENDPOINT</c> env var, or <c>"http://localhost:4317"</c>.
    /// </summary>
    public string Endpoint { get; set; } = "http://localhost:4317";

    /// <summary>
    /// How often to export metrics. Default: 10 seconds.
    /// </summary>
    public TimeSpan MetricExportInterval { get; set; } = TimeSpan.FromSeconds(10);
}
