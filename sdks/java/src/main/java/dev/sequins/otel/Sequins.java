package dev.sequins.otel;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.api.logs.LoggerProvider;
import io.opentelemetry.exporter.otlp.logs.OtlpGrpcLogRecordExporter;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ServiceAttributes;

/**
 * Sequins OpenTelemetry SDK — zero-config observability for local development.
 *
 * <p>Configures all OTel signals (traces, metrics, logs) to export to a local
 * Sequins instance via OTLP/gRPC. Registers providers globally so
 * auto-instrumentation libraries work without additional wiring.
 *
 * <pre>{@code
 * Sequins sequins = Sequins.init("my-app");
 *
 * Tracer tracer = sequins.getTracerProvider().tracerBuilder("my-module").build();
 * Meter meter = sequins.getMeterProvider().meterBuilder("my-module").build();
 *
 * // On application exit:
 * sequins.close();
 * }</pre>
 */
public final class Sequins implements AutoCloseable {

    private final OpenTelemetrySdk sdk;
    private final SdkTracerProvider sdkTracerProvider;
    private final SdkMeterProvider sdkMeterProvider;
    private final SdkLoggerProvider sdkLoggerProvider;

    private Sequins(
            OpenTelemetrySdk sdk,
            SdkTracerProvider sdkTracerProvider,
            SdkMeterProvider sdkMeterProvider,
            SdkLoggerProvider sdkLoggerProvider) {
        this.sdk = sdk;
        this.sdkTracerProvider = sdkTracerProvider;
        this.sdkMeterProvider = sdkMeterProvider;
        this.sdkLoggerProvider = sdkLoggerProvider;
    }

    /**
     * Initializes OpenTelemetry with all signals pre-configured for Sequins.
     *
     * @param serviceName the name of your service
     * @return a {@link Sequins} handle with references to all configured providers
     */
    public static Sequins init(String serviceName) {
        return init(SequinsConfig.builder().serviceName(serviceName).build());
    }

    /**
     * Initializes OpenTelemetry with all signals pre-configured for Sequins.
     *
     * @param config full configuration; see {@link SequinsConfig#builder()}
     * @return a {@link Sequins} handle with references to all configured providers
     */
    public static Sequins init(SequinsConfig config) {
        String resolvedServiceName = config.getServiceName();
        if (resolvedServiceName == null || resolvedServiceName.isEmpty()) {
            resolvedServiceName = System.getenv("OTEL_SERVICE_NAME");
        }
        if (resolvedServiceName == null || resolvedServiceName.isEmpty()) {
            resolvedServiceName = "unknown_service";
        }

        String endpoint = config.getEndpoint();
        String envEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
        if (envEndpoint != null && !envEndpoint.isEmpty()) {
            endpoint = envEndpoint;
        }

        Resource resource = Resource.getDefault().merge(
                Resource.create(io.opentelemetry.api.common.Attributes.of(
                        ServiceAttributes.SERVICE_NAME, resolvedServiceName
                ))
        );

        // --- Traces ---
        OtlpGrpcSpanExporter traceExporter = OtlpGrpcSpanExporter.builder()
                .setEndpoint(endpoint)
                .build();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(traceExporter).build())
                .setResource(resource)
                .build();

        // --- Metrics ---
        OtlpGrpcMetricExporter metricExporter = OtlpGrpcMetricExporter.builder()
                .setEndpoint(endpoint)
                .build();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(
                        PeriodicMetricReader.builder(metricExporter)
                                .setInterval(config.getMetricExportInterval())
                                .build()
                )
                .setResource(resource)
                .build();

        // --- Logs ---
        OtlpGrpcLogRecordExporter logExporter = OtlpGrpcLogRecordExporter.builder()
                .setEndpoint(endpoint)
                .build();
        SdkLoggerProvider loggerProvider = SdkLoggerProvider.builder()
                .addLogRecordProcessor(BatchLogRecordProcessor.builder(logExporter).build())
                .setResource(resource)
                .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setMeterProvider(meterProvider)
                .setLoggerProvider(loggerProvider)
                .buildAndRegisterGlobal();

        return new Sequins(sdk, tracerProvider, meterProvider, loggerProvider);
    }

    /** Returns the configured {@link TracerProvider}. */
    public TracerProvider getTracerProvider() { return sdkTracerProvider; }

    /** Returns the configured {@link MeterProvider}. */
    public MeterProvider getMeterProvider() { return sdkMeterProvider; }

    /** Returns the configured {@link LoggerProvider}. */
    public LoggerProvider getLoggerProvider() { return sdkLoggerProvider; }

    /**
     * Flushes all pending telemetry and shuts down all providers.
     * Always call this before application exit.
     */
    @Override
    public void close() {
        sdk.close();
    }
}
