package dev.sequins.otel;

import java.time.Duration;

/**
 * Configuration for the Sequins OpenTelemetry SDK.
 * Use {@link SequinsConfig#builder()} to construct instances.
 *
 * <p>All fields fall back to standard OTel environment variables when not set:
 * <ul>
 *   <li>{@code serviceName} → {@code OTEL_SERVICE_NAME}</li>
 *   <li>{@code endpoint} → {@code OTEL_EXPORTER_OTLP_ENDPOINT}</li>
 * </ul>
 */
public final class SequinsConfig {

    private final String serviceName;
    private final String endpoint;
    private final Duration metricExportInterval;

    private SequinsConfig(Builder builder) {
        this.serviceName = builder.serviceName;
        this.endpoint = builder.endpoint;
        this.metricExportInterval = builder.metricExportInterval;
    }

    public String getServiceName() { return serviceName; }
    public String getEndpoint() { return endpoint; }
    public Duration getMetricExportInterval() { return metricExportInterval; }

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private String serviceName;
        private String endpoint = "http://localhost:4317";
        private Duration metricExportInterval = Duration.ofSeconds(10);

        public Builder serviceName(String serviceName) {
            this.serviceName = serviceName;
            return this;
        }

        public Builder endpoint(String endpoint) {
            this.endpoint = endpoint;
            return this;
        }

        public Builder metricExportInterval(Duration interval) {
            this.metricExportInterval = interval;
            return this;
        }

        public SequinsConfig build() {
            return new SequinsConfig(this);
        }
    }
}
