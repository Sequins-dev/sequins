package dev.sequins.otel;

import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizer;
import io.opentelemetry.sdk.autoconfigure.spi.AutoConfigurationCustomizerProvider;

/**
 * Plugs Sequins defaults into the OpenTelemetry Java agent's autoconfiguration SPI.
 *
 * <p>When both this library and the OTel Java agent ({@code -javaagent:opentelemetry-javaagent.jar})
 * are present, the agent discovers this class via {@code ServiceLoader} and applies the Sequins
 * defaults (endpoint, protocol, service name) before the agent starts. Explicit system properties
 * and environment variables always take precedence.
 *
 * <p>No code changes are required — simply add this library to the classpath alongside the agent:
 *
 * <pre>{@code
 * java -javaagent:opentelemetry-javaagent.jar \
 *      -cp my-app.jar:sequins-otel.jar \
 *      com.example.Main
 * }</pre>
 */
public final class SequinsAutoConfig implements AutoConfigurationCustomizerProvider {

    private static final String DEFAULT_ENDPOINT = "http://localhost:4317";

    @Override
    public void customize(AutoConfigurationCustomizer customizer) {
        customizer.addPropertiesSupplier(() -> {
            java.util.Map<String, String> props = new java.util.HashMap<>();

            // Set Sequins gRPC endpoint as default only when not already configured.
            String envEndpoint = System.getenv("OTEL_EXPORTER_OTLP_ENDPOINT");
            if (envEndpoint == null || envEndpoint.isEmpty()) {
                props.put("otel.exporter.otlp.endpoint", DEFAULT_ENDPOINT);
                props.put("otel.exporter.otlp.protocol", "grpc");
            }

            // Fall back to "unknown_service" if no service name is configured.
            String envServiceName = System.getenv("OTEL_SERVICE_NAME");
            if (envServiceName == null || envServiceName.isEmpty()) {
                props.put("otel.service.name", "unknown_service");
            }

            return props;
        });
    }
}
