# Sequins OpenTelemetry SDK for Java

A thin OpenTelemetry distribution that pre-configures traces, metrics, and logs to export to a local [Sequins](https://sequins.dev) instance. Zero boilerplate — one call wires up all three signals.

## Requirements

- Java 11+
- Sequins running locally (default: `http://localhost:4317`)

## Install

### Gradle (Kotlin DSL)

```kotlin
dependencies {
    implementation("dev.sequins:sequins-otel:0.1.0")
}
```

### Gradle (Groovy DSL)

```groovy
dependencies {
    implementation 'dev.sequins:sequins-otel:0.1.0'
}
```

### Maven

```xml
<dependency>
    <groupId>dev.sequins</groupId>
    <artifactId>sequins-otel</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Quick Start

```java
import dev.sequins.otel.Sequins;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.LongCounter;

public class Main {
    public static void main(String[] args) {
        // Initialize — wires up traces, metrics, and logs in one call
        Sequins sequins = Sequins.init("my-app");

        // Traces
        Tracer tracer = sequins.getTracerProvider()
                .tracerBuilder("my-app")
                .build();

        Span span = tracer.spanBuilder("handle-request").startSpan();
        try {
            // ... your work here ...
        } finally {
            span.end();
        }

        // Metrics
        Meter meter = sequins.getMeterProvider()
                .meterBuilder("my-app")
                .build();

        LongCounter requests = meter.counterBuilder("requests.total")
                .setDescription("Total number of requests handled")
                .build();
        requests.add(1);

        // Flush and shut down before exit
        sequins.close();
    }
}
```

## Configuration

Use `SequinsConfig.builder()` for full control:

```java
import dev.sequins.otel.SequinsConfig;
import java.time.Duration;

SequinsConfig config = SequinsConfig.builder()
        .serviceName("my-app")
        .endpoint("http://localhost:4317")        // Sequins OTLP/gRPC endpoint
        .metricExportInterval(Duration.ofSeconds(5)) // How often metrics are pushed
        .build();

Sequins sequins = Sequins.init(config);
```

| Option | Default | Description |
|---|---|---|
| `serviceName` | `"unknown_service"` | Name attached to all telemetry as `service.name` |
| `endpoint` | `http://localhost:4317` | OTLP/gRPC endpoint for Sequins |
| `metricExportInterval` | `10s` | Interval between metric export flushes |

## Environment Variables

Standard OTel environment variables are respected and take precedence over programmatic defaults:

| Variable | Overrides |
|---|---|
| `OTEL_SERVICE_NAME` | `serviceName` in `SequinsConfig` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `endpoint` in `SequinsConfig` |

## Signals

| Signal | Status |
|---|---|
| Traces | Supported |
| Metrics | Supported |
| Logs | Supported |
| Profiles | Coming soon |

All supported signals export via OTLP/gRPC using `BatchSpanProcessor`, `PeriodicMetricReader`, and `BatchLogRecordProcessor` respectively.

## Auto-Instrumentation

### With the OTel Java Agent

The [OpenTelemetry Java agent](https://opentelemetry.io/docs/zero-code/java/agent/) instruments your JVM bytecode automatically — no code changes required. Adding this library to the classpath alongside the agent registers Sequins as the default destination via the `AutoConfigurationCustomizerProvider` SPI:

```sh
java -javaagent:opentelemetry-javaagent.jar \
     -cp my-app.jar:sequins-otel.jar \
     com.example.Main
```

The agent picks up Sequins' default endpoint (`http://localhost:4317`) automatically. Standard environment variables still take precedence:

```sh
export OTEL_SERVICE_NAME=my-app
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

### Without the Agent

`Sequins.init()` calls `buildAndRegisterGlobal()`, registering all providers with `GlobalOpenTelemetry`. Any instrumentation library that resolves providers via `GlobalOpenTelemetry.get()` will automatically send data to Sequins without additional configuration.

## Shutdown

Always call `sequins.close()` (or use try-with-resources) before your process exits. This flushes any buffered spans, metric data points, and log records that have not yet been exported.

```java
try (Sequins sequins = Sequins.init("my-app")) {
    // ... application logic ...
} // close() called automatically
```

## License

MIT
