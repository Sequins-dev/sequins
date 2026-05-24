# Sequins OpenTelemetry SDK for Rust

A thin OpenTelemetry distro that pre-configures traces, metrics, logs, and
optional CPU profiling to export to a local [Sequins](https://sequins.io)
instance with zero boilerplate.

---

## Installation

Add the crate to your `Cargo.toml`:

```toml
[dependencies]
sequins-otel = "0.1"
tokio = { version = "1", features = ["full"] }
tracing = "0.1"
```

To enable continuous CPU profiling, turn on the `profiling` feature:

```toml
[dependencies]
sequins-otel = { version = "0.1", features = ["profiling"] }
```

---

## Quick Start

```rust
use opentelemetry::trace::Tracer as _;
use opentelemetry::metrics::MeterProvider as _;
use tracing::info;

#[tokio::main]
async fn main() {
    // Initialise all telemetry signals.  Returns a guard that must be kept
    // alive for the duration of the program.
    let guard = sequins_otel::init("my-app").await;

    // --- Traces ---
    let tracer = guard.tracer_provider().tracer("my-app");
    tracer.in_span("work", |_cx| {
        info!("doing some work");
    });

    // --- Metrics ---
    let meter = guard.meter_provider().meter("my-app");
    let counter = meter.u64_counter("requests").init();
    counter.add(1, &[]);

    // --- Logs (via tracing crate) ---
    info!(user = "alice", "request received");

    // Flush and shut down before exiting.
    guard.shutdown().await;
}
```

---

## Optional Profiling

When the `profiling` feature is enabled a background task captures pprof CPU
profiles and exports them to Sequins via the OTLP HTTP profiles endpoint.

```rust
use sequins_otel::{SequinsConfig, init_with_config};
use std::time::Duration;

#[tokio::main]
async fn main() {
    let config = SequinsConfig {
        profiles_enabled: true,
        profile_interval: Duration::from_secs(30),
        profile_frequency: 99,
        ..SequinsConfig::new("my-app")
    };

    let guard = init_with_config(config).await;

    // ... application code ...

    guard.shutdown().await;
}
```

The profiler runs until `shutdown()` is called.

---

## Configuration

### `SequinsConfig` fields

| Field | Type | Default | Description |
|---|---|---|---|
| `service_name` | `String` | `$OTEL_SERVICE_NAME` or `unknown_service` | Service name in all telemetry |
| `endpoint` | `String` | `$OTEL_EXPORTER_OTLP_ENDPOINT` or `http://localhost:4317` | gRPC OTLP endpoint for traces, metrics, and logs |
| `http_endpoint` | `String` | `http://localhost:4318` | HTTP OTLP endpoint used for profiles |
| `metric_export_interval` | `Duration` | 10 seconds | How often metrics are pushed |
| `profiles_enabled` *(profiling)* | `bool` | `false` | Enable background CPU profiler |
| `profile_interval` *(profiling)* | `Duration` | 30 seconds | Capture window length |
| `profile_frequency` *(profiling)* | `i32` | 99 Hz | CPU sampling frequency |

### Environment Variables

| Variable | Effect |
|---|---|
| `OTEL_SERVICE_NAME` | Sets the service name when using `Default` or `init()` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Overrides the gRPC collector endpoint |
| `RUST_LOG` | Controls log-level filtering via `tracing-subscriber` (e.g. `RUST_LOG=debug`) |

---

## Signals

| Signal | Status | Notes |
|---|---|---|
| Traces | Supported | OTLP gRPC, batch processor |
| Metrics | Supported | OTLP gRPC, configurable export interval |
| Logs | Supported | Bridged from the `tracing` crate via `opentelemetry-appender-tracing` |
| Profiles | Supported | Requires `features = ["profiling"]`; pprof CPU sampling, OTLP HTTP |

---

## `tracing` Auto-Instrumentation

`init` / `init_with_config` installs a global `tracing-subscriber` registry
that includes:

- **`tracing-opentelemetry` span layer** — converts every `tracing::Span`
  (created via `#[tracing::instrument]`, `tracing::info_span!()`, etc.) into
  an OTel span exported to Sequins. This is the primary auto-instrumentation
  hook for Rust.
- **OTel log bridge** — forwards `tracing` events (`info!`, `warn!`, etc.) to
  the OTLP logs pipeline via `opentelemetry-appender-tracing`.
- **`fmt` layer** — human-readable console output for development.
- **`EnvFilter` layer** — honours `RUST_LOG` for log-level filtering.

The registry is installed with `try_init()`, so calling `init` a second time
(e.g. in tests) is safe and will not panic.

Because spans flow through `tracing`, any library that instruments itself with
`#[tracing::instrument]` (e.g. `tower`, `sqlx`, `tonic`) automatically emits
spans to Sequins without any additional configuration.

---

## Requirements

- Rust edition 2021
- An async [Tokio](https://tokio.rs) runtime (`features = ["full"]` or at
  minimum `rt-multi-thread` + `macros`)
- A running Sequins instance (or any OTLP-compatible collector)
