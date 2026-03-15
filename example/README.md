# Sequins Example

A continuous test data generator that produces all four OTLP signal types:

- **Traces** — simulated HTTP request spans with realistic latency distributions
- **Logs** — structured log records at varying severity levels
- **Metrics** — counters, gauges, and histograms for request rate, latency, and error rate
- **Profiles** — real CPU profiles collected via `pprof` and sent as OTLP profiles

## Requirements

- Rust (stable toolchain)
- A running Sequins server accepting OTLP/gRPC on `http://localhost:4317`

## Running

```bash
cargo run
```

The generator runs continuously until interrupted with `Ctrl-C`. On shutdown it flushes all pending telemetry before exiting.

## Configuration

The OTLP endpoint defaults to `http://localhost:4317`. To point at a different collector:

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://my-collector:4317 cargo run
```
