# Sequins OpenTelemetry SDKs

Zero-config OpenTelemetry distros for [Sequins](https://sequins.dev) — the local-first observability platform for developers.

Each SDK wraps the official OpenTelemetry SDK for its language and pre-configures all signals (traces, metrics, logs, and profiles where available) to export to a local Sequins instance. Single function call to get started; sensible defaults for everything.

## Quick Start

Pick your language and follow the link for install instructions and a quick-start example:

| Language | Package | Transport | Traces | Metrics | Logs | Profiles |
|----------|---------|-----------|:------:|:-------:|:----:|:--------:|
| [Node.js](./node/) | `@sequins/otel` | gRPC | ✓ | ✓ | ✓ | ✓ |
| [Python](./python/) | `sequins-otel` | gRPC | ✓ | ✓ | ✓ | — |
| [Go](./go/) | `github.com/sequins-dev/otel-go` | gRPC | ✓ | ✓ | ✓ | ✓ |
| [Java](./java/) | `dev.sequins:sequins-otel` | gRPC | ✓ | ✓ | ✓ | — |
| [Rust](./rust/) | `sequins-otel` | gRPC | ✓ | ✓ | ✓ | ✓ (feature flag) |
| [C# / .NET](./dotnet/) | `Sequins.OpenTelemetry` | gRPC | ✓ | ✓ | ✓ | — |
| [Ruby](./ruby/) | `sequins-otel` | HTTP | ✓ | ✓ | ✓ | — |
| [PHP](./php/) | `sequins/otel` | HTTP | ✓ | ✓ | ✓ | — |
| [Swift](./swift/) | `SequinsOtel` (SPM) | HTTP | ✓ | ✓ | ✓ | — |
| [Elixir](./elixir/) | `sequins_otel` | gRPC | ✓ | ✓ | ✓ | — |

## Defaults

All SDKs default to:

- **gRPC endpoint:** `http://localhost:4317`
- **HTTP endpoint:** `http://localhost:4318` (Ruby, PHP, Swift)
- **Metric export interval:** 10 seconds
- **Sampler:** AlwaysOn
- **Propagator:** W3C TraceContext + Baggage

Override with standard OTel environment variables:

```sh
export OTEL_SERVICE_NAME=my-app
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
```

## Prerequisites

- [Sequins](https://sequins.dev) running locally (starts OTLP servers on ports 4317 and 4318 by default)
