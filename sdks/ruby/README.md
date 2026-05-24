# Sequins OpenTelemetry SDK for Ruby

A zero-config OpenTelemetry distro for Ruby that exports traces, metrics, and logs to a local [Sequins](https://sequins.dev) instance. Get full observability in local development with a single method call.

## Installation

Add to your Gemfile:

```ruby
gem "sequins-otel"
```

Then run:

```bash
bundle install
```

Or install directly:

```bash
gem install sequins-otel
```

## Quick Start

```ruby
require "sequins_otel"

# Initialize once at application startup
sequins = SequinsOtel.init(service_name: "my-app")

# Get a tracer for your module
tracer = sequins.tracer_provider.tracer("my-module")

# Create spans
tracer.in_span("process-request") do |span|
  span.set_attribute("http.method", "GET")
  span.set_attribute("http.url", "https://example.com/api/users")

  # Your code here
  result = fetch_users

  span.set_attribute("result.count", result.length)
end

# Always shut down before your process exits to flush pending telemetry
at_exit { sequins.shutdown }
```

## Configuration

All parameters to `SequinsOtel.init` are optional.

| Parameter | Type | Default | Description |
|---|---|---|---|
| `service_name` | `String` | `"unknown_service"` | The name of your service as it appears in Sequins |
| `endpoint` | `String` | `"http://localhost:4318"` | The OTLP HTTP base endpoint for your Sequins instance |
| `metric_export_interval` | `Integer` | `10000` | How often metrics are exported, in milliseconds |
| `use_all` | `Boolean` | `false` | When `true`, installs all available OTel instrumentations (requires `opentelemetry-instrumentation-all`) |

## Auto-Instrumentation

Pass `use_all: true` to automatically instrument all supported libraries in your app:

```ruby
require "sequins_otel"

sequins = SequinsOtel.init(service_name: "my-app", use_all: true)
```

This requires the `opentelemetry-instrumentation-all` gem, which bundles instrumentation for Rails, Rack, Sinatra, Faraday, Redis, and many others:

```ruby
# Gemfile
gem "sequins-otel"
gem "opentelemetry-instrumentation-all"
```

You can also install only the instrumentations you need:

```ruby
# Gemfile
gem "sequins-otel"
gem "opentelemetry-instrumentation-rack"
gem "opentelemetry-instrumentation-rails"
gem "opentelemetry-instrumentation-active_record"
```

When using individual gems, call `c.use` manually inside an `OpenTelemetry::SDK.configure` block after `SequinsOtel.init`, or pass them to `use_all` via the standard OTel SDK configuration.

## Environment Variables

The SDK respects standard OpenTelemetry environment variables. These take lower precedence than values passed directly to `init`.

| Variable | Description |
|---|---|
| `OTEL_SERVICE_NAME` | Service name (overridden by `service_name:` kwarg if provided) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP HTTP endpoint (overridden by `endpoint:` kwarg if provided) |

## Signals

| Signal | Status | Notes |
|---|---|---|
| Traces | Supported | OTLP/HTTP, `BatchSpanProcessor`, gzip compression |
| Metrics | Supported | Via `meter_provider` from the SDK |
| Logs | Supported | OTLP/HTTP via `opentelemetry-exporter-otlp-logs`; set globally via `OpenTelemetry::Logs.logger_provider` |
| Profiles | Not supported | No Ruby OTel profiles API exists |

## Shutdown

Always call `shutdown` before your process exits. This flushes all buffered telemetry to Sequins so no data is lost.

```ruby
sequins = SequinsOtel.init(service_name: "my-app")

# Register at startup
at_exit { sequins.shutdown }
```

## Requirements

- Ruby >= 3.0
- Sequins running locally (default port 4318 for OTLP HTTP)

## Transport

This SDK uses **HTTP/protobuf** transport to port `4318` (not gRPC port `4317`). HTTP is the most reliable transport for Ruby applications and works without additional native dependencies.

Traces are sent to `http://localhost:4318/v1/traces`.

## License

MIT
