# Sequins OpenTelemetry SDK for Elixir

Zero-config OpenTelemetry distribution for Elixir applications. Exports traces, metrics, and logs to a local [Sequins](https://sequins.dev) instance via OTLP/gRPC with minimal setup.

## Requirements

- Elixir >= 1.15
- Sequins running locally (start with `sequins server`)

## Installation

Add `sequins_otel` to your `mix.exs` dependencies:

```elixir
def deps do
  [
    {:sequins_otel, "~> 0.1"}
  ]
end
```

Then fetch dependencies:

```sh
mix deps.get
```

## Quick Start

### Option 1: configure/1 in Application.start/2 (recommended)

Call `SequinsOtel.configure/1` before starting your supervision tree. The OTel application must already be started (e.g. included in your dependencies).

```elixir
# lib/my_app/application.ex
defmodule MyApp.Application do
  use Application

  def start(_type, _args) do
    SequinsOtel.configure(service_name: "my-app")

    children = [
      # ... your supervision tree
    ]

    opts = [strategy: :one_for_one, name: MyApp.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
```

### Option 2: init/1 for scripts and one-off processes

`SequinsOtel.init/1` configures and starts the OTel application in one call. Use this in scripts or when you need explicit lifecycle control.

```elixir
{:ok, sequins} = SequinsOtel.init(service_name: "my-script")

# ... do work ...

SequinsOtel.shutdown(sequins)
```

## Tracing

Use `:opentelemetry_api` to instrument your code with traces:

```elixir
require OpenTelemetry.Tracer

# Wrap a block in a span
OpenTelemetry.Tracer.with_span "process-order" do
  # your logic here
end

# Set attributes on the current span
OpenTelemetry.Tracer.set_attribute("order.id", order_id)

# Record an error
OpenTelemetry.Tracer.set_status(:error, "something went wrong")
```

### Auto-Instrumentation

Pass `auto_instrument: true` to automatically call `.setup()` on any installed
instrumentation packages:

```elixir
SequinsOtel.configure(service_name: "my-app", auto_instrument: true)
```

This activates `opentelemetry_phoenix`, `opentelemetry_ecto`, and `opentelemetry_oban`
if they are present in your dependency tree. Add the ones you need to `mix.exs`:

```elixir
def deps do
  [
    {:sequins_otel, "~> 0.1"},
    # Add the instrumentation packages you want:
    {:opentelemetry_phoenix, "~> 2.0"},
    {:opentelemetry_ecto, "~> 1.2"},
    {:opentelemetry_oban, "~> 1.1"},     # if using Oban
  ]
end
```

#### Manual setup

You can also call `.setup()` yourself for full control, e.g. to pass Ecto repo names:

```elixir
def start(_type, _args) do
  SequinsOtel.configure(service_name: "my-app")
  OpentelemetryPhoenix.setup()
  OpentelemetryEcto.setup([:my_app, :repo])
  # ...
end
```

## Configuration

| Option | Type | Default | Description |
|---|---|---|---|
| `:service_name` | `String` | `OTEL_SERVICE_NAME` or `"unknown_service"` | Identifies your service in Sequins |
| `:endpoint` | `String` | `OTEL_EXPORTER_OTLP_ENDPOINT` or `"http://localhost:4317"` | OTLP gRPC endpoint |
| `:traces_enabled` | `boolean` | `true` | Enable trace export |
| `:metrics_enabled` | `boolean` | `true` | Enable metrics export |
| `:logs_enabled` | `boolean` | `true` | Enable log export |

Resolution order for each option: explicit argument > environment variable > built-in default.

```elixir
SequinsOtel.configure(
  service_name: "my-app",
  endpoint: "http://localhost:4317"
)
```

## Environment Variables

| Variable | Description |
|---|---|
| `OTEL_SERVICE_NAME` | Service name shown in Sequins |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC base URL (e.g. `http://localhost:4317`) |

Environment variables are read at configuration time. Explicit keyword arguments take precedence.

## Signals

| Signal | Status |
|---|---|
| Traces | Supported |
| Metrics | Supported (via OTLP) |
| Logs | Supported |
| Profiles | Not supported |

## License

MIT
