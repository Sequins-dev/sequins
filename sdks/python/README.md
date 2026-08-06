# Sequins OpenTelemetry SDK for Python

A zero-config OpenTelemetry distro for [Sequins](https://sequins.io) — the local-first observability platform for developers. One call wires up traces, metrics, and logs to export to your local Sequins instance via OTLP/gRPC.

## Installation

```bash
pip install sequins-otel
```

## Quick Start

```python
from sequins_otel import init

# Initialize all OTel signals — traces, metrics, and logs
sequins = init(service_name="my-app")

# Get a tracer and create spans
tracer = sequins.tracer_provider.get_tracer("my-module")

with tracer.start_as_current_span("my-operation") as span:
    span.set_attribute("user.id", "42")
    # ... do work ...

# Get a meter and record metrics
meter = sequins.meter_provider.get_meter("my-module")
counter = meter.create_counter("requests.total")
counter.add(1, {"endpoint": "/api/users"})

# Flush and shut down on exit
sequins.shutdown()
```

## Configuration

### Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `service_name` | `str` | `"unknown_service"` | Name of your service, as it appears in Sequins. |
| `endpoint` | `str` | `"http://localhost:4317"` | OTLP gRPC endpoint for your Sequins instance. |
| `metric_export_interval_ms` | `int` | `10000` | How often metrics are exported, in milliseconds. |
| `auto_instrument` | `bool` | `False` | When `True`, activates all installed OTel instrumentors discovered via entry points. |

## Auto-Instrumentation

Pass `auto_instrument=True` to automatically activate all installed OTel instrumentors:

```python
from sequins_otel import init

sequins = init(service_name="my-app", auto_instrument=True)
```

Install framework-specific instrumentor packages alongside your app:

```bash
# Flask
pip install opentelemetry-instrumentation-flask

# Django
pip install opentelemetry-instrumentation-django

# HTTPX / requests
pip install opentelemetry-instrumentation-httpx opentelemetry-instrumentation-requests

# SQLAlchemy
pip install opentelemetry-instrumentation-sqlalchemy
```

Each instrumentor registers itself under the `opentelemetry_instrumentor` entry-points group and is activated automatically when `auto_instrument=True`. Only installed packages are activated — there are no mandatory framework dependencies.

### Environment Variables

The SDK respects standard OpenTelemetry environment variables as fallbacks:

| Variable | Description |
|---|---|
| `OTEL_SERVICE_NAME` | Service name (overridden by the `service_name` parameter if provided). |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint (overridden by the `endpoint` parameter if provided). |

Parameter values always take precedence over environment variables.

## Signals

| Signal | Status |
|---|---|
| Traces | Supported |
| Metrics | Supported |
| Logs | Supported |
| Profiles | Coming soon |

## Requirements

- Python >= 3.9
- Sequins running locally (default: `http://localhost:4317`)

## License

MIT
