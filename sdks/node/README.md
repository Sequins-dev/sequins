# Sequins OpenTelemetry SDK for Node.js

A zero-config OpenTelemetry distro for [Sequins](https://github.com/sequinstream/sequins) — the local-first observability platform for developers. Drop this into any Node.js app to start sending traces, metrics, and logs to a locally running Sequins instance with a single function call.

## Requirements

- Node.js >= 18
- [Sequins](https://github.com/sequinstream/sequins) running locally

## Installation

```bash
npm install @sequins/otel
```

## Quick Start

Call `init()` at the very start of your application, before importing any other modules that need instrumentation:

```typescript
import { init } from '@sequins/otel';

const sequins = init({ serviceName: 'my-api' });

// Get a tracer for manual instrumentation
const tracer = sequins.tracerProvider.getTracer('my-module');

// Create a span
tracer.startActiveSpan('handle-request', (span) => {
  // ... your work here ...
  span.end();
});

// Flush and shut down before process exit
process.on('SIGTERM', async () => {
  await sequins.shutdown();
  process.exit(0);
});
```

## Configuration

Pass a config object to `init()` to override defaults:

| Option | Type | Default | Description |
|---|---|---|---|
| `serviceName` | `string` | `OTEL_SERVICE_NAME` env var, or `'unknown_service'` | Service name reported in all telemetry |
| `endpoint` | `string` | `OTEL_EXPORTER_OTLP_ENDPOINT` env var, or `'http://localhost:4317'` | OTLP gRPC endpoint for all signals |
| `metricExportIntervalMs` | `number` | `10000` | How often metrics are exported, in milliseconds |
| `instrumentations` | `Instrumentation[]` | All from `@opentelemetry/auto-instrumentations-node` | Auto-instrumentation plugins. Pass `[]` to disable. |

## Auto-Instrumentation

`init()` automatically registers [`@opentelemetry/auto-instrumentations-node`](https://www.npmjs.com/package/@opentelemetry/auto-instrumentations-node), which covers HTTP, gRPC, Express, Fastify, Koa, GraphQL, Redis, PostgreSQL, MySQL, and many more.

**Call `init()` before importing any other modules** so the instrumentation patches are applied before the libraries are loaded:

```typescript
// index.ts — first lines of your app entry point
import { init } from '@sequins/otel';
const sequins = init({ serviceName: 'my-api' });

// Import application modules AFTER init
import { createServer } from './server';
```

To disable auto-instrumentation entirely, pass an empty array:

```typescript
const sequins = init({ serviceName: 'my-api', instrumentations: [] });
```

To select specific instrumentations:

```typescript
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';

const sequins = init({
  serviceName: 'my-api',
  instrumentations: getNodeAutoInstrumentations({
    '@opentelemetry/instrumentation-fs': { enabled: false }, // disable noisy fs instrumentation
  }),
});
```

## Environment Variables

Standard OpenTelemetry environment variables are respected and take precedence over built-in defaults (but are overridden by explicit config options):

| Variable | Description |
|---|---|
| `OTEL_SERVICE_NAME` | Service name reported in all telemetry |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint for all signals |

## Signals

| Signal | Status |
|---|---|
| Traces | Supported |
| Metrics | Supported |
| Logs | Supported |
| Profiles | Coming soon |

All signals are exported via OTLP gRPC to the configured endpoint (default: `http://localhost:4317`). Sequins also accepts OTLP HTTP on `http://localhost:4318` if you prefer HTTP transport.

## API

### `init(config?: SequinsConfig): SequinsHandle`

Initializes all three OTel providers and registers them as the global providers. Returns a `SequinsHandle` with:

- **`tracerProvider`** — `NodeTracerProvider` for creating tracers
- **`meterProvider`** — `MeterProvider` for creating meters
- **`loggerProvider`** — `LoggerProvider` for creating loggers
- **`shutdown()`** — flushes all pending telemetry and shuts down providers; always call this before process exit

## Links

- [Sequins](https://github.com/sequinstream/sequins)
- [OpenTelemetry JS](https://github.com/open-telemetry/opentelemetry-js)

## License

MIT
