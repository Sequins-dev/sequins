# Sequins OpenTelemetry SDK for PHP

OpenTelemetry distro for [Sequins](https://sequins.io) — zero-config observability for local development. Pre-configures traces, metrics, and logs to export to a local Sequins instance via OTLP/HTTP.

## Requirements

- PHP >= 8.1
- [Sequins](https://sequins.io) running locally (listens on port 4318 by default)

## Installation

```bash
composer require sequins/otel
```

## Quick Start

```php
<?php

require 'vendor/autoload.php';

use Sequins\Otel\Sequins;

// Initialize all signals (traces, metrics, logs) in one call
$sequins = Sequins::init('my-app');

// Create a tracer and record a span
$tracer = $sequins->getTracerProvider()->getTracer('my-module');

$span = $tracer->spanBuilder('process-order')
    ->startSpan();

$span->setAttribute('order.id', '12345');

try {
    // ... your application logic ...
} finally {
    $span->end();
}

// Shutdown is called automatically on script exit.
// You can also call it explicitly:
$sequins->shutdown();
```

## Configuration

### `Sequins::init()` Parameters

| Parameter     | Type          | Default                          | Description                                    |
|---------------|---------------|----------------------------------|------------------------------------------------|
| `$serviceName` | `string`     | _(required)_                     | The name of your service, as it appears in Sequins. |
| `$endpoint`   | `string\|null` | `http://localhost:4318`          | OTLP HTTP base URL. Overrides the environment variable if set. |
| `$options`    | `array`       | `[]`                             | Reserved for future use.                       |

### Environment Variables

| Variable                        | Description                                                                 |
|---------------------------------|-----------------------------------------------------------------------------|
| `OTEL_SERVICE_NAME`             | Fallback service name if not provided in code.                              |
| `OTEL_EXPORTER_OTLP_ENDPOINT`   | OTLP HTTP base URL (e.g. `http://localhost:4318`). Used when `$endpoint` is not passed to `init()`. |

Environment variables are only used as fallbacks — values passed directly to `Sequins::init()` always take precedence.

## Signals

| Signal   | Supported | Notes                                    |
|----------|-----------|------------------------------------------|
| Traces   | Yes       | Exported via OTLP HTTP to `/v1/traces`   |
| Metrics  | Yes       | Exported via OTLP HTTP to `/v1/metrics`  |
| Logs     | Yes       | Exported via OTLP HTTP to `/v1/logs`     |
| Profiles | No        | Not supported by the OTLP PHP SDK        |

All signals are exported using **HTTP transport on port 4318** with protobuf encoding (`application/x-protobuf`).

## Automatic Shutdown

`Sequins::init()` registers a PHP shutdown function that automatically flushes all pending telemetry when your script exits. Batched spans, metrics, and log records are all flushed before the process terminates, so you do not need to call `shutdown()` manually in most cases.

For long-running processes (e.g. workers) where you want to flush mid-execution, call `$sequins->shutdown()` explicitly.

## Accessing Providers

The `SequinsHandle` returned by `Sequins::init()` provides access to all three providers:

```php
$sequins = Sequins::init('my-app');

// Tracing
$tracer = $sequins->getTracerProvider()->getTracer('instrumentation-scope');

// Metrics
$meter = $sequins->getMeterProvider()->getMeter('instrumentation-scope');
$counter = $meter->createCounter('requests.total', '{request}', 'Total HTTP requests');
$counter->add(1, ['http.method' => 'GET']);

// Logs (via PSR-3 bridge or directly)
$logger = $sequins->getLoggerProvider()->getLogger('instrumentation-scope');
```

All providers are also registered globally via `OpenTelemetry\API\Globals`, so instrumentation libraries that use `Globals::tracerProvider()` will automatically pick up the Sequins configuration.

## Auto-Instrumentation

PHP OpenTelemetry auto-instrumentation works through the [`opentelemetry-php-instrumentation`](https://github.com/open-telemetry/opentelemetry-php-instrumentation) C extension, which hooks into PHP function calls at the extension level.

### Install the extension

```bash
pecl install opentelemetry
# Add to php.ini: extension=opentelemetry.so
```

### Add framework packages

Once the extension is installed, add auto-instrumentation packages for your framework. These activate automatically when the extension is loaded:

```bash
# PSR-18 HTTP clients (Guzzle, Symfony HttpClient, etc.)
composer require open-telemetry/opentelemetry-auto-psr18

# PSR-15 middleware / request handlers (Slim, Mezzio, etc.)
composer require open-telemetry/opentelemetry-auto-psr15

# PSR-3 loggers (Monolog, etc.)
composer require open-telemetry/opentelemetry-auto-psr3
```

All auto-instrumentation packages read providers from `OpenTelemetry\API\Globals`, which `Sequins::init()` already populates — no additional wiring required.

### Laravel / Symfony

Framework-specific packages are available:
- **Laravel**: `composer require open-telemetry/opentelemetry-auto-laravel`
- **Symfony**: `composer require open-telemetry/opentelemetry-auto-symfony`

## License

MIT
