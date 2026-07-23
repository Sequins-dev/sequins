# Sequins OpenTelemetry SDK for Swift

Zero-config OpenTelemetry distribution for Swift applications. Exports traces, metrics, and logs to a local [Sequins](https://sequins.dev) instance via OTLP/HTTP with a single function call — no configuration files required.

Works with server-side Swift frameworks (Vapor, Hummingbird) and macOS applications.

## Requirements

- Swift 5.9+
- macOS 13+ or iOS 16+
- Sequins running locally (start with `sequins server`)

## Installation

Add `SequinsOtel` to your `Package.swift` dependencies:

```swift
// Package.swift
dependencies: [
    .package(url: "https://github.com/sequins-dev/sequins-swift.git", from: "0.1.0"),
],
targets: [
    .target(
        name: "MyApp",
        dependencies: [
            .product(name: "SequinsOtel", package: "sequins-swift"),
        ]
    ),
]
```

## Quick Start

```swift
import SequinsOtel

// Initialize once at app startup
let sequins = try Sequins.initialize(serviceName: "my-app")

// Get a tracer for manual instrumentation
let tracer = sequins.tracerProvider.tracer(name: "my-module")

// Create spans
let span = tracer.startSpan("process-request")
defer { span.end() }

// Flush pending telemetry before exit
sequins.shutdown()
```

### Vapor Example

```swift
import Vapor
import SequinsOtel

@main
struct App {
    static func main() async throws {
        let sequins = try Sequins.initialize(serviceName: "my-vapor-app")
        defer { sequins.shutdown() }

        let app = try await Application.make(.detect())
        try configure(app)
        try await app.execute()
    }
}
```

### Hummingbird Example

```swift
import Hummingbird
import SequinsOtel

let sequins = try Sequins.initialize(serviceName: "my-hummingbird-app")
defer { sequins.shutdown() }

let app = HBApplication(configuration: .init(address: .hostname("localhost", port: 8080)))
try app.start()
app.wait()
```

## Configuration

Pass a `SequinsConfig` for custom settings:

```swift
let config = SequinsConfig(
    serviceName: "my-app",
    endpoint: "http://localhost:4318"
)
let sequins = try Sequins.initialize(config: config)
```

| Option | Type | Default | Description |
|---|---|---|---|
| `serviceName` | `String?` | `OTEL_SERVICE_NAME` or `"unknown_service"` | Identifies your service in Sequins |
| `endpoint` | `String?` | `OTEL_EXPORTER_OTLP_ENDPOINT` or `"http://localhost:4318"` | OTLP HTTP endpoint |

Resolution order for each option: explicit argument > `SequinsConfig` field > environment variable > built-in default.

## Environment Variables

| Variable | Description |
|---|---|
| `OTEL_SERVICE_NAME` | Service name shown in Sequins |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP HTTP base URL (e.g. `http://localhost:4318`) |

Environment variables are read at initialization time. Explicit arguments and `SequinsConfig` values take precedence.

## Signals

| Signal | Status | Notes |
|---|---|---|
| Traces | Supported | Via `swift-otel` + OTLP/HTTP |
| Metrics | Supported | Bootstraps `swift-metrics` `MetricsSystem` with an OTel factory |
| Logs | Supported | Bootstraps `swift-log` `LoggingSystem` with `OTelLogHandler` |
| Profiles | Not supported | No Swift OTel profiles API |

After calling `Sequins.initialize(...)`, standard `swift-log` and `swift-metrics` usage routes automatically through OTel:

```swift
import Logging
import Metrics

var logger = Logger(label: "my-module")
logger.info("Request received", metadata: ["user": "alice"])

let counter = Counter(label: "requests.total")
counter.increment()
```

## Auto-Instrumentation

Swift auto-instrumentation works through the [`swift-distributed-tracing`](https://github.com/apple/swift-distributed-tracing) universal tracing API. Once `Sequins.initialize(...)` calls `OTel.bootstrapTracing(tracerProvider:)`, any library that instruments itself via `swift-distributed-tracing` automatically routes its spans to Sequins — no additional configuration required.

### Vapor

Vapor integrates with `swift-distributed-tracing` natively. Add `vapor-tracing`:

```swift
// Package.swift
.package(url: "https://github.com/vapor-community/vapor-tracing.git", from: "1.0.0"),
```

```swift
// configure.swift
import VaporTracing

app.middleware.use(TracingMiddleware())
```

Every HTTP request is automatically wrapped in a span.

### Hummingbird

Hummingbird 2.x supports `swift-distributed-tracing` natively:

```swift
import HummingbirdTracing

let router = Router()
router.middlewares.add(TracingMiddleware())
```

### Manual span propagation

For any code path you want to trace, wrap it in `withSpan`:

```swift
import Tracing

try await withSpan("process-order") { span in
    span.attributes["order.id"] = orderId
    try await processOrder(orderId)
}
```

This works regardless of framework because `withSpan` uses the globally-bootstrapped tracer.

## License

MIT
