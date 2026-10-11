# Sequins OpenTelemetry SDK for Go

A zero-config OpenTelemetry distro for [Sequins](https://sequins.dev) — the local-first observability platform for OpenTelemetry. One call configures traces, metrics, logs, and optional CPU profiles to export to your local Sequins instance.

## Requirements

- Go >= 1.22
- Sequins running locally (default: `localhost:4317` for gRPC, `localhost:4318` for HTTP)

## Install

```sh
go get github.com/sequins-dev/otel-go
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    "time"

    sequins "github.com/sequins-dev/otel-go"
)

func main() {
    ctx := context.Background()

    s, err := sequins.Init(ctx, sequins.Config{
        ServiceName: "my-app",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer s.Shutdown(ctx)

    // Traces
    tracer := s.Tracer("my-module")
    ctx, span := tracer.Start(ctx, "do-work")
    defer span.End()

    // Metrics
    meter := s.Meter("my-module")
    counter, _ := meter.Int64Counter("requests.total")
    counter.Add(ctx, 1)

    time.Sleep(100 * time.Millisecond)
}
```

The providers are also registered globally, so instrumentation libraries work without additional wiring:

```go
import "go.opentelemetry.io/otel"

tracer := otel.Tracer("my-module")
```

## Instrumentation Libraries

Go requires explicit opt-in for framework instrumentation. Add wrapper middleware after calling `sequins.Init`:

### net/http

```go
import "go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

// Wrap your handler — spans are created for every incoming request.
http.Handle("/", otelhttp.NewHandler(myHandler, "my-server"))

// Wrap outgoing requests.
client := &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
```

### gRPC

```go
import "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"

// Server
grpc.NewServer(
    grpc.StatsHandler(otelgrpc.NewServerHandler()),
)

// Client
conn, _ := grpc.Dial(addr,
    grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
)
```

### Gin

```go
import "go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

router := gin.Default()
router.Use(otelgin.Middleware("my-server"))
```

All contrib packages are at `go.opentelemetry.io/contrib/instrumentation/...` and use the globally-registered providers from `sequins.Init` automatically.

## Configuration

```go
sequins.Config{
    // ServiceName is reported in all telemetry signals.
    // Defaults to OTEL_SERVICE_NAME env var, or "unknown_service".
    ServiceName: "my-app",

    // Endpoint is the OTLP gRPC endpoint (host:port, no scheme).
    // Defaults to OTEL_EXPORTER_OTLP_ENDPOINT env var, or "localhost:4317".
    Endpoint: "localhost:4317",

    // HTTPEndpoint is used only for profile export.
    // Defaults to "http://localhost:4318".
    HTTPEndpoint: "http://localhost:4318",

    // MetricInterval controls how often metrics are exported.
    // Default: 10 seconds.
    MetricInterval: 10 * time.Second,

    // ProfilesEnabled enables periodic CPU profile capture and export.
    // Default: false.
    ProfilesEnabled: true,

    // ProfileInterval controls how often CPU profiles are captured.
    // Default: 30 seconds.
    ProfileInterval: 30 * time.Second,
}
```

## Environment Variables

| Variable | Description | Default |
|---|---|---|
| `OTEL_SERVICE_NAME` | Service name reported in all signals | `unknown_service` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP gRPC endpoint (`host:port`) | `localhost:4317` |

Environment variables are used as fallbacks when the corresponding `Config` field is empty.

## Signals

| Signal | Status | Transport |
|---|---|---|
| Traces | Supported | OTLP/gRPC |
| Metrics | Supported | OTLP/gRPC |
| Logs | Supported | OTLP/gRPC |
| Profiles | Supported (set `ProfilesEnabled: true`) | OTLP/HTTP |

## Shutdown

Always call `Shutdown` before your program exits to flush buffered telemetry:

```go
s, err := sequins.Init(ctx, sequins.Config{ServiceName: "my-app"})
if err != nil {
    log.Fatal(err)
}
defer func() {
    if err := s.Shutdown(context.Background()); err != nil {
        log.Printf("sequins shutdown: %v", err)
    }
}()
```

## License

MIT
