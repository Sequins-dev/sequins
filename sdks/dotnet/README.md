# Sequins OpenTelemetry SDK for .NET

A thin OpenTelemetry distribution that pre-configures traces, metrics, and logs to export to a local [Sequins](https://sequins.io) instance. Zero boilerplate — one line to instrument your app.

## Requirements

- .NET 8 or later
- Sequins running locally (default: `http://localhost:4317`)

## Installation

```sh
dotnet add package Sequins.OpenTelemetry
```

## Quick Start

### ASP.NET Core / Generic Host

Add Sequins in `Program.cs` before building the host:

```csharp
using Sequins.OpenTelemetry;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSequins("my-app");

var app = builder.Build();
// ...
app.Run();
```

To customise options:

```csharp
builder.Services.AddSequins("my-app", options =>
{
    options.Endpoint = "http://localhost:4317";
    options.MetricExportInterval = TimeSpan.FromSeconds(5);
});
```

### Standalone (no DI / console apps)

```csharp
using Sequins.OpenTelemetry;

using var sequins = new SequinsBuilder("my-app").Build();

// Obtain a tracer from the configured provider
var tracer = sequins.TracerProvider.GetTracer("my-module");

using (var span = tracer.StartActiveSpan("do-work"))
{
    // ... your code ...
}
```

With custom options:

```csharp
var options = new SequinsOptions
{
    ServiceName = "my-app",
    Endpoint = "http://localhost:4317",
    MetricExportInterval = TimeSpan.FromSeconds(5),
};

using var sequins = new SequinsBuilder(options).Build();
```

## Configuration

All options can be set programmatically via `SequinsOptions` or overridden with standard OpenTelemetry environment variables.

### SequinsOptions Properties

| Property | Type | Default | Description |
|---|---|---|---|
| `ServiceName` | `string?` | `"unknown_service"` | Name of your service as it appears in Sequins |
| `Endpoint` | `string` | `"http://localhost:4317"` | OTLP gRPC endpoint for the Sequins collector |
| `MetricExportInterval` | `TimeSpan` | `00:00:10` | How often metrics are pushed to Sequins |

### Environment Variables

Environment variables take precedence over values set in code.

| Variable | Description |
|---|---|
| `OTEL_SERVICE_NAME` | Overrides `SequinsOptions.ServiceName` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Overrides `SequinsOptions.Endpoint` |

## Signals

| Signal | Status |
|---|---|
| Traces | Supported |
| Metrics | Supported |
| Logs | Supported |
| Profiles | Coming soon |

## License

MIT
