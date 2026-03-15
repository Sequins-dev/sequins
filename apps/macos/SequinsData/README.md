# SequinsData

Swift package providing access to Sequins telemetry data through a clean, type-safe API.

## Overview

SequinsData wraps the Sequins FFI library, providing Swift-native types and APIs for:

- **DataSource Management** - Local (embedded storage) and remote (client to daemon) modes
- **Span Queries** - Query OpenTelemetry spans with streaming results
- **Management Operations** - Configure retention policies, view statistics, run maintenance

## Installation

Add SequinsData as a dependency in your `Package.swift`:

```swift
dependencies: [
    .package(path: "../SequinsData")
]
```

## Requirements

- macOS 13.0+ or iOS 16.0+
- Swift 5.9+
- sequins-ffi library (Rust FFI layer)

## Usage

### Create a Data Source

```swift
import SequinsData

// Local mode with embedded storage
let config = OTLPServerConfig(grpcPort: 4317, httpPort: 4318)
let dataSource = try DataSource.local(
    dbPath: "/path/to/sequins.db",
    config: config
)

// Or remote mode (connect to sequins-daemon)
let dataSource = try DataSource.remote(
    queryURL: "http://localhost:8080/query",
    managementURL: "http://localhost:8080/management"
)
```

### Query Spans

```swift
let query = SpanQuery(
    traceId: nil,
    service: "my-service",
    startTime: Date().addingTimeInterval(-3600),
    endTime: Date(),
    limit: 100
)

// Phase 1: Query historical data
let result = try dataSource.querySpans(query: query)
print("Loaded \(result.spans.count) historical spans")

// Phase 2: Subscribe for live updates
let streamHandle = try dataSource.subscribeSpans(
    query: query,
    cursor: result.cursor
) { span in
    print("Received live span: \(span.operationName)")
    return true // Continue streaming
}

// Later: cancel the subscription
streamHandle.cancel()
```

### Management Operations

```swift
// Update retention policy
let policy = RetentionPolicy(
    spansRetention: 86400,      // 1 day
    logsRetention: 604800,      // 7 days
    metricsRetention: 2592000,  // 30 days
    profilesRetention: 172800   // 2 days
)
try dataSource.updateRetentionPolicy(policy)

// Get storage statistics
let stats = try dataSource.getStorageStats()
print("Spans: \(stats.spanCount), Logs: \(stats.logCount)")

// Run maintenance
let maintenanceStats = try dataSource.runMaintenance()
print("Evicted: \(maintenanceStats.entriesEvicted)")
```

## Architecture

The package is structured in layers:

- **SequinsFFI** - C module wrapping the Rust FFI library
- **SequinsData** - Swift types and APIs:
  - `SequinsError` - Error types
  - `DataSource` - Data source lifecycle
  - `SpanTypes` - OpenTelemetry span types
  - `QueryAPI` - Query operations
  - `ManagementAPI` - Management operations

## Testing

Run the test suite:

```bash
swift test
```

All tests follow TDD principles - tests were written first, then implementations.

## License

See the main Sequins project for license information.
