# Module Breakdown

[← Back to Index](INDEX.md)

**Related Documentation:** [workspace-and-crates.md](workspace-and-crates.md) | [implementation-roadmap.md](implementation-roadmap.md)

---

## Module Organization

### `src/main.rs`
**Responsibilities:**
- Initialize GPUI app
- Create app window
- Start OTLP servers in background tasks
- Start retention manager
- Handle graceful shutdown

**Key Functions:**
```rust
#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    // Load configuration
    // Create storage
    // Start OTLP servers
    // Start GPUI app
    // Handle shutdown
}
```

### `src/ui/` - UI Components

#### `mod.rs`
- Re-exports all UI components
- Common UI types and utilities

#### `app.rs`
- Main `AppWindow` component
- Top-level layout
- Window lifecycle management

#### `sidebar.rs`
- Service navigator (flat list)
- Service item rendering
- Health indicator dots
- Service selection handling

#### `title_bar.rs`
- Service information display
- Instance count
- Uptime display
- Window controls

#### `tabs.rs`
- Tab navigation component
- Tab switching logic
- Active tab state

#### `filter_bar.rs`
- Time range picker
- Search input
- Filter dropdown
- Filter state management

#### `logs/`
- `mod.rs` - LogsView component
- `log_list.rs` - Virtualized log list
- `log_row.rs` - Individual log row with expand/collapse
- `log_detail.rs` - Expanded log detail view

#### `metrics/`
- `mod.rs` - MetricsView component
- `histogram.rs` - Histogram visualization
- `time_series.rs` - Time series chart
- `bar_chart.rs` - Bar chart for status codes

#### `traces/`
- `mod.rs` - TracesView component
- `trace_timeline.rs` - Waterfall timeline
- `trace_list.rs` - List of traces
- `span_details.rs` - Span details panel

#### `profiles/`
- `mod.rs` - ProfilesView component
- `flame_graph.rs` - Flame graph visualization
- `frame_details.rs` - Frame details panel

### `src/otlp/` - OTLP Endpoints

#### `mod.rs`
- Re-exports OTLP components
- Server lifecycle management
- Start/stop functions

#### `grpc.rs`
- gRPC service implementation
- `TraceService`, `MetricsService`, `LogsService` implementations
- Protobuf → internal model conversion

#### `http.rs`
- HTTP service implementation (Axum)
- Route handlers for `/v1/traces`, `/v1/logs`, `/v1/metrics`
- Protobuf and JSON parsing

#### `ingest.rs`
- `IngestionPipeline` implementation
- Data parsing and enrichment
- Service discovery
- Background storage worker

### `src/storage/` - Database Operations

#### `mod.rs`
- Connection management
- Database initialization
- Common utilities

#### `schema.rs`
- Schema creation
- Table definitions
- Index creation
- Migrations (future)

#### `queries.rs`
- Common query functions
- Query builders
- Pagination helpers

#### `traces.rs`
- Trace CRUD operations
- `insert_trace`, `get_trace`, `query_traces`
- Span operations

#### `logs.rs`
- Log CRUD operations
- Full-text search
- `insert_log`, `query_logs`, `search_logs`

#### `metrics.rs`
- Metrics CRUD operations
- `insert_metric_data_point`, `query_metrics`
- Histogram operations

#### `profiles.rs`
- Profile CRUD operations
- pprof parsing
- Flame graph generation

#### `retention.rs`
- `RetentionManager` implementation
- Cleanup logic
- VACUUM operations

### `src/models/` - Data Structures

#### `mod.rs`
- Re-exports all models
- Common traits

#### `service.rs`
- `Service` struct
- Service-related types

#### `trace.rs`
- `Trace`, `Span`, `SpanEvent` structs
- Trace status enums
- Span kind enums

#### `log.rs`
- `LogEntry` struct
- Log severity enum

#### `metric.rs`
- `Metric`, `MetricDataPoint` structs
- Metric type enum
- Histogram types

#### `profile.rs`
- `Profile` struct
- `FlameGraphNode` struct
- Profile parsing utilities

## File Structure

```
src/
├── main.rs                     # Application entry point
├── lib.rs                      # Library exports (if needed)
│
├── ui/
│   ├── mod.rs
│   ├── app.rs                  # AppWindow
│   ├── sidebar.rs              # ServiceNavigator
│   ├── title_bar.rs            # TitleBar
│   ├── tabs.rs                 # TabList
│   ├── filter_bar.rs           # FilterBar
│   │
│   ├── logs/
│   │   ├── mod.rs              # LogsView
│   │   ├── log_list.rs
│   │   ├── log_row.rs
│   │   └── log_detail.rs
│   │
│   ├── metrics/
│   │   ├── mod.rs              # MetricsView
│   │   ├── histogram.rs
│   │   ├── time_series.rs
│   │   └── bar_chart.rs
│   │
│   ├── traces/
│   │   ├── mod.rs              # TracesView
│   │   ├── trace_timeline.rs
│   │   ├── trace_list.rs
│   │   └── span_details.rs
│   │
│   └── profiles/
│       ├── mod.rs              # ProfilesView
│       ├── flame_graph.rs
│       └── frame_details.rs
│
├── otlp/
│   ├── mod.rs
│   ├── grpc.rs                 # gRPC OTLP service
│   ├── http.rs                 # HTTP OTLP service
│   └── ingest.rs               # Ingestion pipeline
│
├── storage/
│   ├── mod.rs
│   ├── schema.rs               # Database schema
│   ├── queries.rs              # Common queries
│   ├── traces.rs               # Trace operations
│   ├── logs.rs                 # Log operations
│   ├── metrics.rs              # Metrics operations
│   ├── profiles.rs             # Profile operations
│   └── retention.rs            # Data retention
│
└── models/
    ├── mod.rs
    ├── service.rs              # Service model
    ├── trace.rs                # Trace/Span models
    ├── log.rs                  # Log model
    ├── metric.rs               # Metric models
    └── profile.rs              # Profile model
```

## Module Responsibilities

### Separation of Concerns

**UI Layer** (`src/ui/`):
- GPUI views and components
- User interaction handling
- Rendering logic
- State observation

**Business Logic** (`src/otlp/`, `src/storage/`):
- Data processing
- Database operations
- OTLP protocol handling
- Background tasks

**Data Models** (`src/models/`):
- Type definitions
- Serialization/deserialization
- Data validation
- Type conversions

### Module Dependencies

```
main.rs
  ├─→ ui/        (depends on models, storage via client)
  ├─→ otlp/      (depends on models, storage)
  └─→ storage/   (depends on models)

ui/
  └─→ models/

otlp/
  ├─→ models/
  └─→ storage/

storage/
  └─→ models/
```

## Testing Organization

```
tests/
├── integration/
│   ├── otlp_ingestion.rs       # Test OTLP endpoints
│   ├── storage.rs              # Test database operations
│   └── retention.rs            # Test retention cleanup
│
├── fixtures/
│   ├── sample_traces.json      # Test data
│   ├── sample_logs.json
│   └── sample_metrics.json
│
└── common/
    ├── mod.rs                  # Test utilities
    └── helpers.rs              # Helper functions
```

---

**Last Updated:** 2025-11-05
