# Architecture

[← Back to Index](INDEX.md)

**Related Documentation:** [workspace-and-crates](workspace-and-crates.md) | [deployment](deployment.md) | [otlp-ingestion](otlp-ingestion.md) | [scaling-strategy](scaling-strategy.md)

---

## Overview

Sequins follows a three-layer architecture that cleanly separates concerns between presentation, business logic, and data persistence. This design enables flexibility in deployment modes (local vs remote), maintainability, and testability.

---

## Three-Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         UI Layer (GPUI)                      │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Service  │  │   Logs   │  │  Traces  │  │ Profiles │   │
│  │   Map    │  │   View   │  │   View   │  │   View   │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
                              │
                    Reactive State Updates
                              │
┌─────────────────────────────────────────────────────────────┐
│                     Business Logic Layer                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ OTLP Ingest  │  │Query Builder │  │  Retention   │     │
│  │   Pipeline   │  │   Service    │  │   Manager    │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└─────────────────────────────────────────────────────────────┘
                              │
                   DataFusion SQL Queries
                              │
┌─────────────────────────────────────────────────────────────┐
│                Data Layer (Parquet + DataFusion)             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
│  │ Traces   │  │   Logs   │  │ Metrics  │  │ Profiles │   │
│  │ Parquet  │  │ Parquet  │  │ Parquet  │  │ Parquet  │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
└─────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

#### UI Layer (GPUI)
**Purpose:** Presentation and user interaction

**Responsibilities:**
- Render all UI components (service list, logs, traces, metrics, profiles)
- Handle user input (clicks, scrolling, filtering, searching)
- Manage local UI state (selected items, expanded rows, etc.)
- Trigger data fetches via reactive updates
- Display loading states and errors

**Technology:** GPUI (GPU-accelerated UI framework in Rust)

**Key Components:**
- `AppWindow` - Main application window
- `ServiceNavigator` - Service list with name filtering
- `TabList` - Navigation between views
- `FilterBar` - Time range and query filters
- View components: `LogsView`, `MetricsView`, `TracesView`, `ProfilesView`

See [UI Design](ui-design.md) for detailed component hierarchy.

---

#### Business Logic Layer
**Purpose:** Application logic and data transformation

**Responsibilities:**
- OTLP protocol handling (gRPC, HTTP)
- Data parsing and enrichment
- Query construction and execution
- Data retention management
- Service discovery and mapping

**Technology:** Rust with async/await (Tokio runtime)

**Key Components:**
- `IngestionPipeline` - Parses OTLP protobuf, enriches data, stores in database
- `QueryBuilder` - Constructs SQL queries from user filters
- `RetentionManager` - Automatic cleanup of old data

See [OTLP Ingestion](otlp-ingestion.md) for ingestion details.
See [Retention](retention.md) for retention management.

---

#### Data Layer (Parquet + DataFusion)
**Purpose:** Persistent storage and querying

**Responsibilities:**
- Store all telemetry data (traces, logs, metrics, profiles)
- Provide fast indexed lookups via Parquet bloom filters or optional RocksDB
- SQL queries via DataFusion
- Two-tier architecture (hot in-memory + cold Parquet)
- Data retention via file deletion

**Technology:**
- Hot tier: Papaya lock-free HashMap (in-memory, async-friendly)
- Cold tier: Apache Parquet via object_store
- Querying: Apache DataFusion (SQL engine)
- Indexes: Parquet built-in bloom filters (default) or optional RocksDB (high query volume)

**Storage:**
- `traces/` - Parquet files with traces and spans
- `logs/` - Parquet files with log entries
- `metrics/` - Parquet files with metric data points
- `profiles/` - Parquet files with profile data (pprof format)

See [Database](database.md) for storage architecture.
See [Parquet Schema](parquet-schema.md) for Arrow schemas.
See [Data Models](data-models.md) for data structures.

---

## Component Communication

### UI → Client Communication
- **Interface:** Via trait bounds (`QueryApi`, `ManagementApi`)
- **Pattern:** Async method calls returning `Result<T>`
- **Error Handling:** Errors propagated to UI for display

Example:
```rust
// UI calls QueryApi to fetch traces
let traces = query_client.query_traces(query).await?;
```

### Client → Storage Communication

**Client Types:**
- **QueryClient** - Implements `QueryApi` for trace/log/metric queries
- **ManagementClient** - Implements `ManagementApi` for admin operations

**Communication Modes:**
- **Local Mode:** Direct method calls to `TieredStorage` (implements all three traits)
- **Remote Mode:** HTTP requests to daemon's Query/Management servers

**Abstraction:** Same trait interface regardless of transport

Example:
```rust
// Local mode - direct storage access
impl QueryApi for TieredStorage {
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>> {
        // Check hot tier, then query Parquet via DataFusion
    }
}

// Remote mode - HTTP call to QueryServer
impl QueryApi for QueryClient {
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>> {
        // HTTP GET to http://daemon:8080/api/traces
    }
}

// Remote mode - HTTP call to ManagementServer
impl ManagementApi for ManagementClient {
    async fn get_storage_stats(&self) -> Result<StorageStats> {
        // HTTP GET to http://daemon:8081/api/storage/stats
    }
}
```

See [Workspace & Crates](workspace-and-crates.md) for separated server/client architecture.

### OTLP Endpoints → Storage
- **Transport:** Async channels (`tokio::sync::mpsc`)
- **Pattern:** Non-blocking ingestion
- **Flow:** OTLP handler → channel → background worker → database

Example:
```rust
// OTLP handler sends to channel
self.tx.send(IngestionEvent::Traces(traces)).await?;

// Background worker consumes from channel
while let Some(event) = rx.recv().await {
    storage.insert_traces(event).await?;
}
```

**Benefits:**
- Non-blocking: OTLP endpoint responds immediately
- Batching: Worker can batch multiple events
- Backpressure: Channel provides natural flow control

See [OTLP Ingestion](otlp-ingestion.md) for implementation details.

### Data Updates → UI
- **Pattern:** GPUI reactive updates via `cx.notify()`
- **Flow:** Data change → `cx.notify()` → UI re-renders
- **Efficiency:** Only changed components re-render

Example:
```rust
impl AppState {
    pub fn refresh_data(&mut self, cx: &mut ModelContext<Self>) {
        cx.spawn(|this, mut cx| async move {
            let data = fetch_data().await?;

            this.update(&mut cx, |state, cx| {
                state.update_data(data);
                cx.notify(); // Triggers UI re-render
            })?;

            Ok(())
        }).detach();
    }
}
```

See [State Management](state-management.md) for details.

---

## Trait Architecture

Sequins uses **six traits** to separate concerns between external APIs and internal storage operations.

### External API Traits

These traits define the public interface and are implemented by both `Storage` (local mode) and remote clients (enterprise mode).

#### 1. OtlpIngest Trait

**Purpose:** Ingest telemetry data from OTLP endpoints
**Location:** `crates/sequins-core/src/traits/ingest.rs`
**Implementors:** `Storage` only (ingestion is always local)

```rust
pub trait OtlpIngest: Send + Sync {
    async fn ingest_spans(&self, spans: Vec<Span>) -> Result<()>;
    async fn ingest_logs(&self, logs: Vec<LogEntry>) -> Result<()>;
    async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<()>;
    async fn ingest_profiles(&self, profiles: Vec<Profile>) -> Result<()>;
}
```

#### 2. QueryApi Trait

**Purpose:** Read-only access to telemetry data
**Location:** `crates/sequins-core/src/traits/query.rs`
**Implementors:** `Storage` (local), `QueryClient` (remote)

```rust
pub trait QueryApi: Send + Sync {
    async fn get_services(&self) -> Result<Vec<Service>>;
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<QueryTrace>>;
    async fn get_spans(&self, trace_id: TraceId) -> Result<Vec<Span>>;
    async fn get_span(&self, trace_id: TraceId, span_id: SpanId) -> Result<Option<Span>>;
    async fn query_logs(&self, query: LogQuery) -> Result<Vec<LogEntry>>;
    async fn get_log(&self, log_id: LogId) -> Result<Option<LogEntry>>;
    async fn query_metrics(&self, query: MetricQuery) -> Result<Vec<Metric>>;
    async fn get_metric(&self, metric_id: MetricId) -> Result<Option<Metric>>;
    async fn get_profiles(&self, query: ProfileQuery) -> Result<Vec<Profile>>;
    async fn get_profile(&self, profile_id: ProfileId) -> Result<Option<Profile>>;
}
```

#### 3. ManagementApi Trait

**Purpose:** Administrative operations
**Location:** `crates/sequins-core/src/traits/management.rs`
**Implementors:** `Storage` (local), `ManagementClient` (remote)

```rust
pub trait ManagementApi: Send + Sync {
    async fn run_retention_cleanup(&self) -> Result<usize>;
    async fn update_retention_policy(&self, policy: RetentionPolicy) -> Result<()>;
    async fn get_retention_policy(&self) -> Result<RetentionPolicy>;
    async fn run_maintenance(&self) -> Result<MaintenanceStats>;
    async fn get_storage_stats(&self) -> Result<StorageStats>;
}
```

### Internal Storage Traits

These traits abstract storage tier operations and enable future distributed querying.

#### 4. StorageRead Trait

**Purpose:** Unified read interface for hot and cold tiers
**Location:** `crates/sequins-core/src/traits/storage.rs`
**Implementors:** `HotTier`, `ColdTier`, future `RemoteNode`

```rust
pub trait StorageRead: Send + Sync {
    async fn query_traces(&self, query: &TraceQuery) -> Result<Vec<QueryTrace>>;
    async fn query_logs(&self, query: &LogQuery) -> Result<Vec<LogEntry>>;
    async fn query_metrics(&self, query: &MetricQuery) -> Result<Vec<Metric>>;
    async fn query_profiles(&self, query: &ProfileQuery) -> Result<Vec<Profile>>;
    async fn get_spans(&self, trace_id: TraceId) -> Result<Vec<Span>>;
    async fn get_span(&self, trace_id: TraceId, span_id: SpanId) -> Result<Option<Span>>;
    async fn get_log(&self, log_id: LogId) -> Result<Option<LogEntry>>;
    async fn get_metric(&self, metric_id: MetricId) -> Result<Option<Metric>>;
    async fn get_profile(&self, profile_id: ProfileId) -> Result<Option<Profile>>;
}
```

#### 5. StorageWrite Trait

**Purpose:** Unified write interface for storage destinations
**Location:** `crates/sequins-core/src/traits/storage.rs`
**Implementors:** `ColdTier`, future `RemoteNode`

```rust
pub trait StorageWrite: Send + Sync {
    async fn write_spans(&self, spans: Vec<Span>) -> Result<()>;
    async fn write_logs(&self, logs: Vec<LogEntry>) -> Result<()>;
    async fn write_metrics(&self, metrics: Vec<Metric>) -> Result<()>;
    async fn write_profiles(&self, profiles: Vec<Profile>) -> Result<()>;
}
```

Note: `HotTier` doesn't implement `StorageWrite` - it uses direct insert methods instead.

#### 6. TierMetadata Trait

**Purpose:** Query routing information for storage tiers
**Location:** `crates/sequins-core/src/traits/storage.rs`
**Implementors:** `HotTier`, `ColdTier`

```rust
pub trait TierMetadata {
    fn tier_id(&self) -> &str;
    fn priority(&self) -> u8;  // Lower = higher priority
    fn covers_time_range(&self, start: Timestamp, end: Timestamp) -> bool;
}
```

**Usage:** The query aggregator uses this metadata to:
1. Query sources in priority order (hot tier first: priority 0)
2. Skip sources that don't cover the query time range
3. Stop early when limit is reached

See [workspace-and-crates.md](workspace-and-crates.md) for detailed implementation examples.

---

## Data Lifecycle Stages

Sequins manages telemetry data through distinct lifecycle stages, each optimized for different access patterns and time scales.

### Three Stages of Data

```
Ingestion → HOT (In-Memory) → COLD (Parquet) → Deletion
            5-15 minutes      Hours/Days        After Retention
            < 1ms latency     15-35ms latency   Removed
            90-95% of queries 5-10% of queries
```

### Stage 1: HOT Tier (In-Memory)

**Purpose:** Performance optimization for recent data

**Technology:**
- `papaya::HashMap` (lock-free, async-friendly)
- Stored entirely in RAM
- No persistence

**Duration:** 5-15 minutes (configurable via `data-lifecycle.hot-tier.duration-minutes`)

**Performance:**
- Query latency: < 1ms
- Cache hit rate: 90-95% (most queries are for recent data)
- Memory usage: ~512MB default, configurable

**When data enters:** Immediately upon OTLP ingestion

**When data exits:** Background flush task checks every 60 seconds (configurable via `data-lifecycle.flush-check-interval-seconds`) and moves data older than hot tier duration to cold tier

**Independent component:** Flush task (separate from retention management)

### Stage 2: COLD Tier (Parquet)

**Purpose:** Persistent queryable storage

**Technology:**
- Apache Parquet files with Zstd compression
- Stored via `object_store` (S3, MinIO, or local filesystem)
- Optional RocksDB index for faster lookups

**Duration:** From end of hot tier until retention expiration (hours to days)

**Performance:**
- Query latency: 15-35ms (diskless), 12-20ms (with RocksDB index)
- Cache hit rate: 5-10% of queries
- Storage: ~10-40x compression vs raw data

**When data enters:** After hot tier duration expires (flush from memory to Parquet)

**When data exits:** Retention cleanup task checks every 5 minutes (configurable via `data-lifecycle.retention.cleanup-interval-seconds`) and deletes Parquet files older than retention policy

**Independent component:** RetentionManager (separate from hot-cold transition)

### Stage 3: Deletion

**Purpose:** Compliance and cost management

**When it happens:** After retention period expires

**Retention periods** (configurable per data type):
- Traces: 24-168 hours typical (1-7 days)
- Logs: 24-168 hours typical
- Metrics: 168-720 hours typical (7-30 days)
- Profiles: 24-72 hours typical (1-3 days)

**Implementation:** File-based deletion (Parquet files are immutable, space reclaimed immediately)

### Time Scale Differences

**Important:** Hot-to-cold and retention operate on very different time scales:

| Stage Transition | Time Scale | Purpose | Component |
|------------------|------------|---------|-----------|
| Ingestion → Hot | Immediate | Real-time access | OTLP ingestion |
| Hot → Cold | **Minutes** (5-15) | Performance optimization | Flush task |
| Cold → Deletion | **Hours/Days** (24-720) | Compliance/cost | RetentionManager |

**Why separate components:**
- Flush needs to be fast and frequent (every 60s)
- Retention cleanup can be slower and less frequent (every 5min)
- Different failure modes and recovery strategies
- Independent configuration and tuning

### Configuration Relationship

Although managed by separate components, both are configured together in the `data-lifecycle` section for clarity:

```kdl
data-lifecycle {
    // Performance optimization (minutes scale)
    hot-tier {
        duration-minutes 15
        max-memory-mb 512
    }
    flush-check-interval seconds=60

    // Compliance/cost management (hours/days scale)
    retention {
        traces hours=168    // 7 days
        logs hours=168
        metrics hours=720   // 30 days
        profiles hours=72   // 3 days
        cleanup-interval seconds=300
    }
}
```

**Key insight:** A trace might spend:
- 15 minutes in hot tier (fast queries)
- 167 hours 45 minutes in cold tier (acceptable latency)
- Then deleted

The 96:1 time ratio (168 hours retention vs 15 minutes hot) justifies the separate implementations.

See [Configuration](configuration.md) for detailed lifecycle configuration options.
See [Database](database.md) for hot tier implementation details.
See [Retention](retention.md) for retention management details.

---

## Deployment Architectures

### Local Development Mode
```
┌──────────────────────────────────────────────┐
│   Sequins App (GPUI)                         │
│                                              │
│  ┌──────────────┐  ┌──────────────────────┐ │
│  │ UI Components│  │ OtlpServer (Embedded)│ │
│  │              │  │  - gRPC: 4317        │ │
│  │              │  │  - HTTP: 4318        │ │
│  └──────┬───────┘  └──────────┬───────────┘ │
│         │                     │             │
│         │  Direct Storage     │ OTLP        │
│         │  (QueryApi impl)    │ Ingest      │
│         │                     │             │
│  ┌──────▼─────────────────────▼───────────┐ │
│  │   TieredStorage                        │ │
│  │   ~/sequins/ (Parquet + bloom filters) │ │
│  │   Implements: OtlpIngest + QueryApi    │ │
│  └────────────────────────────────────────┘ │
└──────────────────────────────────────────────┘
            │
            │ OTLP from local services
            ▼
    [Your Node/Python/Go Apps]
```

**Characteristics:**
- All components run in single process
- Direct TieredStorage access (no network overhead for queries)
- OtlpServer embedded for receiving telemetry
- FREE for local development
- Zero configuration required

**Components:**
- **UI** - Uses TieredStorage directly via QueryApi
- **OtlpServer** - Embedded server listening on `127.0.0.1:4317/4318` (localhost only)
- **TieredStorage** - Implements both OtlpIngest and QueryApi

**Security/Monetization:**
- OtlpServer binds to `127.0.0.1` (localhost only) to prevent abuse
- Only local services on same machine can send telemetry
- Cannot be reconfigured to accept network connections

See [Deployment](deployment.md) for configuration details.

### Enterprise Cloud Mode
```
┌─────────────────────┐       ┌─────────────────────┐
│  Sequins App #1     │       │  Sequins App #2     │
│  (GPUI)             │       │  (GPUI)             │
│  ┌───────────────┐  │       │  ┌───────────────┐  │
│  │ UI Components │  │       │  │ UI Components │  │
│  ├───────────────┤  │       │  ├───────────────┤  │
│  │ QueryClient   │  │       │  │ QueryClient   │  │
│  │ ManagementCli │  │       │  │ ManagementCli │  │
│  └───────┬───────┘  │       │  └───────┬───────┘  │
└──────────┼──────────┘       └──────────┼──────────┘
           │ HTTPS :8080/:8081           │
           └──────────┬──────────────────┘
                      │
    ┌─────────────────▼─────────────────┐
    │   Sequins Daemon (Cloud/Network)  │
    │                                   │
    │  ┌─────────────────────────────┐  │
    │  │ OtlpServer                  │  │
    │  │  - gRPC: 4317               │  │
    │  │  - HTTP: 4318               │  │
    │  └────────────┬────────────────┘  │
    │               │                   │
    │  ┌────────────▼────────────────┐  │
    │  │ QueryServer (Port 8080)     │  │
    │  │  + Authentication           │  │
    │  │  + CORS                     │  │
    │  └────────────┬────────────────┘  │
    │               │                   │
    │  ┌────────────▼────────────────┐  │
    │  │ ManagementServer (Port 8081)│  │
    │  │  + Admin Authentication     │  │
    │  └────────────┬────────────────┘  │
    │               │                   │
    │  ┌────────────▼────────────────┐  │
    │  │   TieredStorage             │  │
    │  │   S3 + Parquet + RocksDB    │  │
    │  └─────────────────────────────┘  │
    │               ▲                   │
    └───────────────┼───────────────────┘
                    │ OTLP
          [Production Services]
```

**Characteristics:**
- Three independent servers running in daemon process
- Multiple apps connect via QueryClient/ManagementClient
- Centralized telemetry from production services
- PAID enterprise feature with authentication

**Components:**
- **OtlpServer** - Receives OTLP telemetry from services (binds to `0.0.0.0:4317/4318`)
- **QueryServer** - Handles queries from clients (port 8080, optional auth)
- **ManagementServer** - Admin operations (port 8081, required auth)
- **TieredStorage** - Shared storage backend implementing all three traits

**Benefits of Separation:**
- Independent lifecycle management per server
- Different auth requirements (Query: optional, Management: required)
- Can run on different ports/interfaces
- Future: Could run on different machines for specialized deployments

**Network Access:**
- OtlpServer binds to `0.0.0.0` (all interfaces) for network telemetry
- Production services send telemetry from any machine
- This is the key difference from free tier (localhost-only)

See [Deployment](deployment.md) for enterprise setup.
See [Scaling Strategy](scaling-strategy.md) for multi-node clustering.

---

## Benefits of This Architecture

### Separation of Concerns
- Each layer has single responsibility
- Easy to modify one layer without affecting others
- Clear boundaries between components

### Deployment Flexibility
- Same codebase supports local and remote modes
- Easy to switch between modes via configuration
- No code changes needed for different deployments

### Testability
- Each layer can be tested independently
- Mock implementations for trait interfaces
- Unit tests for business logic
- Integration tests for end-to-end flows

### Performance
- Async throughout (non-blocking I/O)
- Reactive UI updates (only re-render what changed)
- Efficient data access (indexed database queries)
- GPU-accelerated rendering

### Maintainability
- Clear module boundaries
- Focused responsibilities
- Easy to onboard new developers
- Well-documented interfaces

---

## Related Documentation

- **[Workspace & Crates](workspace-and-crates.md)** - Implementation of three-trait architecture
- **[Deployment](deployment.md)** - How architecture maps to deployment modes
- **[OTLP Ingestion](otlp-ingestion.md)** - Business logic layer implementation
- **[UI Design](ui-design.md)** - UI layer component hierarchy
- **[State Management](state-management.md)** - Reactive state patterns
- **[Scaling Strategy](scaling-strategy.md)** - Distributed multi-node architecture

---

**Last Updated:** 2025-01-07 (added trait architecture section with actual method signatures)
