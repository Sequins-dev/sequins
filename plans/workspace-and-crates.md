# Cargo Workspace Structure & Crate Responsibilities

[← Back to Index](INDEX.md)

**Related Documentation:** [architecture.md](architecture.md) | [deployment.md](deployment.md) | [data-models.md](data-models.md) | [retention.md](retention.md) | [module-breakdown.md](module-breakdown.md)

---

## Cargo Workspace Structure

To support future deployment scenarios (local development vs. enterprise cloud deployment), the project uses a Cargo workspace with multiple crates:

```
sequins/
├── Cargo.toml                    # Workspace root
├── PLAN.md
├── CLAUDE.md
├── crates/
│   ├── sequins-core/             # Shared types and traits
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── models/           # Data models (Service, Trace, etc.)
│   │       ├── traits/           # All trait definitions (external API + internal storage)
│   │       │   ├── mod.rs
│   │       │   ├── ingest.rs     # OtlpIngest trait
│   │       │   ├── query.rs      # QueryApi trait
│   │       │   ├── management.rs # ManagementApi trait
│   │       │   └── storage.rs    # StorageRead, StorageWrite, TierMetadata traits
│   │       └── error.rs          # Error types
│   │
│   ├── sequins-storage/          # Complete data layer
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── storage.rs        # Storage struct (implements all three API traits)
│   │       ├── hot_tier.rs       # HotTier (Papaya HashMap for recent data)
│   │       ├── cold_tier.rs      # ColdTier (Parquet + DataFusion for historical data)
│   │       ├── hot_tier_provider.rs  # DataFusion TableProvider for hot tier
│   │       ├── hot_tier_exec.rs  # DataFusion ExecutionPlan for hot tier
│   │       ├── config.rs         # Configuration types (StorageConfig, HotTierConfig, ColdTierConfig)
│   │       └── error.rs          # Storage-specific error types
│   │
│   ├── sequins-server/           # Three separate server types
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── otlp_server.rs    # OtlpServer (ports 4317/4318)
│   │       ├── query_server.rs   # QueryServer (port 8080)
│   │       ├── management_server.rs # ManagementServer (port 8081)
│   │       ├── builder.rs        # Unified SequinsServerBuilder
│   │       ├── otlp/             # OTLP protocol handlers
│   │       │   ├── mod.rs
│   │       │   ├── grpc.rs       # Receive OTLP/gRPC → OtlpIngest
│   │       │   └── http.rs       # Receive OTLP/HTTP → OtlpIngest
│   │       ├── query/            # Query API routes
│   │       │   ├── mod.rs
│   │       │   ├── traces.rs     # HTTP → QueryApi.query_traces()
│   │       │   ├── logs.rs       # HTTP → QueryApi.query_logs()
│   │       │   ├── metrics.rs    # HTTP → QueryApi.query_metrics()
│   │       │   └── profiles.rs   # HTTP → QueryApi.get_profiles()
│   │       ├── management/       # Management API routes
│   │       │   ├── mod.rs
│   │       │   ├── retention.rs  # HTTP → ManagementApi retention methods
│   │       │   └── config.rs     # HTTP → ManagementApi config methods
│   │       └── auth.rs           # Authentication middleware
│   │
│   ├── sequins-client/           # Two separate client types
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── query_client.rs   # QueryClient (implements QueryApi via HTTP)
│   │       ├── management_client.rs # ManagementClient (implements ManagementApi via HTTP)
│   │       └── config.rs         # Client configuration
│   │
│   ├── sequins-app/              # GPUI desktop application
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── main.rs
│   │       ├── lib.rs
│   │       └── ui/               # All UI components
│   │           ├── mod.rs
│   │           ├── app.rs
│   │           ├── sidebar.rs
│   │           ├── logs/
│   │           ├── metrics/
│   │           ├── traces/
│   │           └── profiles/
│   │
│   ├── sequins-web/              # Web UI (WASM alternative to desktop)
│   │   ├── Cargo.toml
│   │   └── src/
│   │       ├── lib.rs
│   │       └── components/       # Leptos components
│   │           ├── mod.rs
│   │           ├── app.rs
│   │           ├── sidebar.rs
│   │           └── views/
│   │
│   └── sequins-daemon/           # Enterprise daemon (ingest + query)
│       ├── Cargo.toml
│       └── src/
│           ├── main.rs
│           ├── auth.rs           # Authentication/authorization
│           └── config.rs         # Configuration file support
│
└── tests/                        # Integration tests
```

## Workspace Crate Responsibilities

### `sequins-core`
**Purpose:** Shared types, traits, and interfaces used across all crates.

**Dependencies:** Minimal (serde, chrono, opentelemetry)

**Exports:**
- Data models: `Service`, `Trace`, `Span`, `LogEntry`, `Metric`, `Profile`
- Query types: `TraceQuery`, `LogQuery`, `MetricQuery`, `ProfileQuery`
- Error types: `SequinsError`, `Result<T>`
- Configuration types: `RetentionPolicy`, `ServerConfig`

**Note:** All trait definitions are in `sequins-core/src/traits/` for dependency management:
- External API traits: `OtlpIngest`, `QueryApi`, `ManagementApi`
- Internal storage traits: `StorageRead`, `StorageWrite`, `TierMetadata`

This crate also contains the shared data models and error types that all other crates depend on.

### `sequins-storage`
**Purpose:** Complete data layer - owns the entire telemetry data lifecycle.

**Dependencies:**
- `datafusion` - SQL query engine for Parquet files
- `arrow` - columnar in-memory data format
- `parquet` - columnar storage format with compression
- `object_store` - universal blob storage abstraction (LocalFileSystem, S3, MinIO)
- `rocksdb` - optional embedded key-value index (when disk available)
- `sequins-core` - shared types and models
- `tokio` - async runtime
- `opentelemetry` - OTLP types (TraceId, SpanId)
- `opentelemetry-proto` - OTLP protobuf definitions
- `bincode` - binary serialization for indexes
- `papaya` - lock-free concurrent hashmap for hot tier (async-friendly)

**Exports:**
- `Storage` - implements all three external API traits (OtlpIngest, QueryApi, ManagementApi)
- `HotTier` - lock-free Papaya HashMap for recent data (< 15 minutes)
- `ColdTier` - Parquet + DataFusion for historical data
- `HotTierTableProvider` - DataFusion integration for unified hot+cold queries
- `StorageConfig` - configuration types for storage tiers
- `StorageStats`, `EvictionStats`, `MaintenanceStats` - operational metrics

**Storage Architecture (Two-Tier):**
- **Tier 1 (Hot):** In-memory `papaya::HashMap` (lock-free) - last 5-15 minutes, < 1ms queries, 90-95% hit rate
- **Tier 2 (Cold):** `object_store` Parquet batches - all older data, 15-35ms queries, 5-10% hit rate
  - Default: Parquet with built-in indexes (bloom filters, column stats, min/max)
  - Optional: RocksDB index on local disk for faster trace_id lookups
  - Local: `LocalFileSystem` → `/var/lib/sequins/batches/`
  - Cloud: `AmazonS3` / MinIO → `s3://bucket/batches/`
  - Same API for both via `Arc<dyn ObjectStore>`

**Key Concept:** This layer abstracts "where is data, how long is it kept, and where does it come from" through three focused traits that separate ingestion, querying, and management concerns. The `object_store` crate provides a universal abstraction for blob storage, enabling config-driven switching between local and cloud backends. DataFusion provides SQL query engine for Parquet files with automatic optimizations (predicate pushdown, partition pruning, bloom filter utilization).

**See Also:**
- [object-store-integration.md](object-store-integration.md) - Index patterns and Parquet storage
- [parquet-schema.md](parquet-schema.md) - Arrow schema definitions
- [database.md](database.md) - Query patterns with DataFusion
- [technology-decisions.md](technology-decisions.md) - Why DataFusion + Parquet

## Three-Trait Architecture: Separation of Concerns

Sequins splits storage operations into **three focused traits** based on distinct responsibilities. All use **generics with trait bounds** for zero-cost abstractions and optimal performance.

### 1. OtlpIngest Trait (Ingestion)

Handles OTLP protocol ingestion. Only implemented by local storage, not remote clients.

```rust
// sequins-storage/src/ingest_trait.rs
pub trait OtlpIngest: Send + Sync {
    async fn ingest_traces(&self, request: ExportTraceServiceRequest) -> Result<()>;
    async fn ingest_logs(&self, request: ExportLogsServiceRequest) -> Result<()>;
    async fn ingest_metrics(&self, request: ExportMetricsServiceRequest) -> Result<()>;
}
```

**Implementations:** `Storage`
**Used by:** OTLP servers (ports 4317/4318)
**Remote:** RemoteClient does NOT implement (OTLP goes directly to daemon)

### 2. QueryApi Trait (Data Access)

Provides read-only access to telemetry data. Implemented by both local and remote.

```rust
// sequins-storage/src/query_trait.rs
use opentelemetry::trace::TraceId;

pub trait QueryApi: Send + Sync {
    async fn get_services(&self) -> Result<Vec<Service>>;
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>>;
    async fn get_spans(&self, trace_id: TraceId) -> Result<Vec<Span>>;
    async fn query_logs(&self, query: LogQuery) -> Result<Vec<LogEntry>>;
    async fn query_metrics(&self, query: MetricQuery) -> Result<Vec<MetricDataPoint>>;
    async fn get_profiles(&self, query: ProfileQuery) -> Result<Vec<Profile>>;
}
```

**Implementations:** `Storage`, `RemoteClient`
**Used by:** App UI, Query API server
**Remote:** Always available

### 3. ManagementApi Trait (Admin Operations)

Administrative operations for system management. Requires elevated permissions.

```rust
// sequins-storage/src/management_trait.rs
pub trait ManagementApi: Send + Sync {
    // Retention management
    async fn run_retention_cleanup(&self) -> Result<usize>;
    async fn update_retention_policy(&self, policy: RetentionPolicy) -> Result<()>;
    async fn get_retention_policy(&self) -> Result<RetentionPolicy>;

    // Database maintenance
    async fn vacuum_database(&self) -> Result<()>;
    async fn get_storage_stats(&self) -> Result<StorageStats>;
    async fn optimize_indexes(&self) -> Result<()>;

    // Configuration
    async fn get_config(&self) -> Result<ServerConfig>;
    async fn update_config(&self, config: ServerConfig) -> Result<()>;

    // User & auth management (future)
    async fn create_user(&self, user: User) -> Result<UserId>;
    async fn list_users(&self) -> Result<Vec<User>>;
    async fn delete_user(&self, user_id: UserId) -> Result<()>;
    async fn create_api_token(&self, user_id: UserId, name: String) -> Result<ApiToken>;
    async fn revoke_api_token(&self, token_id: TokenId) -> Result<()>;
}
```

**Implementations:** `Storage` (always), `RemoteClient` (if admin)
**Used by:** Settings UI, Admin CLI, Management endpoints
**Remote:** Optional (depends on user permissions)

## Benefits of Three-Trait Design

- **Zero-cost abstraction** - generics compile to static dispatch, no vtable overhead
- **Clear separation of concerns** - each trait has single purpose
- **Type safety** - can't accidentally expose wrong endpoints on wrong ports
- **Flexible deployment** - OTLP on standard ports, Query+Management on separate port
- **Better access control** - management endpoints easily restricted via auth middleware
- **Future-proof** - easy to add new operations to appropriate trait
- **Testability** - mock individual traits independently
- **Performance** - compiler can inline and optimize across trait boundaries

## Storage Implements All Three Traits

```rust
// sequins-storage/src/lib.rs
use crate::retention::{RetentionManager, RetentionPolicy};
use papaya::HashMap;
use datafusion::prelude::*;

pub struct Storage {
    // Hot tier: in-memory lock-free hashmap (async-friendly)
    hot: Arc<HashMap<TraceId, Trace>>,

    // Cold tier: Parquet via object_store (trait object for flexibility)
    object_store: Arc<dyn ObjectStore>,
    datafusion_ctx: SessionContext,

    // Optional RocksDB index for faster lookups (enum for static dispatch)
    index: Option<RocksDbIndex>,

    // Retention management
    retention_manager: Arc<Mutex<RetentionManager>>,
}

impl Storage {
    /// Create new storage with custom retention policy and optional index
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        index: Option<RocksDbIndex>,
        policy: RetentionPolicy,
    ) -> Result<Self> {
        // Initialize hot tier (in-memory lock-free hashmap)
        let hot = Arc::new(HashMap::new());

        // Initialize cold tier (Parquet + DataFusion)
        let datafusion_ctx = SessionContext::new();

        // Create retention manager but don't start it yet
        let retention_manager = Arc::new(Mutex::new(
            RetentionManager::new(object_store.clone(), policy)
        ));

        Ok(Self {
            hot,
            object_store,
            datafusion_ctx,
            index,
            retention_manager,
        })
    }

    /// Create with default retention policy (24h for all data types) and no index
    pub fn with_defaults(object_store: Arc<dyn ObjectStore>) -> Result<Self> {
        Self::new(object_store, None, RetentionPolicy::default())
    }

    /// Start the retention manager background task
    /// Call this after construction to enable automatic cleanup
    pub fn start_retention(&self) {
        let mut manager = self.retention_manager.lock().unwrap();
        manager.start();
    }

    /// Stop retention manager gracefully (call before dropping)
    pub async fn shutdown(&self) {
        let mut manager = self.retention_manager.lock().unwrap();
        manager.stop().await;
    }

    /// Async flush hot tier to Parquet (called every 5-15 minutes)
    async fn flush_hot_to_parquet(&self) -> Result<()> {
        // 1. Drain hot tier using Papaya's pin_owned guard (async-safe)
        let guard = self.hot.pin_owned();
        let mut traces = Vec::new();

        for (id, trace) in guard.iter() {
            traces.push((*id, trace.clone()));
        }

        if traces.is_empty() {
            return Ok(());
        }

        // 2. Convert to RecordBatch
        let trace_values: Vec<_> = traces.iter().map(|(_, t)| t.clone()).collect();
        let batch = traces_to_record_batch(&trace_values)?;

        // 3. Write to Parquet (can hold guard across .await - no deadlock risk!)
        let hour_path = format!("traces/{}/", current_hour_bucket());
        let file_path = format!("{}/batch-{}.parquet.zst", hour_path, uuid::Uuid::new_v4());
        write_parquet_with_index(&self.object_store, &self.index, &file_path, batch).await?;

        // 4. Clear hot tier (still using same guard)
        for (trace_id, _) in &traces {
            guard.remove(trace_id);
        }

        Ok(())
    }
}

// Storage implements OtlpIngest
impl OtlpIngest for Storage {
    async fn ingest_traces(&self, request: ExportTraceServiceRequest) -> Result<()> {
        // Parse protobuf, extract services, enrich spans
        let traces = self.parse_and_enrich_traces(request)?;

        // Store in hot tier using Papaya's lock-free API (async-safe)
        let guard = self.hot.pin_owned();
        for trace in traces {
            guard.insert(trace.trace_id, trace);
        }
        // Guard can be safely held across .await points if needed

        // Background task will flush to Parquet asynchronously
        Ok(())
    }

    async fn ingest_logs(&self, request: ExportLogsServiceRequest) -> Result<()> {
        // Parse logs
        let logs = self.parse_logs(request)?;

        // Store in hot tier (same lock-free pattern as traces)
        let guard = self.logs_hot.pin_owned();
        for log in logs {
            guard.insert(log.log_id, log);
        }

        // Background flush to Parquet
        Ok(())
    }

    async fn ingest_metrics(&self, request: ExportMetricsServiceRequest) -> Result<()> {
        // Parse metrics
        let metrics = self.parse_metrics(request)?;

        // Store in hot tier (same lock-free pattern)
        let guard = self.metrics_hot.pin_owned();
        for metric in metrics {
            guard.insert(metric.metric_id, metric);
        }

        // Background flush to Parquet
        Ok(())
    }
}

// Storage implements QueryApi
impl QueryApi for Storage {
    async fn get_services(&self) -> Result<Vec<Service>> {
        // Register Parquet files with DataFusion
        self.datafusion_ctx.register_parquet(
            "traces",
            "traces/",
            ParquetReadOptions::default()
        ).await?;

        // Query distinct services
        let df = self.datafusion_ctx.sql(
            "SELECT DISTINCT service_name FROM traces ORDER BY service_name"
        ).await?;

        let batches = df.collect().await?;
        Ok(record_batches_to_services(batches)?)
    }

    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>> {
        let mut results = Vec::new();

        // 1. Check hot tier first (in-memory, < 1ms) - using lock-free Papaya
        let guard = self.hot.pin_owned();
        for (_, trace) in guard.iter() {
            if query.matches(trace) {
                results.push(trace.clone());
            }
        }
        // Safe to hold guard across .await - no deadlock risk!

        // 2. If time range extends beyond hot tier, query cold (Parquet)
        if query.needs_cold_tier() {
            self.datafusion_ctx.register_parquet(
                "traces",
                "traces/",
                ParquetReadOptions::default()
            ).await?;

            let sql = query.to_sql(); // Generate SQL from query parameters
            let df = self.datafusion_ctx.sql(&sql).await?;
            let batches = df.collect().await?;
            let cold_results = record_batches_to_traces(batches)?;
            results.extend(cold_results);
        }

        Ok(results)
    }

    async fn get_spans(&self, trace_id: TraceId) -> Result<Vec<Span>> {
        // 1. Check hot tier using lock-free API
        let guard = self.hot.pin_owned();
        if let Some(trace) = guard.get(&trace_id) {
            return Ok(trace.spans.clone());
        }

        // 2. Use index to locate trace in Parquet
        let location = self.index.lookup(trace_id).await?
            .ok_or_else(|| Error::TraceNotFound(trace_id))?;

        // 3. Read from Parquet
        let file = self.object_store.get(&location.file_path).await?;
        let reader = ParquetRecordBatchReader::try_new(file, 1024)?;
        reader.skip_to_row_group(location.row_group)?;

        for batch in reader {
            let spans = record_batch_to_spans(batch?)?;
            if let Some(trace_spans) = spans.into_iter().find(|s| s.trace_id == trace_id) {
                return Ok(trace_spans);
            }
        }

        Err(Error::TraceNotFound(trace_id))
    }

    async fn query_logs(&self, query: LogQuery) -> Result<Vec<LogEntry>> {
        // Similar two-tier pattern: hot tier + DataFusion query
        self.datafusion_ctx.register_parquet(
            "logs",
            "logs/",
            ParquetReadOptions::default()
        ).await?;

        let sql = query.to_sql();
        let df = self.datafusion_ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        Ok(record_batches_to_logs(batches)?)
    }

    async fn query_metrics(&self, query: MetricQuery) -> Result<Vec<MetricDataPoint>> {
        // Similar pattern for metrics
        self.datafusion_ctx.register_parquet(
            "metrics",
            "metrics/",
            ParquetReadOptions::default()
        ).await?;

        let sql = query.to_sql();
        let df = self.datafusion_ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        Ok(record_batches_to_metrics(batches)?)
    }

    async fn get_profiles(&self, query: ProfileQuery) -> Result<Vec<Profile>> {
        // Similar pattern for profiles
        self.datafusion_ctx.register_parquet(
            "profiles",
            "profiles/",
            ParquetReadOptions::default()
        ).await?;

        let sql = query.to_sql();
        let df = self.datafusion_ctx.sql(&sql).await?;
        let batches = df.collect().await?;
        Ok(record_batches_to_profiles(batches)?)
    }
}

// Storage implements ManagementApi
impl ManagementApi for Storage {
    async fn run_retention_cleanup(&self) -> Result<usize> {
        // Delegate to retention manager (deletes old Parquet files)
        let manager = self.retention_manager.lock().unwrap();
        manager.cleanup_now().await
    }

    async fn update_retention_policy(&self, policy: RetentionPolicy) -> Result<()> {
        let mut manager = self.retention_manager.lock().unwrap();
        manager.update_policy(policy);
        Ok(())
    }

    async fn get_retention_policy(&self) -> Result<RetentionPolicy> {
        let manager = self.retention_manager.lock().unwrap();
        Ok(manager.policy().clone())
    }

    async fn compact_parquet(&self, hour_path: &str) -> Result<()> {
        // Merge small Parquet files in hour bucket into larger files
        // 1. List all Parquet files in hour
        let files = list_parquet_files(&self.object_store, hour_path).await?;

        if files.len() < 10 {
            return Ok(()); // Not worth compacting
        }

        // 2. Read all RecordBatches
        let mut all_batches = Vec::new();
        for file in &files {
            let batches = read_parquet(&self.object_store, file).await?;
            all_batches.extend(batches);
        }

        // 3. Write merged file
        let merged_path = format!("{}/compacted-{}.parquet.zst", hour_path, uuid::Uuid::new_v4());
        write_parquet_with_index(&self.object_store, &self.index, &merged_path, all_batches).await?;

        // 4. Delete old files
        for file in files {
            self.object_store.delete(&file).await?;
        }

        Ok(())
    }

    async fn get_storage_stats(&self) -> Result<StorageStats> {
        // Count files, calculate sizes, report hot tier memory usage
        let mut stats = StorageStats::default();

        // Hot tier stats (using lock-free Papaya API)
        let guard = self.hot.pin_owned();
        stats.hot_tier_size = guard.len();
        stats.hot_tier_memory_bytes = estimate_memory_usage(&guard);

        // Cold tier stats (scan object store)
        let mut file_count = 0;
        let mut total_bytes = 0;

        let prefix = "traces/".into();
        let mut stream = self.object_store.list(Some(&prefix)).await?;

        while let Some(meta) = stream.next().await.transpose()? {
            file_count += 1;
            total_bytes += meta.size;
        }

        stats.cold_tier_files = file_count;
        stats.cold_tier_bytes = total_bytes;

        Ok(stats)
    }

    async fn optimize_indexes(&self) -> Result<()> {
        // If using RocksDB, run compaction
        self.index.compact().await?;
        Ok(())
    }

    async fn get_config(&self) -> Result<ServerConfig> {
        // Get server configuration (from config file or env)
        Ok(ServerConfig::default())
    }

    async fn update_config(&self, config: ServerConfig) -> Result<()> {
        // Update server configuration
        Ok(())
    }

    async fn create_user(&self, user: User) -> Result<UserId> {
        // Create new user (future - auth/RBAC)
        Ok(UserId::default())
    }

    async fn list_users(&self) -> Result<Vec<User>> {
        // List all users (future - auth/RBAC)
        Ok(vec![])
    }

    async fn delete_user(&self, user_id: UserId) -> Result<()> {
        // Delete user (future - auth/RBAC)
        Ok(())
    }

    async fn create_api_token(&self, user_id: UserId, name: String) -> Result<ApiToken> {
        // Create API token (future - auth)
        Ok(ApiToken::default())
    }

    async fn revoke_api_token(&self, token_id: TokenId) -> Result<()> {
        // Revoke API token (future - auth)
        Ok(())
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        // Note: Can't .await in Drop, but RetentionManager's Drop will send shutdown signal
        // For graceful shutdown, call .shutdown().await before dropping
    }
}
```

### `sequins-server`
**Purpose:** Three separate server types, one per storage trait. Each server runs independently on its own port.

**Dependencies:** sequins-core, sequins-storage, tonic, axum, prost, tokio

**Exports:**
- `OtlpServer` - Dedicated server for OTLP ingestion (ports 4317/4318)
- `QueryServer` - Dedicated server for Query API (port 8080)
- `ManagementServer` - Dedicated server for Management API (port 8081)
- `SequinsServerBuilder` - Unified builder for composing all servers together
- Authentication middleware

**Key Features:**
- Each server wraps one trait, runs on separate ports
- Independent lifecycle management (start/stop individually)
- Better type safety (management auth required at construction)
- Flexible deployment (run only needed servers)

**Examples:**

```rust
// sequins-server/src/otlp_server.rs - Dedicated OTLP server
use std::net::SocketAddr;

pub struct OtlpServer {
    grpc_addr: Option<SocketAddr>,
    http_addr: Option<SocketAddr>,
}

impl OtlpServer {
    /// Create new OtlpServer with no endpoints configured
    pub fn new() -> Self {
        Self {
            grpc_addr: None,
            http_addr: None,
        }
    }

    /// Enable gRPC endpoint on given address
    pub fn with_grpc(mut self, addr: SocketAddr) -> Self {
        self.grpc_addr = Some(addr);
        self
    }

    /// Enable HTTP endpoint on given address
    pub fn with_http(mut self, addr: SocketAddr) -> Self {
        self.http_addr = Some(addr);
        self
    }

    pub async fn start<I>(self, ingest: Arc<I>) -> Result<OtlpServerHandle>
    where
        I: OtlpIngest + 'static,
    {
        let mut handles = vec![];
        if let Some(addr) = self.grpc_addr {
            handles.push(tokio::spawn(start_otlp_grpc(addr, ingest.clone())));
        }
        if let Some(addr) = self.http_addr {
            handles.push(tokio::spawn(start_otlp_http(addr, ingest)));
        }
        Ok(OtlpServerHandle { handles })
    }
}

// sequins-server/src/query_server.rs - Dedicated Query API server
pub struct QueryServer {
    port: u16,
    auth: Option<AuthConfig>,
}

impl QueryServer {
    pub fn new(port: u16) -> Self {
        Self { port, auth: None }
    }

    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.auth = Some(auth);
        self
    }

    pub async fn start<Q>(self, query: Arc<Q>) -> Result<QueryServerHandle>
    where
        Q: QueryApi + 'static,
    {
        let app = build_query_routes(query, self.auth);
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", self.port)).await?;
        let handle = tokio::spawn(axum::serve(listener, app));
        Ok(QueryServerHandle { handle, port: self.port })
    }
}

// sequins-server/src/management_server.rs - Dedicated Management API server
pub struct ManagementServer {
    port: u16,
    auth: AuthConfig, // Required
}

impl ManagementServer {
    pub fn new(port: u16, auth: AuthConfig) -> Self {
        Self { port, auth }
    }

    pub async fn start<M>(self, management: Arc<M>) -> Result<ManagementServerHandle>
    where
        M: ManagementApi + 'static,
    {
        let app = build_management_routes(management, self.auth);
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", self.port)).await?;
        let handle = tokio::spawn(axum::serve(listener, app));
        Ok(ManagementServerHandle { handle, port: self.port })
    }
}

// sequins-server/src/builder.rs - Unified builder for convenience
pub struct SequinsServerBuilder<I, Q, M>
where
    I: OtlpIngest + 'static,
    Q: QueryApi + 'static,
    M: ManagementApi + 'static,
{
    ingest: Option<Arc<I>>,
    query: Option<Arc<Q>>,
    management: Option<Arc<M>>,
    otlp_server: Option<OtlpServer>,
    query_server: Option<QueryServer>,
    management_server: Option<ManagementServer>,
}

impl<I, Q, M> SequinsServerBuilder<I, Q, M>
where
    I: OtlpIngest + 'static,
    Q: QueryApi + 'static,
    M: ManagementApi + 'static,
{
    pub fn new() -> Self {
        Self {
            ingest: None,
            query: None,
            management: None,
            otlp_server: None,
            query_server: None,
            management_server: None,
        }
    }

    // Simple: Use same storage for all traits
    pub fn with_storage<S>(mut self, storage: Arc<S>) -> Self
    where
        S: OtlpIngest + QueryApi + ManagementApi + 'static,
    {
        self.ingest = Some(storage.clone());
        self.query = Some(storage.clone());
        self.management = Some(storage);
        self
    }

    // Advanced: Configure each server separately
    pub fn with_otlp_server(mut self, server: OtlpServer) -> Self {
        self.otlp_server = Some(server);
        self
    }

    pub fn with_query_server(mut self, server: QueryServer) -> Self {
        self.query_server = Some(server);
        self
    }

    pub fn with_management_server(mut self, server: ManagementServer) -> Self {
        self.management_server = Some(server);
        self
    }

    pub async fn start(self) -> Result<SequinsServerHandle> {
        let otlp = match (self.otlp_server, self.ingest) {
            (Some(s), Some(i)) => Some(s.start(i).await?),
            _ => None,
        };

        let query = match (self.query_server, self.query) {
            (Some(s), Some(q)) => Some(s.start(q).await?),
            _ => None,
        };

        let management = match (self.management_server, self.management) {
            (Some(s), Some(m)) => Some(s.start(m).await?),
            _ => None,
        };

        Ok(SequinsServerHandle { otlp, query, management })
    }
}
```

### `sequins-client`
**Purpose:** Two separate client types that implement QueryApi and ManagementApi via HTTP. Used ONLY for remote connections.

**Dependencies:** sequins-core, reqwest, serde_json

**Exports:**
- `QueryClient` - implements `QueryApi` trait via HTTP calls to QueryServer
- `ManagementClient` - implements `ManagementApi` trait via HTTP calls to ManagementServer

**Key Concept:** In **local mode**, the app uses `Storage` directly (no client). In **remote mode**, the app uses separate `QueryClient` and `ManagementClient` which implement the same traits but make HTTP calls. The app code is identical in both modes - it just calls trait methods.

**Architecture Flow:**
```
LOCAL MODE (no clients):
  App holds Arc<Storage>
  └─> App → Storage (implements QueryApi + ManagementApi) → Direct storage access

REMOTE MODE (separate clients):
  App holds Arc<QueryClient> + Arc<ManagementClient>
  ├─> App → QueryClient (implements QueryApi) → HTTP to QueryServer (port 8080)
  └─> App → ManagementClient (implements ManagementApi) → HTTP to ManagementServer (port 8081)
      └─> Daemon servers → Storage → Direct storage access

Key insight: App code is identical in both modes. It just calls QueryApi/ManagementApi methods.
The implementation (Storage vs Clients) determines if it's local or remote.
```

**Examples:**

```rust
// sequins-client/src/query_client.rs - Implements QueryApi via HTTP
use sequins_storage::QueryApi;
use sequins_core::*;

pub struct QueryClient {
    client: reqwest::Client,
    base_url: String,
    auth_token: Option<String>, // Optional - some deployments allow unauthenticated queries
}

impl QueryClient {
    pub fn new(endpoint: String) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: endpoint,
            auth_token: None,
        })
    }

    pub fn with_auth(mut self, token: String) -> Self {
        self.auth_token = Some(token);
        self
    }
}

impl QueryApi for QueryClient {
    async fn get_services(&self) -> Result<Vec<Service>> {
        let url = format!("{}/api/query/services", self.base_url);
        let mut request = self.client.get(&url);

        // Add auth header if configured
        if let Some(token) = &self.auth_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await?;
        Ok(response.json::<Vec<Service>>().await?)
    }

    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>> {
        let url = format!("{}/api/query/traces", self.base_url);
        let mut request = self.client.post(&url).json(&query);

        if let Some(token) = &self.auth_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        let response = request.send().await?;
        Ok(response.json::<Vec<Trace>>().await?)
    }

    // ... all other QueryApi methods follow same pattern
}

// sequins-client/src/management_client.rs - Implements ManagementApi via HTTP
use sequins_storage::ManagementApi;
use sequins_core::*;

pub struct ManagementClient {
    client: reqwest::Client,
    base_url: String,
    auth_token: String, // Required - management always needs auth
}

impl ManagementClient {
    pub fn new(endpoint: String, auth_token: String) -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: endpoint,
            auth_token,
        })
    }
}

impl ManagementApi for ManagementClient {
    async fn run_retention_cleanup(&self) -> Result<usize> {
        let url = format!("{}/api/management/retention/cleanup", self.base_url);
        let response = self.client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .send()
            .await?;

        let result: CleanupResponse = response.json().await?;
        Ok(result.deleted_count)
    }

    async fn update_retention_policy(&self, policy: RetentionPolicy) -> Result<()> {
        let url = format!("{}/api/management/retention/policy", self.base_url);
        self.client
            .put(&url)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .json(&policy)
            .send()
            .await?;
        Ok(())
    }

    // ... all other ManagementApi methods follow same pattern
    // All make authenticated calls to /api/management/*
}

// Note: Neither client implements OtlpIngest
// OTLP data goes directly to daemon's OtlpServer endpoints (ports 4317/4318)
```

### `sequins-app`
**Purpose:** GPUI desktop application. Embeds OTLP-only server for local development.

**Dependencies:**
- `gpui` - Desktop UI framework
- `sequins-core` - Data models and traits
- `sequins-storage` - Storage (used directly in local mode)
- `sequins-client` - QueryClient + ManagementClient (used in remote mode)
- `sequins-server` - OtlpServer (embedded in local mode)

**Features:**
- All UI components
- Application state management
- Settings UI
- Embeds OtlpServer for ingestion (FREE - for local dev)
- Can connect to remote daemon via separate clients (PAID enterprise feature)

**Local Mode:** App starts embedded OtlpServer and uses `Storage` directly (no clients needed)
**Remote Mode:** App connects to enterprise daemon via `QueryClient` + `ManagementClient`

**Example:**
```rust
// sequins-app/src/main.rs
use sequins_server::OtlpServer;
use sequins_storage::{Storage, QueryApi, ManagementApi};
use sequins_client::{QueryClient, ManagementClient};
use object_store::local::LocalFileSystem;

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;

    // Choose implementation based on mode
    let (query_api, management_api): (Arc<dyn QueryApi>, Arc<dyn ManagementApi>) =
        if let Some(remote_url) = config.remote_url {
            // REMOTE MODE: Use separate clients for each API
            let query_client = if let Some(token) = config.query_token {
                QueryClient::new(remote_url.clone())?.with_auth(token)
            } else {
                QueryClient::new(remote_url.clone())?
            };

            let management_client = ManagementClient::new(
                remote_url,
                config.admin_token.ok_or_else(|| anyhow!("Admin token required"))?,
            )?;

            // Separate Arc for each client
            (
                Arc::new(query_client) as Arc<dyn QueryApi>,
                Arc::new(management_client) as Arc<dyn ManagementApi>,
            )
        } else {
            // LOCAL MODE: Use Storage directly (no clients needed!)
            let storage_path = config.storage_path.unwrap_or_else(default_storage_path);

            // Initialize object store (LocalFileSystem for local mode)
            let object_store = Arc::new(LocalFileSystem::new_with_prefix(&storage_path)?);

            // Optional RocksDB index
            let index = if let Some(path) = config.index_path {
                Some(RocksDbIndex::new(path)?)
            } else {
                None
            };

            // Storage implements all three traits directly
            let storage = Arc::new(Storage::new(
                object_store,
                index,
                RetentionPolicy::default(),
            )?);

            // Start retention manager for automatic cleanup
            storage.start_retention();

            // Start embedded OtlpServer (uses Storage for ingest)
            // IMPORTANT: Free tier binds to 127.0.0.1 (localhost only)
            // This prevents network access - only local services can connect
            let otlp_server = OtlpServer::new()
                .with_grpc("127.0.0.1:4317".parse()?)  // Localhost only!
                .with_http("127.0.0.1:4318".parse()?)  // Localhost only!
                .start(storage.clone())
                .await?;

            // Keep server running in background
            tokio::spawn(async move {
                otlp_server.wait().await
            });

            // App uses Storage directly via trait objects - no clients!
            (
                storage.clone() as Arc<dyn QueryApi>,
                storage as Arc<dyn ManagementApi>,
            )
        };

    // Start GPUI app with trait objects
    // App code is IDENTICAL regardless of which implementation is used!
    // query_api and management_api are just trait objects that work the same way.
    // The app never knows or cares if it's talking to local storage or remote clients.
    App::new().run(|cx| {
        let state = AppState::new(query_api, management_api);
        cx.open_window(WindowOptions::default(), |cx| {
            cx.new_view(|cx| AppWindow::new(state, cx))
        });
    });

    Ok(())
}
```

### `sequins-web`
**Purpose:** Web-based UI alternative to GPUI desktop app. Provides browser-based access to telemetry data.

**Dependencies:**
- `leptos` - Reactive web framework for Rust (client-side rendering)
- `leptos_meta` - Meta tags and document head management
- `leptos_router` - Client-side routing
- `wasm-bindgen` - WebAssembly JavaScript bindings
- `console_error_panic_hook` - Better panic messages in browser console

**Features:**
- WASM-compiled Rust UI running in browser
- Client-side rendered (CSR) for fast interaction
- Same UI components as desktop app, adapted for web
- Connects to remote daemon via HTTP API (no local OTLP server)

**Target:** `wasm32-unknown-unknown`
**Build output:** WebAssembly module + JavaScript glue code

**Usage:**
- Embedded in enterprise daemon web interface
- Standalone deployment for teams preferring web access
- Alternative to GPUI for platforms without native support

**Example build:**
```bash
cd crates/sequins-web
trunk build --release  # Builds WASM + HTML + JS bundle
```

**Deployment modes:**
- Served by `sequins-daemon` at `/` (bundled web UI)
- Standalone static hosting (S3, CDN, etc.) pointing to daemon API

**Why Leptos:**
- Full Rust ecosystem (share types with backend)
- Fine-grained reactivity (fast updates)
- Small WASM bundle size (~200KB gzipped)
- SSR-ready for future server-side rendering

See [deployment.md](deployment.md) for web UI deployment scenarios.

### `sequins-daemon`
**Purpose:** Enterprise daemon with OTLP ingest + query API + management API. Paid deployment option.

**Dependencies:** sequins-core, sequins-storage, sequins-server, tokio

**Features:**
- Runs OTLP endpoints (gRPC, HTTP) - for ingesting telemetry (OtlpIngest trait)
- Runs query API - for remote app connections (QueryApi trait)
- Runs management API - for admin operations (ManagementApi trait)
- Authentication/authorization
- Multi-tenancy support (optional future feature)
- Configuration file support
- Metrics and monitoring

**Usage:** Deployed in enterprise cloud/network, multiple developers connect their apps to it

**Example:**
```rust
// sequins-daemon/src/main.rs
use sequins_server::{SequinsServerBuilder, OtlpServer, QueryServer, ManagementServer};
use sequins_storage::{Storage, RetentionPolicy};
use object_store::aws::AmazonS3;

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration from file
    let config = Config::from_file("/etc/sequins/config.kdl")?;

    // Configure retention policy per data type
    let retention_policy = RetentionPolicy {
        traces_hours: config.traces_retention_hours.unwrap_or(24),
        logs_hours: config.logs_retention_hours.unwrap_or(24),
        metrics_hours: config.metrics_retention_hours.unwrap_or(168), // 7 days
        profiles_hours: config.profiles_retention_hours.unwrap_or(24),
        cleanup_interval_secs: 300, // 5 minutes
    };

    // Initialize object store (S3 for enterprise deployment)
    let object_store = Arc::new(AmazonS3::from_env()?);

    // Optional RocksDB index for high query volume
    let index = if let Some(index_path) = config.index_path {
        Some(RocksDbIndex::new(index_path)?)
    } else {
        None // Use Parquet built-in bloom filters
    };

    // Initialize storage with custom retention policy
    // Storage implements OtlpIngest, QueryApi, and ManagementApi
    let storage = Arc::new(Storage::new(
        object_store,
        index,
        retention_policy,
    )?);

    // Start retention manager background task
    storage.start_retention();

    // Unified builder for convenience - uses same storage for all servers
    let servers = SequinsServerBuilder::new()
        .with_storage(storage)  // Single storage implements all three traits
        .with_otlp_server(
            OtlpServer::new()
                .with_grpc(format!("0.0.0.0:{}", config.otlp_grpc_port).parse()?)
                .with_http(format!("0.0.0.0:{}", config.otlp_http_port).parse()?)
        )
        .with_query_server(
            QueryServer::new()
                .with_port(config.query_api_port)
                .with_auth(config.query_auth)
        )
        .with_management_server(
            ManagementServer::new()
                .with_port(config.management_api_port)
                .with_auth(config.management_auth)
        )
        .start()
        .await?;

    info!("Sequins daemon started");
    info!("OTLP gRPC: {}", config.otlp_grpc_port);
    info!("OTLP HTTP: {}", config.otlp_http_port);
    info!("Query API: {}", config.query_api_port);
    info!("Management API: {}", config.management_api_port);

    // Wait forever
    server.wait().await?;

    Ok(())
}
```

## Benefits of This Structure

1. **Composable Servers:** Same server crate used in both app and daemon, just different configurations
2. **Clean Separation:** OTLP ingest is separate from query API, enabling free local + paid remote
3. **Zero Lock-in:** App always has embedded OTLP, developers never forced to pay
4. **Enterprise Value:** Teams get centralized telemetry with auth, making paid tier compelling
5. **No Code Duplication:** Daemon reuses all the same components, just adds query API
6. **Testability:** Each crate can be tested independently
7. **Flexibility:** App can work locally or connect to remote daemon
8. **Reusability:** Core types and client can be used by other tools

---

**Last Updated:** 2025-01-07 (updated file structure, trait locations, added sequins-web, renamed Storage)
