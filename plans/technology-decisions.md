# Technology Choices & Rationale

[← Back to Index](INDEX.md)

**Related Documentation:** [architecture.md](architecture.md) | [workspace-and-crates.md](workspace-and-crates.md)

---

## Core Technologies

### Why GPUI?

**Benefits:**
- **Performance:** GPU-accelerated rendering for smooth 60fps UI
- **Native feel:** Platform-specific controls and behavior
- **Rust-native:** Shares memory model with rest of app, no FFI overhead
- **Reactive:** Built-in state management similar to React
- **Proven:** Used in production by Zed editor

**Trade-offs:**
- Smaller community compared to Electron or web frameworks
- Less documentation and examples
- Steeper learning curve
- Fewer third-party components

**Alternatives Considered:**
- **Tauri:** Web-based, larger binary size, FFI overhead
- **egui:** Immediate mode, less polished, limited styling
- **Electron:** Large binary, memory heavy, web technologies

**Decision:** GPUI's performance, native feel, and Rust integration outweigh the learning curve.

### Why DataFusion + Parquet?

**Benefits:**
- **Unified storage:** Parquet everywhere (no format conversion between tiers)
- **Columnar efficiency:** 40x compression, fast aggregations for analytics
- **Cloud-native:** Works seamlessly with S3 via `object_store` crate
- **Diskless compatible:** No local database files required
- **Production-proven:** Powers InfluxDB 3, OpenObserve (4,500+ orgs)
- **Scales to petabytes:** Columnar compression handles massive datasets
- **SQL + DataFrame API:** Flexible query interface
- **Open standard:** Apache Parquet is widely supported

**Trade-offs:**
- Slower point queries than B-tree indexes (15-35ms vs 5-20ms)
- No built-in full-text search (requires separate solution)
- Append-mostly model (no UPDATE, difficult DELETE)
- More complex than embedded SQL database

**Alternatives Considered:**
- **SQLite/Turso:** Faster point queries, but warm tier adds architectural complexity, conflicts with diskless deployment
- **Polars:** DataFrame-focused, less natural for SQL queries
- **DuckDB:** Good OLAP engine but still file-based like SQLite
- **Custom Parquet:** Reinventing DataFusion's wheel

**Decision:** DataFusion + Parquet provides unified storage format, cloud-native architecture, and massive scale, with acceptable point query latency (15-35ms for 5-10% of queries).

### Why Two-Tier Storage (Hot + Cold)?

**Architecture:**
- **Tier 1 (Hot):** In-memory HashMap for last 5-15 minutes → < 1ms, 90-95% hit rate
- **Tier 2 (Cold):** Parquet via object_store for all older data → 15-35ms, 5-10% hit rate

**Benefits:**
- **Architectural simplicity:** Only 2 systems (HashMap + Parquet), not 3
- **Diskless deployment:** Works with S3-only storage (no local database files)
- **Local deployment:** Also works with `object_store::LocalFileSystem`
- **Unified format:** Parquet from 16 minutes to 16 years
- **Cache hit rate:** 90-95% of queries are instant (hot tier)

**Why Not Three Tiers?**
- Third tier (warm SQLite) adds complexity for 4-9% of queries
- Requires local disk (conflicts with diskless deployment goal)
- Data duplication between SQLite and Parquet
- Extra conversion overhead (SQL ↔ Parquet)

**Decision:** Two tiers provides the right balance of simplicity and performance for observability workloads.

### Why Papaya for Hot Tier?

**Benefits:**
- **Lock-free:** No thread blocking ever - operations use atomic CAS loops
- **Async-friendly:** Guards are `Send + Sync`, safe to hold across `.await` points
- **Better tail latency:** No lock queuing under contention (99.9th percentile)
- **Unlimited concurrency:** Not limited by shard count like DashMap (8-16 shards)
- **No deadlocks:** No locks = no deadlock risk
- **Tokio-optimized:** Won't block worker threads, yields gracefully
- **Read-optimized:** Multiple concurrent readers without contention
- **Production-ready:** Battle-tested, stable API, actively maintained

**Trade-offs:**
- **Clone on read:** Must clone values when retrieving (can't return references)
- **Memory overhead:** Epoch-based reclamation holds memory until safe to free
- **Complex internals:** Lock-free algorithms harder to debug (but API is simple)

**Alternatives Considered:**
- **DashMap:** Per-shard locking (8-16 shards), but blocks threads on contention
  - Problem: Uses `std::sync::RwLock` which blocks Tokio worker threads
  - Can't hold guards across `.await` (deadlock risk)
  - Limited to shard count concurrent writers (typically 16)
- **RwLock<HashMap>:** Full-map locking, only 1 writer at a time
  - Unacceptable for concurrent OTLP ingestion
- **Moka:** Lock-free cache with async API and built-in eviction
  - Great if we need TTL-based eviction
  - More complex API (async methods)
  - May add later if manual flushing becomes a bottleneck
- **flurry:** Older lock-free HashMap, less maintained
- **evmap:** Eventually-consistent, doesn't fit our consistency model

**Why Async-Friendly Matters:**

In a Tokio-based system like ours:
```rust
// Bad: DashMap blocks Tokio threads
async fn handle_otlp_request(dashmap: Arc<DashMap<TraceId, Trace>>) {
    // This blocks a Tokio worker thread if there's contention
    dashmap.insert(trace_id, trace);  // ⚠️ Thread blocking!
    // Tokio can't schedule other tasks on this thread while waiting
}

// Good: Papaya never blocks
async fn handle_otlp_request(papaya: Arc<papaya::HashMap<TraceId, Trace>>) {
    let guard = papaya.pin_owned();
    guard.insert(trace_id, trace);  // ✅ Lock-free CAS, no blocking
    // Can hold guard across .await safely
    let other_data = fetch_data().await;  // ✅ No deadlock risk
    guard.insert(other_id, other_trace);
}
```

**Performance Characteristics:**

For our workload (1000-10,000 spans/sec ingestion + queries):
- DashMap: 8-16 concurrent writers max, thread blocking on contention
- Papaya: Unlimited concurrent writers, no blocking ever

Example: 1000 concurrent operations
- DashMap (16 shards): ~62 operations per shard, 6.25% collision rate, some threads block
- Papaya: All operations proceed in parallel, retry on CAS collision (~1-2 attempts)

**Decision:** Papaya's lock-free, async-friendly design eliminates thread blocking and provides better tail latency for our Tokio-based OTLP ingestion and query workload. The clone-on-read trade-off is acceptable (traces are small, queries uncommon from hot tier).

### Why RocksDB for Index (Optional)?

**Benefits:**
- **Optional acceleration:** When disk available, improves cold tier latency (15-35ms → 12-20ms)
- **Small footprint:** 100MB index for 1M traces vs 10GB Parquet data
- **Indexes everything:** All historical data, not just warm tier
- **Battle-tested:** Used by Cassandra, MySQL, Kafka, InfluxDB IOx
- **Point query optimization:** O(log n) lookup for trace_id → Parquet location

**Trade-offs:**
- C++ dependency (FFI overhead ~5μs per operation)
- Requires local disk (not for diskless deployments)
- Additional operational complexity (compaction, backups)

**Alternatives Considered:**
- **Redb:** Pure Rust embedded DB, less mature but promising
- **Sled:** Pure Rust, but no longer maintained
- **Tantivy:** Inverted index designed for full-text search, overkill for key-value

**Decision:**
- **Default:** Parquet built-in bloom filters and column statistics (no external index)
- **Optional (high query volume):** RocksDB for 2-3x faster trace lookups

**Configuration modes:**
```kdl
// Diskless mode (default)
storage {
    backend "s3"
    bucket "sequins-traces"
    // Default: Uses Parquet bloom filters (no additional config needed)
}

// Optional: Enable RocksDB for faster queries
storage {
    backend "s3"
    bucket "sequins-traces"
    mode "disk-indexed"
    index-type "rocksdb"
    index-path "/var/lib/sequins/index"
}
```

### Why Tokio?

**Benefits:**
- **Industry standard:** De facto async runtime for Rust
- **Battle-tested:** Used in production by major companies
- **Full-featured:** Includes everything we need (HTTP, gRPC, timers)
- **Well-documented:** Extensive documentation and examples
- **Ecosystem:** Most async libraries built on Tokio

**Trade-offs:**
- Slightly more complex than simpler runtimes
- Runtime overhead (minimal)

**Alternatives Considered:**
- **async-std:** Simpler API, less ecosystem support
- **smol:** Lightweight, fewer features

**Decision:** Tokio's maturity and ecosystem make it the clear choice.

### Why Tonic (gRPC)?

**Benefits:**
- **OTLP standard:** gRPC is the primary OTLP transport
- **Efficient:** Binary protocol, HTTP/2 multiplexing
- **Type-safe:** Generated code from protobuf definitions
- **Async:** Works seamlessly with Tokio
- **Streaming:** Supports bidirectional streaming (future use)

**Trade-offs:**
- Requires HTTP/2
- More complex than plain HTTP
- Code generation step

**Alternatives Considered:**
- **grpcio:** Uses C++ gRPC, FFI overhead
- **tarpc:** Custom RPC, not OTLP-compatible

**Decision:** Tonic is the standard Rust gRPC implementation and required for OTLP compliance.

### Why Axum (HTTP)?

**Benefits:**
- **Modern:** Latest generation Rust web framework
- **Ergonomic:** Clean, composable API
- **Fast:** Built on Hyper, one of the fastest HTTP libraries
- **Tokio-native:** First-class async support
- **Type-safe:** Compile-time request/response validation
- **Middleware:** Easy to add authentication, logging, etc.

**Trade-offs:**
- Younger than Actix-web
- Smaller ecosystem

**Alternatives Considered:**
- **Actix-web:** More mature, but more complex API
- **warp:** Similar to Axum, less ergonomic
- **Rocket:** Not async-native

**Decision:** Axum's ergonomics and Tokio integration make it ideal for our HTTP endpoints.

## Supporting Libraries

### datafusion

**Purpose:** SQL query engine for Parquet
- Execute SQL queries on Parquet files
- DataFrame API for programmatic queries
- Predicate pushdown, partition pruning
- Optimized for analytical workloads

**Why:** Production-ready Parquet query engine, powers InfluxDB 3 and OpenObserve

### arrow & parquet

**Purpose:** Columnar data format
- Apache Arrow in-memory representation
- Parquet on-disk storage format
- Efficient compression and encoding
- Row group statistics and bloom filters

**Why:** Industry-standard columnar format, excellent compression, wide ecosystem support

### rocksdb

**Purpose:** Embedded key-value store (optional index)
- Index trace_id → Parquet location mappings
- O(log n) point query lookups
- Disk-based persistence

**Why:** Battle-tested at scale (Cassandra, MySQL), fast point queries

### object_store

**Purpose:** Unified blob storage API
- `LocalFileSystem` for local development
- `AmazonS3` for cloud deployments
- `MinIO` for self-hosted S3-compatible storage
- Same API for all backends

**Why:** Enables diskless deployment, cloud-native architecture

### opentelemetry & opentelemetry-proto

**Purpose:** OTLP protocol support
- Parse OTLP protobuf messages
- Standard trace/span types (`TraceId`, `SpanId`)
- OpenTelemetry semantic conventions

**Why:** Required for OTLP compliance, battle-tested implementations

### serde & serde_json

**Purpose:** Serialization/deserialization
- JSON parsing for OTLP/HTTP+JSON
- Settings persistence
- Database JSON columns

**Why:** Industry standard, excellent ergonomics

### uuid

**Purpose:** Generate unique IDs
- Service IDs
- Log IDs
- Metric IDs

**Why:** Standard UUID v4 implementation

### chrono

**Purpose:** Date/time handling
- Timestamp conversion
- Duration calculations
- Time range formatting

**Why:** Most mature Rust datetime library

### tracing

**Purpose:** Internal logging
- Debug logs
- Error tracking
- Performance monitoring

**Why:** Standard Rust logging framework, integrates with OpenTelemetry

## Build & Tooling

### Cargo

**Purpose:** Build system and package manager
- Workspace management
- Dependency resolution
- Build profiles

**Why:** Standard Rust tooling

### cargo-clippy

**Purpose:** Linting
- Catch common mistakes
- Enforce best practices
- Suggest improvements

**Why:** Essential Rust development tool

### cargo-fmt

**Purpose:** Code formatting
- Consistent style
- Automatic formatting

**Why:** Standard Rust formatter

### cargo-test

**Purpose:** Testing
- Unit tests
- Integration tests
- Documentation tests

**Why:** Built into Cargo

## Development Workflow

### IDE Support

**Recommended:** VS Code with rust-analyzer
- Excellent type checking
- Code completion
- Inline documentation
- Debugging support

**Alternative:** RustRover, Zed, Neovim with LSP

### CI/CD

**Platform:** GitHub Actions
- Automated testing
- Cross-platform builds
- Release automation

**Why:** Free for open source, excellent Rust support

## Platform Support

### macOS
- Primary development platform
- Native GPUI support
- Metal GPU acceleration

### Linux
- Full support
- Vulkan GPU acceleration

### Windows
- Planned support
- DirectX GPU acceleration

**Decision:** Start with macOS, add Linux and Windows in later phases.

---

**Last Updated:** 2025-11-05
