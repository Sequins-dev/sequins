# Storage Architecture

[← Back to Index](INDEX.md)

**Related Documentation:** [data-models.md](data-models.md) | [parquet-schema.md](parquet-schema.md) | [retention.md](retention.md) | [workspace-and-crates.md](workspace-and-crates.md) | [scaling-strategy.md](scaling-strategy.md) | [object-store-integration.md](object-store-integration.md) | [technology-decisions.md](technology-decisions.md)

---

## Storage Architecture Overview

Sequins uses a **two-tier storage architecture** optimized for observability query patterns:

### Tier 1: HOT (In-Memory)
- **Duration:** Last 5-15 minutes
- **Purpose:** Immediate ingestion, instant queries
- **Implementation:** `papaya::HashMap<TraceId, Trace>` - lock-free concurrent HashMap
- **Concurrency:** Lock-free design with epoch-based reclamation
  - No thread blocking - operations never wait for locks
  - Async-friendly - safe to hold references across `.await`
  - No deadlock risk - no locks to deadlock on
  - Excellent for concurrent OTLP ingestion + queries
- **Query latency:** < 1ms
- **Cache hit rate:** 90-95% (most queries are for recent data)
- **Not persisted:** Ephemeral buffer only

**Why Papaya?**
- **Lock-free design:** No thread blocking, ever - operations use atomic CAS loops
- **Async-friendly:** Guards are `Send + Sync`, safe to hold across `.await` points
- **Read-optimized:** Multiple concurrent readers without contention
- **Write throughput:** Unlimited concurrent writers, no shard bottleneck
- **No deadlocks:** No locks = no deadlock risk
- **Better tail latency:** Under contention, no threads blocked waiting for locks
- **API convenience:** All methods take `&self` (works with `Arc<HashMap>` without `Mutex`)

**Usage Pattern:**

```rust
use papaya::HashMap;
use std::sync::Arc;

// Create hot tier
let hot_tier = Arc::new(HashMap::<TraceId, Trace>::new());

// Insert trace (async-safe)
async fn insert_trace(
    hot_tier: Arc<HashMap<TraceId, Trace>>,
    id: TraceId,
    trace: Trace,
) {
    let guard = hot_tier.pin_owned();  // Epoch-based guard, Send + Sync
    guard.insert(id, trace);
    // Safe to hold guard across .await - no deadlock risk
}

// Query trace (async-safe)
async fn query_trace(
    hot_tier: Arc<HashMap<TraceId, Trace>>,
    id: &TraceId,
) -> Option<Trace> {
    let guard = hot_tier.pin_owned();
    guard.get(id).map(|v| v.clone())  // Need to clone (lock-free requires this)
}

// Flush to cold tier (drain hot tier)
async fn flush_hot_tier(
    hot_tier: Arc<HashMap<TraceId, Trace>>,
) -> Vec<(TraceId, Trace)> {
    let guard = hot_tier.pin_owned();
    let mut traces = Vec::new();

    for (id, trace) in guard.iter() {
        traces.push((*id, trace.clone()));
    }

    // Clear flushed traces
    for (id, _) in &traces {
        guard.remove(id);
    }

    traces
}
```

**Performance Characteristics:**

*Lock-free advantages for 1000-10,000 spans/sec ingestion:*
- **No blocking:** Operations never wait for locks, only retry CAS on contention
- **Better tail latency:** 99.9th percentile much lower (no lock queuing)
- **Async-friendly:** Won't block Tokio worker threads
- **Unlimited concurrency:** Not limited by shard count (e.g., 16 shards in DashMap)
- **Predictable performance:** No sudden spikes from lock contention

*Trade-offs:*
- **Memory overhead:** Epoch-based reclamation holds some memory until safe to free
- **Clone on read:** Must clone values (can't return references like with locks)
- **More complex internals:** Lock-free algorithms are harder to reason about

*When lock-free matters:*
- Async workloads (like ours with Tokio)
- High concurrency (>16 concurrent operations)
- Tail latency sensitive workloads
- Operations that need to hold references across `.await`

**Visual Example - Concurrent Operations:**

```
Time: T0 (no locks, all operations proceed in parallel)
Thread 1: Insert(TraceA) → CAS on bucket → Success ✅
Thread 2: Insert(TraceB) → CAS on bucket → Success ✅
Thread 3: Query(TraceC)  → Read atomic   → Success ✅
Thread 4: Insert(TraceD) → CAS on bucket → Success ✅

Result: All 4 operations proceed in parallel, no waiting

If two threads try to modify same bucket:
Thread 1: CAS attempt → Success ✅
Thread 2: CAS attempt → Retry (bucket changed) → Success ✅
  - No blocking, just retry (typically 1-2 attempts under contention)
```

**Comparison to alternatives:**

| Approach | Blocking | Async-Friendly | Concurrent Writers | Tail Latency |
|----------|----------|----------------|-------------------|--------------|
| `RwLock<HashMap>` | Yes | No (blocks threads) | 1 | Poor |
| `DashMap` (8-16 shards) | Yes | No (blocks threads) | 8-16 | Medium |
| `Papaya` (lock-free) | No | Yes (no blocking) | Unlimited | Excellent |
| `Moka` (lock-free + eviction) | No | Yes (async API) | Unlimited | Excellent |

**Alternative: Moka (if eviction needed)**

If we need automatic eviction of old traces from hot tier:

```rust
use moka::future::Cache;

// Create hot tier with TTL eviction
let hot_tier = Cache::builder()
    .max_capacity(100_000)  // Max traces in memory
    .time_to_live(Duration::from_secs(900))  // 15 minutes TTL
    .build();

// Insert (async API)
async fn insert_trace(hot_tier: Arc<Cache<TraceId, Trace>>, id: TraceId, trace: Trace) {
    hot_tier.insert(id, trace).await;
}

// Query (async API)
async fn query_trace(hot_tier: Arc<Cache<TraceId, Trace>>, id: &TraceId) -> Option<Trace> {
    hot_tier.get(id).await
}
```

**Decision:** Papaya for hot tier provides lock-free, async-friendly design perfect for Tokio-based OTLP ingestion and query workload. Use Moka if automatic eviction becomes necessary.

### Tier 2: COLD (Parquet via `object_store`)
- **Duration:** All data older than hot tier (15 min to full retention)
- **Purpose:** Queryable persistent storage
- **Implementation:** Parquet files via `object_store` crate (LocalFileSystem or S3/MinIO)
- **Storage format:** Parquet + Zstd level 3 compression
- **Query latency:** 15-35ms (diskless), 12-20ms (with disk index)
- **Cache hit rate:** 5-10% of queries
- **Durability:** S3 provides 99.999999999% durability (11 nines)

**Index Architecture:**
```
Default:
  Parquet files with built-in indexes
  • Bloom filters on trace_id, span_id, service_name
  • Column statistics (min/max per row group)
  • DataFusion automatic predicate pushdown
  • Zero operational overhead

Optional (High Query Volume):
  Parquet files + RocksDB index on local disk
  • RocksDB indexes trace_id → Parquet location
  • O(log n) lookups, faster than bloom filter scan
  • Requires local disk (100MB-1GB)
```

**Data Flow:**
```
OTLP Ingestion
  ↓
Tier 1: In-Memory (immediate, 5-15 min)
  ↓ Async flush every 5-15 minutes
Tier 2: Parquet via object_store (permanent, all older data)
  • Default: Parquet with bloom filters
  • Optional: + RocksDB index on disk for faster lookups
```

**Query Strategy:**
1. Check Tier 1 (in-memory) → < 1ms, 90-95% hit
2. Check Tier 2 (Parquet + index) → 15-35ms, 5-10% hit

**Why Two Tiers?**
- **Simpler architecture:** Only 2 systems instead of 3
- **Diskless compatible:** Works with S3-only deployment
- **Unified format:** Parquet everywhere below hot tier
- **Good latency:** 95% instant, 5% acceptable (15-35ms)

**See Also:**
- [parquet-schema.md](parquet-schema.md) - Arrow schema definitions
- [technology-decisions.md](technology-decisions.md) - Why DataFusion + Parquet
- [object-store-integration.md](object-store-integration.md) - Index and Parquet patterns

---

## Parquet Schema

All telemetry data is stored in Parquet format using Apache Arrow schemas. See **[parquet-schema.md](parquet-schema.md)** for complete schema definitions.

**Key schemas:**
- **Traces:** TraceId (FixedSizeBinary), service_name, timing, status
- **Spans:** SpanId, TraceId, parent relationships, attributes
- **Logs:** Timestamp, severity, body (for search), trace context
- **Metrics:** Gauge/Counter/Histogram data points with timestamps
- **Profiles:** pprof binary data with metadata

**Indexing:**
- Bloom filters on high-cardinality columns (trace_id, span_id)
- Min/max statistics on timestamps and durations
- Dictionary encoding for low-cardinality fields (service_name, status)

---

## Query Patterns with DataFusion

Sequins uses DataFusion to query Parquet files via SQL. DataFusion automatically applies:
- **Predicate pushdown:** Filters applied at Parquet row group level
- **Partition pruning:** Skips entire files/directories based on partitioning
- **Projection pushdown:** Only reads required columns
- **Bloom filter utilization:** Skips row groups using Parquet bloom filters

### Get Recent Traces

```rust
use datafusion::prelude::*;

async fn query_recent_traces(
    ctx: &SessionContext,
    service_name: &str,
    window: TimeWindow,
) -> Result<Vec<Trace>> {
    // Register Parquet files
    ctx.register_parquet(
        "traces",
        "s3://bucket/traces/",  // or local path
        ParquetReadOptions::default()
    ).await?;

    // SQL query with automatic optimization
    let df = ctx.sql(&format!(
        "SELECT * FROM traces
         WHERE service_name = '{}'
           AND start_time BETWEEN {} AND {}
         ORDER BY start_time DESC
         LIMIT 100",
        service_name, window.start().as_nanos(), window.end().as_nanos()
    )).await?;

    // Collect results
    let batches = df.collect().await?;
    Ok(record_batches_to_traces(batches)?)
}
```

**Optimization:**
- Partition pruning skips non-matching hour directories
- Bloom filter eliminates row groups without this service
- Min/max stats on start_time narrow to relevant row groups
- Only reads columns needed for query (projection pushdown)

### Get Trace by ID (with Index)

```rust
async fn get_trace_by_id(
    index: &Option<RocksDbIndex>,  // Optional RocksDB index
    datafusion_ctx: &SessionContext,
    trace_id: TraceId,
) -> Result<Option<Trace>> {
    if let Some(rocksdb) = index {
        // Fast path: RocksDB lookup (3-10ms)
        let location = match rocksdb.lookup(trace_id).await? {
            Some(loc) => loc,
            None => return Ok(None),
        };
        // Targeted Parquet read (10-20ms)
        return read_trace_at_location(location).await;
    }

    // Default path: DataFusion with bloom filters (15-35ms)
    let file = object_store.get(&location.file_path).await?;
    let reader = ParquetRecordBatchReader::try_new(file, 1024)?;

    // Jump to specific row group
    reader.skip_to_row_group(location.row_group)?;

    // Read and filter
    for batch in reader {
        let traces = record_batch_to_traces(batch?)?;
        if let Some(trace) = traces.into_iter().find(|t| t.trace_id == trace_id) {
            return Ok(Some(trace));
        }
    }

    Ok(None)
}
```

**Latency:** 15-45ms total (index lookup + Parquet read)

### Search Logs (Body Filter)

```rust
async fn search_logs(
    ctx: &SessionContext,
    service_name: &str,
    search_term: &str,
    window: TimeWindow,
) -> Result<Vec<LogEntry>> {
    ctx.register_parquet("logs", "s3://bucket/logs/", ParquetReadOptions::default()).await?;

    // Use regex for basic search (v1.0)
    let df = ctx.sql(&format!(
        "SELECT * FROM logs
         WHERE service_name = '{}'
           AND body LIKE '%{}%'
           AND timestamp BETWEEN {} AND {}
         ORDER BY timestamp DESC
         LIMIT 1000",
        service_name, search_term, window.start().as_nanos(), window.end().as_nanos()
    )).await?;

    let batches = df.collect().await?;
    Ok(record_batches_to_logs(batches)?)
}
```

**Full-Text Search Note:**
- For v1.0: LIKE/regex on body column (acceptable for 1-2 hours of data)
- For v2.0: Consider Tantivy integration for advanced FTS features

### Get Service Metrics

```rust
async fn query_metrics(
    ctx: &SessionContext,
    metric_name: &str,
    service_name: &str,
    window: TimeWindow,
) -> Result<Vec<MetricDataPoint>> {
    ctx.register_parquet("metrics", "s3://bucket/metrics/", ParquetReadOptions::default()).await?;

    let df = ctx.sql(&format!(
        "SELECT * FROM metrics
         WHERE name = '{}'
           AND service_name = '{}'
           AND timestamp BETWEEN {} AND {}
         ORDER BY timestamp ASC",
        metric_name, service_name, window.start().as_nanos(), window.end().as_nanos()
    )).await?;

    let batches = df.collect().await?;
    Ok(record_batches_to_metrics(batches)?)
}
```

## Performance Characteristics

### Query Optimization

**Automatic by DataFusion:**
- ✅ Predicate pushdown (WHERE clauses → Parquet row group filters)
- ✅ Projection pushdown (only read needed columns)
- ✅ Partition pruning (skip non-matching directories/files)
- ✅ Bloom filter utilization (skip row groups probabilistically)
- ✅ Min/max statistics (skip row groups deterministically)

### Retention and Cleanup

**File-based deletion (simple and efficient):**
```rust
// Delete old Parquet files (retention cleanup)
async fn cleanup_old_data(
    object_store: &Arc<dyn ObjectStore>,
    retention_hours: u32,
) -> Result<()> {
    let cutoff = now() - Duration::hours(retention_hours);

    // List all hour-based directories
    let prefix = "traces/".into();
    let mut stream = object_store.list(Some(&prefix)).await?;

    while let Some(meta) = stream.next().await.transpose()? {
        // Parse directory name (e.g., "traces/2025-01-15-14/")
        if let Some(timestamp) = parse_hour_from_path(&meta.location) {
            if timestamp < cutoff {
                // Delete entire directory
                object_store.delete(&meta.location).await?;
            }
        }
    }

    Ok(())
}
```

**Index cleanup:**
- RocksDB: Run compaction to remove stale entries (if optional RocksDB index is enabled)
- Parquet bloom filters: No cleanup needed, deleted with Parquet files

**No VACUUM needed:** Parquet files are immutable, deletion frees space immediately

### Parquet Compaction (Optional)

**When to compact:**
- Many small Parquet files (< 10MB each)
- High overhead from file metadata
- Sub-optimal compression ratio

**Compaction process:**
```rust
// Merge small files into larger files (optional optimization)
async fn compact_hour_bucket(
    object_store: &Arc<dyn ObjectStore>,
    hour_path: &str,  // "traces/2025-01-15-14/"
) -> Result<()> {
    // 1. List all Parquet files in hour
    let files = list_parquet_files(object_store, hour_path).await?;

    if files.len() < 10 {
        return Ok(());  // Not worth compacting
    }

    // 2. Read all RecordBatches
    let mut all_batches = Vec::new();
    for file in files {
        let batches = read_parquet(object_store, &file).await?;
        all_batches.extend(batches);
    }

    // 3. Write merged file
    let merged_path = format!("{}/compacted-{}.parquet.zst", hour_path, uuid::Uuid::new_v4());
    write_parquet(object_store, &merged_path, all_batches).await?;

    // 4. Delete old files
    for file in files {
        object_store.delete(&file).await?;
    }

    Ok(())
}
```

**Benefits:**
- Fewer files → faster queries (less metadata overhead)
- Better compression (larger batches compress better)
- More efficient bloom filters (one per merged file)

**When NOT to compact:**
- Recent data (likely to be queried soon, keep as-is)
- Already large files (> 100MB compressed)
- High write throughput (compaction can't keep up)

---

**Last Updated:** 2025-11-05
