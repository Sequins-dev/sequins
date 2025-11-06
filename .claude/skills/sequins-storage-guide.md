# Sequins Storage Implementation Guide

**Purpose:** Detailed patterns for implementing Storage and the two-tier architecture.

**When to use:**
- When editing `sequins-storage/` crate
- When implementing new storage features
- When optimizing query performance
- When working on data lifecycle (flush, retention)
- Manually when refreshing on storage patterns

**Invocation:** Automatically when editing storage code, or manually via `sequins-storage-guide`

---

## Two-Tier Storage Architecture

Sequins uses a dual-tier approach for optimal performance:

```
┌──────────────────────────────────────────────────────────┐
│                     Storage                        │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  ┌─────────────────────────┐   ┌────────────────────┐   │
│  │     HOT TIER            │   │    COLD TIER       │   │
│  │  (In-Memory, Papaya)    │   │  (Parquet + DF)    │   │
│  ├─────────────────────────┤   ├────────────────────┤   │
│  │ Duration: 5-15 min      │   │ Duration: hours+   │   │
│  │ Latency: <1ms           │   │ Latency: 15-35ms   │   │
│  │ Cache hit: 90-95%       │   │ Cache hit: 5-10%   │   │
│  │ Not persisted           │   │ Persistent         │   │
│  │ Lock-free (Papaya)      │   │ Queryable (DF)     │   │
│  └─────────────────────────┘   └────────────────────┘   │
│            │                             ▲               │
│            │                             │               │
│            └──────── Flush ──────────────┘               │
│                   (every 5-15 min)                       │
│                                                          │
│  ┌─────────────────────────────────────────────────┐    │
│  │          Optional RocksDB Index                 │    │
│  │  (Trace ID → Parquet file mapping)              │    │
│  │  - 100MB index for 1M traces                    │    │
│  │  - 2-3x faster cold tier queries                │    │
│  │  - Only if disk available                       │    │
│  │  - Falls back to bloom filters                  │    │
│  └─────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```

---

## Hot Tier: Papaya Lock-Free HashMap

### Why Papaya?

**Papaya is a lock-free concurrent HashMap designed for async workloads.**

Key advantages:
- **No locks** - CAS-based operations, no thread blocking
- **Async-safe** - Guards are `Send + Sync`, safe across `.await`
- **No deadlocks** - No locks = no deadlock possibility
- **Unlimited concurrency** - Not limited by shard count
- **Epoch-based reclamation** - Efficient memory management

### Hot Tier Structure

```rust
pub struct HotTier {
    traces: papaya::HashMap<TraceId, Trace>,
    logs: papaya::HashMap<LogId, LogRecord>,
    metrics: papaya::HashMap<MetricId, Metric>,
    profiles: papaya::HashMap<ProfileId, Profile>,

    // Track insertion time for flush decisions
    insertion_times: papaya::HashMap<TraceId, Timestamp>,
}
```

### Basic Papaya Patterns

#### Pattern 1: Insert

```rust
// ✅ GOOD - Simple insert
self.hot.traces.pin().insert(trace.trace_id, trace.clone());

// ✅ ALSO GOOD - Check if exists first
let guard = self.hot.traces.pin();
if !guard.contains_key(&trace_id) {
    guard.insert(trace_id, trace);
}
```

#### Pattern 2: Read

```rust
// ✅ GOOD - Pin, get, clone, drop guard
let trace = {
    let guard = self.hot.traces.pin();
    guard.get(&trace_id).cloned()
};
// Guard dropped here

// Use trace
if let Some(trace) = trace {
    // ...
}
```

#### Pattern 3: Iterate

```rust
// ✅ GOOD - Minimal guard scope
let matching_traces: Vec<Trace> = {
    let guard = self.hot.traces.pin();
    guard
        .iter()
        .filter(|(_, trace)| query.matches(trace))
        .map(|(_, trace)| trace.clone())
        .collect()
};
// Guard dropped here
```

#### Pattern 4: Remove

```rust
// ✅ GOOD - Remove during flush
let guard = self.hot.traces.pin();
for trace_id in traces_to_flush {
    guard.remove(&trace_id);
}
```

### ⚠️ Critical Rules for Papaya

#### Rule 1: Never Hold Guard Across `.await`

```rust
// ❌ BAD - Guard held across await (blocks other operations!)
let guard = self.hot.traces.pin();
let trace = guard.get(&trace_id).cloned();
self.write_to_cold_tier(trace).await?;  // Guard still alive!
drop(guard);

// ✅ GOOD - Drop guard before await
let trace = {
    let guard = self.hot.traces.pin();
    guard.get(&trace_id).cloned()
};  // Guard dropped

self.write_to_cold_tier(trace).await?;  // Safe!
```

**Why?** Guards pin the epoch. Holding across `.await` prevents memory reclamation and blocks concurrent operations.

#### Rule 2: Clone Out of Guard

```rust
// ❌ BAD - Returning reference tied to guard
fn get_trace(&self, id: TraceId) -> Option<&Trace> {
    let guard = self.hot.traces.pin();
    guard.get(&id)  // Lifetime tied to guard!
}

// ✅ GOOD - Clone and return owned value
fn get_trace(&self, id: TraceId) -> Option<Trace> {
    let guard = self.hot.traces.pin();
    guard.get(&id).cloned()
}  // Guard dropped
```

#### Rule 3: Keep Guard Scopes Small

```rust
// ❌ BAD - Guard lives too long
let guard = self.hot.traces.pin();
let trace1 = guard.get(&id1).cloned();
do_expensive_computation();  // Guard still alive!
let trace2 = guard.get(&id2).cloned();

// ✅ GOOD - Separate pin() calls
let trace1 = { self.hot.traces.pin().get(&id1).cloned() };
do_expensive_computation();
let trace2 = { self.hot.traces.pin().get(&id2).cloned() };
```

**Why?** Each `pin()` is cheap (~10-20ns). Keeping guards alive is expensive (blocks reclamation).

---

## Cold Tier: Parquet + DataFusion

### Why Parquet + DataFusion?

**Parquet:** Columnar storage format
- 40x compression vs row format
- Fast column scans (for aggregations)
- Built-in bloom filters and statistics
- Industry standard (used by InfluxDB 3, OpenObserve)

**DataFusion:** SQL query engine
- Optimized query execution
- Predicate pushdown (filters applied in Parquet reader)
- Parallel execution
- Arrow-native (zero-copy from Parquet)

### Cold Tier Structure

```rust
pub struct ColdTier {
    object_store: ObjectStoreType,  // Static dispatch - no dyn Trait!
    datafusion: Arc<SessionContext>,
    index: Option<RocksDbIndex>,  // Optional for faster lookups
}
```

### Object Store: Enum for Static Dispatch

**Critical:** Use enum, NOT `dyn ObjectStore` (violates zero-cost abstraction principle).

```rust
use object_store::{local::LocalFileSystem, aws::AmazonS3};

/// Enum for static dispatch across object store types
/// No trait objects - compiler knows exact type at compile time
pub enum ObjectStoreType {
    Local(LocalFileSystem),
    S3(AmazonS3),
}

impl ObjectStoreType {
    /// Create local filesystem store
    pub fn local(path: impl AsRef<Path>) -> Result<Self> {
        let store = LocalFileSystem::new_with_prefix(path)?;
        Ok(Self::Local(store))
    }

    /// Create S3 store
    pub fn s3(bucket: &str, region: &str) -> Result<Self> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .build()?;
        Ok(Self::S3(store))
    }

    /// Put object - delegates to specific implementation
    pub async fn put(&self, path: &Path, data: Bytes) -> Result<()> {
        match self {
            Self::Local(store) => store.put(path, data).await.map_err(Into::into),
            Self::S3(store) => store.put(path, data).await.map_err(Into::into),
        }
    }

    /// Get object - delegates to specific implementation
    pub async fn get(&self, path: &Path) -> Result<Bytes> {
        match self {
            Self::Local(store) => store.get(path).await.map_err(Into::into),
            Self::S3(store) => store.get(path).await.map_err(Into::into),
        }
    }

    /// List objects - delegates to specific implementation
    pub async fn list(&self, prefix: Option<&Path>) -> Result<Vec<ObjectMeta>> {
        match self {
            Self::Local(store) => {
                let list = store.list(prefix).await?;
                // Collect stream into Vec
                Ok(list.try_collect().await?)
            }
            Self::S3(store) => {
                let list = store.list(prefix).await?;
                Ok(list.try_collect().await?)
            }
        }
    }

    /// Delete object - delegates to specific implementation
    pub async fn delete(&self, path: &Path) -> Result<()> {
        match self {
            Self::Local(store) => store.delete(path).await.map_err(Into::into),
            Self::S3(store) => store.delete(path).await.map_err(Into::into),
        }
    }
}

// Usage:
// Local mode (default)
let store = ObjectStoreType::local("/data")?;

// S3 mode (diskless deployment)
let store = ObjectStoreType::s3("sequins-data", "us-east-1")?;
```

**Why enum over trait object?**
- ✅ **Static dispatch** - Compiler knows exact type, can inline
- ✅ **Zero overhead** - No vtable, no indirection
- ✅ **Match is fast** - Single branch, predicted correctly
- ❌ **Not extensible** - Can't add new store types without modifying enum
- ✅ **That's OK!** - We only support what we have config for anyway

**Performance:** Enum dispatch is essentially free. The match compiles to a simple branch that the CPU predicts correctly after the first call.

### Writing Parquet Files

#### Pattern 1: Batch Flush from Hot Tier

```rust
async fn flush_hot_to_cold(&self) -> Result<usize> {
    // 1. Collect traces to flush (older than hot_duration)
    let cutoff_time = Timestamp::now() - self.config.hot_duration;
    let to_flush: Vec<Trace> = {
        let guard = self.hot.traces.pin();
        guard
            .iter()
            .filter(|(_, trace)| trace.start_time < cutoff_time)
            .map(|(_, trace)| trace.clone())
            .collect()
    };  // Guard dropped

    if to_flush.is_empty() {
        return Ok(0);
    }

    // 2. Convert to Arrow RecordBatch
    let schema = Trace::arrow_schema();
    let batch = traces_to_record_batch(&to_flush, &schema)?;

    // 3. Write to Parquet
    let file_name = format!("traces_{}.parquet", Timestamp::now().as_nanos());
    let path = object_store::path::Path::from(file_name);

    let mut writer = ParquetWriter::try_new(
        self.cold.object_store.clone(),
        path,
        schema,
    )?;

    writer.write(&batch).await?;
    writer.close().await?;

    // 4. Update index if present
    if let Some(index) = &self.cold.index {
        for trace in &to_flush {
            index.insert(trace.trace_id, file_name.clone()).await?;
        }
    }

    // 5. Remove from hot tier
    {
        let guard = self.hot.traces.pin();
        for trace in &to_flush {
            guard.remove(&trace.trace_id);
        }
    }  // Guard dropped

    Ok(to_flush.len())
}
```

#### Pattern 2: Parquet Writer Configuration

```rust
use parquet::{
    basic::{Compression, Encoding},
    file::properties::WriterProperties,
};

let props = WriterProperties::builder()
    // Compression: Zstd is faster than Snappy with better ratio
    .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
    // Dictionary encoding for string columns (service names, etc.)
    .set_dictionary_enabled(true)
    .set_encoding(Encoding::PLAIN_DICTIONARY)
    // Enable bloom filters for fast lookups
    .set_bloom_filter_enabled(true)
    // Statistics for query optimization
    .set_statistics_enabled(true)
    .build();
```

### Querying Parquet Files

#### Pattern 1: DataFusion SQL Query

```rust
async fn query_cold_traces(&self, query: &TraceQuery) -> Result<Vec<Trace>> {
    let ctx = &self.cold.datafusion;

    // Register Parquet files as table (if not already registered)
    ctx.register_parquet(
        "traces",
        self.cold.object_store.clone(),
        "traces_*.parquet",
    ).await?;

    // Build SQL query with pushdown filters
    let sql = format!(
        r#"
        SELECT *
        FROM traces
        WHERE timestamp >= {}
          AND timestamp <= {}
          AND service_id = '{}'
        ORDER BY timestamp DESC
        LIMIT {}
        "#,
        query.start_time.as_nanos(),
        query.end_time.as_nanos(),
        query.service.as_ref().map(|s| s.to_string()).unwrap_or_default(),
        query.limit
    );

    let df = ctx.sql(&sql).await?;
    let batches = df.collect().await?;

    // Convert RecordBatch back to Rust types
    let traces = record_batches_to_traces(batches)?;

    Ok(traces)
}
```

#### Pattern 2: DataFrame API (Preferred)

```rust
use datafusion::prelude::*;

async fn query_cold_traces(&self, query: &TraceQuery) -> Result<Vec<Trace>> {
    let ctx = &self.cold.datafusion;

    let df = ctx
        .table("traces").await?
        .filter(col("timestamp").gt_eq(lit(query.start_time.as_nanos())))?
        .filter(col("timestamp").lt_eq(lit(query.end_time.as_nanos())))?;

    // Optional service filter
    let df = if let Some(service) = &query.service {
        df.filter(col("service_id").eq(lit(service.to_string())))?
    } else {
        df
    };

    let df = df
        .sort(vec![col("timestamp").sort(false, true)])?  // DESC
        .limit(0, Some(query.limit))?;

    let batches = df.collect().await?;
    let traces = record_batches_to_traces(batches)?;

    Ok(traces)
}
```

**Why DataFrame API?**
- Type-safe (catches errors at compile time)
- More composable (build query dynamically)
- Same performance as SQL (both use same query planner)

### Optional RocksDB Index

#### When to Use Index

- ✅ Disk available (not diskless deployment)
- ✅ Need 2-3x faster cold tier lookups
- ✅ Willing to use ~10% of data size for index (100MB for 1M traces)

#### When to Skip Index

- ❌ Diskless deployment (S3 only)
- ❌ Parquet bloom filters sufficient (most queries)
- ❌ Index rebuild overhead not worth it

#### Index Structure

```rust
pub struct RocksDbIndex {
    db: Arc<rocksdb::DB>,
}

impl RocksDbIndex {
    /// Insert mapping: Trace ID → Parquet file
    pub async fn insert(&self, trace_id: TraceId, file: String) -> Result<()> {
        self.db.put(trace_id.to_bytes(), file.as_bytes())?;
        Ok(())
    }

    /// Lookup which Parquet file contains trace
    pub async fn lookup(&self, trace_id: TraceId) -> Result<Option<String>> {
        let value = self.db.get(trace_id.to_bytes())?;
        Ok(value.map(|v| String::from_utf8_lossy(&v).to_string()))
    }
}
```

#### Query with Index

```rust
async fn get_trace_by_id(&self, trace_id: TraceId) -> Result<Option<Trace>> {
    // 1. Check hot tier first
    let hot = { self.hot.traces.pin().get(&trace_id).cloned() };
    if let Some(trace) = hot {
        return Ok(Some(trace));
    }

    // 2. Check index for file location
    if let Some(index) = &self.cold.index {
        if let Some(file) = index.lookup(trace_id).await? {
            // Query specific file only
            return self.query_parquet_file(&file, trace_id).await;
        }
    }

    // 3. Fall back to scanning all files (slow)
    self.scan_all_parquet_files(trace_id).await
}
```

---

## Data Lifecycle

### Phase 1: Ingestion → Hot Tier

```rust
impl OtlpIngest for Storage {
    async fn ingest_traces(&self, request: ExportTraceServiceRequest) -> Result<()> {
        // 1. Parse OTLP protobuf
        let traces = parse_otlp_traces(request)?;

        // 2. Validate and enrich
        for mut trace in traces {
            trace.validate()?;
            trace.service = self.resolve_service(&trace)?;
        }

        // 3. Insert into hot tier
        for trace in traces {
            self.hot.traces.pin().insert(trace.trace_id, trace);
            self.hot.insertion_times.pin().insert(trace.trace_id, Timestamp::now());
        }

        Ok(())
    }
}
```

### Phase 2: Hot → Cold (Flush)

**Background task runs every 1-5 minutes:**

```rust
pub async fn start_flush_task(&self) {
    let storage = self.clone();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;

            match storage.flush_hot_to_cold().await {
                Ok(count) => tracing::info!("Flushed {} records to cold tier", count),
                Err(e) => tracing::error!("Flush failed: {}", e),
            }
        }
    });
}
```

### Phase 3: Retention (Deletion)

**Background task runs periodically (hourly or daily):**

```rust
impl ManagementApi for Storage {
    async fn run_retention_cleanup(&self) -> Result<RetentionReport> {
        let cutoff = Timestamp::now() - self.config.retention_duration;

        // Delete old Parquet files
        let files = self.cold.object_store.list(None).await?;
        let mut deleted = 0;

        for file in files {
            // Check file timestamp (encoded in filename)
            if file_timestamp(&file) < cutoff {
                self.cold.object_store.delete(&file).await?;
                deleted += 1;

                // Remove from index
                if let Some(index) = &self.cold.index {
                    index.delete_file(&file.to_string()).await?;
                }
            }
        }

        Ok(RetentionReport {
            deleted_files: deleted,
            freed_bytes: deleted * AVG_FILE_SIZE,
        })
    }
}
```

---

## Performance Optimization

### Optimization 1: Early Return

```rust
// If hot tier has enough results and query is recent, skip cold tier
if hot_results.len() >= query.limit && query.within_hot_window() {
    return Ok(hot_results.into_iter().take(query.limit).collect());
}
```

**Impact:** 90-95% of queries avoid cold tier (50-100x faster)

### Optimization 2: Parallel Parquet Reads

DataFusion automatically parallelizes:
```rust
// DataFusion splits Parquet files across worker threads
let batches = df.collect().await?;  // Parallel!
```

### Optimization 3: Predicate Pushdown

```rust
// ✅ GOOD - Filter pushes down to Parquet reader
let df = ctx.table("traces")
    .filter(col("timestamp").gt(lit(cutoff)))?;  // Only reads matching rows!

// ❌ BAD - Reads all rows, then filters
let df = ctx.table("traces");
let batches = df.collect().await?;
let filtered = batches.filter(|b| b.timestamp > cutoff);  // Too late!
```

### Optimization 4: Bloom Filters

```rust
// Enable bloom filters in Parquet writer
let props = WriterProperties::builder()
    .set_bloom_filter_enabled(true)
    .build();

// DataFusion automatically uses bloom filters for point lookups
let df = ctx.table("traces")
    .filter(col("trace_id").eq(lit(target_id)))?;  // Uses bloom filter!
```

---

## Configuration

### Unified `data-lifecycle` Configuration

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataLifecycleConfig {
    // Hot tier duration (5-15 minutes typical)
    pub hot_duration: Duration,

    // Flush interval (1-5 minutes)
    pub flush_interval: Duration,

    // Cold tier retention (hours to days)
    pub cold_retention: Duration,

    // Cleanup interval (hourly or daily)
    pub cleanup_interval: Duration,

    // Optional index
    pub enable_rocksdb_index: bool,

    // Object store config
    pub object_store: ObjectStoreConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ObjectStoreConfig {
    Local { path: PathBuf },
    S3 { bucket: String, region: String },
}
```

### Reasonable Defaults

```rust
impl Default for DataLifecycleConfig {
    fn default() -> Self {
        Self {
            hot_duration: Duration::minutes(10),
            flush_interval: Duration::minutes(2),
            cold_retention: Duration::days(7),
            cleanup_interval: Duration::hours(1),
            enable_rocksdb_index: true,  // Enable if disk available
            object_store: ObjectStoreConfig::Local {
                path: PathBuf::from("./data"),
            },
        }
    }
}
```

### Creating ObjectStoreType from Config

```rust
impl ObjectStoreConfig {
    /// Convert config enum to runtime ObjectStoreType
    /// Still static dispatch - compiler knows both enum types!
    pub fn into_store(self) -> Result<ObjectStoreType> {
        match self {
            Self::Local { path } => ObjectStoreType::local(path),
            Self::S3 { bucket, region } => ObjectStoreType::s3(&bucket, &region),
        }
    }
}

// Usage:
let config = DataLifecycleConfig::load()?;
let object_store = config.object_store.into_store()?;

// Still zero-cost! Both enums compile to static dispatch
```

---

## Testing Storage

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_hot_tier_insert_and_retrieve() {
        let storage = Storage::new_in_memory()?;
        let trace = create_test_trace();

        storage.hot.traces.pin().insert(trace.trace_id, trace.clone());

        let retrieved = storage.hot.traces.pin().get(&trace.trace_id).cloned();
        assert_eq!(retrieved, Some(trace));
    }

    #[tokio::test]
    async fn test_flush_moves_to_cold() {
        let storage = Storage::new_in_memory()?;
        let trace = create_old_trace();  // Older than hot_duration

        storage.ingest_trace(trace.clone()).await?;
        storage.flush_hot_to_cold().await?;

        // Should be gone from hot
        assert!(storage.hot.traces.pin().get(&trace.trace_id).is_none());

        // Should be in cold
        let found = storage.query_cold_traces(&trace.trace_id).await?;
        assert_eq!(found, Some(trace));
    }
}
```

### Integration Tests

```rust
#[tokio::test]
async fn test_query_spans_hot_and_cold() {
    let storage = Storage::new_temp()?;

    // Insert recent trace (stays in hot)
    let recent = create_trace_at(Timestamp::now());
    storage.ingest_trace(recent.clone()).await?;

    // Insert old trace (will be flushed to cold)
    let old = create_trace_at(Timestamp::now() - Duration::hours(1));
    storage.ingest_trace(old.clone()).await?;
    storage.flush_hot_to_cold().await?;

    // Query should find both
    let query = TraceQuery::all();
    let results = storage.query_traces(query).await?;

    assert_eq!(results.len(), 2);
    assert!(results.contains(&recent));
    assert!(results.contains(&old));
}
```

---

## Success Criteria

Storage implementation is correct when:

- ✅ Hot tier uses Papaya correctly (no guards across await)
- ✅ Flush moves data from hot to cold periodically
- ✅ Queries check hot first, then cold
- ✅ Deduplication prevents duplicates during flush window
- ✅ Parquet files have bloom filters and compression
- ✅ DataFusion uses predicate pushdown
- ✅ Retention cleanup deletes old files
- ✅ Index (if enabled) stays in sync with Parquet files
- ✅ Performance meets targets (< 1ms hot, < 50ms cold)

---

**Remember:** The two-tier architecture is the performance foundation of Sequins. Correct implementation ensures sub-millisecond query latency for recent data (90%+ of queries) while maintaining efficient long-term storage.
