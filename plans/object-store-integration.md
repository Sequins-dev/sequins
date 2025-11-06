# Object Store Integration

[← Back to Index](INDEX.md)

**Related Documentation:** [scaling-strategy.md](scaling-strategy.md) | [database.md](database.md) | [workspace-and-crates.md](workspace-and-crates.md)

---

## Overview

Sequins uses the `object_store` crate as a universal abstraction for all Parquet batch storage (Tier 3 / cold storage). This enables seamless switching between local filesystem storage (for development) and cloud object storage (for production) without code changes.

**Key Decision:** Use `object_store::LocalFileSystem` for local deployments and `object_store::AmazonS3` / MinIO for cloud deployments, with the same API for both.

---

## Why `object_store`?

### Architectural Rationale

**Problem:** Need to support both local-first development (FREE) and cloud deployments (PAID) without maintaining duplicate storage code.

**Solution:** Use `object_store` crate as universal blob storage API.

**Benefits:**
1. **Single Code Path:** Same storage implementation works locally and in cloud
2. **Config-Driven:** Switch backends via configuration file, not code changes
3. **Production-Proven:** Used by InfluxDB IOx, Apache DataFusion, crates.io
4. **Negligible Overhead:** LocalFileSystem adds ~95μs per operation (irrelevant for multi-MB batches)
5. **Testing Simplicity:** Use `InMemoryStore` for unit tests, validates all backends
6. **Future-Proof:** Easy to add GCS, Azure Blob Storage later

### Why NOT Alternatives?

| Alternative | Why Rejected |
|------------|--------------|
| **Direct `std::fs` + separate S3 code** | Requires conditional logic throughout codebase, duplicate code paths, harder to test |
| **`vfs` crate** | Not production-ready, designed for embedding files in binaries, poor semantic match for object storage, missing critical features |
| **`s3s-fs` / `rust-s3-server`** | Experimental or not embeddable, no documented production usage |
| **Running MinIO per-node** | High operational overhead: 15% CPU + 500MB-1GB memory per node, requires separate process management |

---

## Architecture

### Storage Tiers

```
┌─────────────────────────────────────────────────────────────┐
│                    Two-Tier Storage                          │
└─────────────────────────────────────────────────────────────┘

Tier 1: HOT (In-Memory)
  └─> papaya::HashMap<TraceId, Trace> (lock-free)
      • Direct memory, no object_store
      • Last 5-15 minutes of data
      • Immediate write latency: < 1ms (lock-free CAS)
      • Query latency: < 1ms
      • 90-95% cache hit rate
      • Async-friendly: no thread blocking

Tier 2: COLD (Parquet via object_store) ← THIS USES object_store
  └─> object_store::LocalFileSystem  (local development)
       → /var/lib/sequins/traces/*.parquet.zst
  └─> object_store::AmazonS3         (cloud production)
       → s3://bucket/traces/*.parquet.zst
      • All data older than hot tier (15 minutes to full retention)
      • Compressed (Zstd level 3, bloom filters, dictionary encoding)
      • Query latency: 15-35ms (with Parquet built-in indexes) or 12-25ms (with optional RocksDB)
      • 5-10% query hit rate
      • DataFusion SQL queries with predicate pushdown
```

### Backend Selection

Backends are selected at runtime via configuration:

```rust
use object_store::{ObjectStore, local::LocalFileSystem, aws::AmazonS3Builder};
use std::sync::Arc;

pub enum StorageBackend {
    Local { path: PathBuf },
    S3 { bucket: String, region: String },
    MinIO { endpoint: String, bucket: String },
}

pub fn create_object_store(config: &StorageBackend) -> Result<Arc<dyn ObjectStore>> {
    match config {
        StorageBackend::Local { path } => {
            let store = LocalFileSystem::new_with_prefix(path)?
                .with_automatic_cleanup();
            Ok(Arc::new(store))
        },
        StorageBackend::S3 { bucket, region } => {
            let store = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(region)
                .build()?;
            Ok(Arc::new(store))
        },
        StorageBackend::MinIO { endpoint, bucket } => {
            let store = AmazonS3Builder::new()
                .with_endpoint(endpoint)
                .with_bucket_name(bucket)
                .build()?;
            Ok(Arc::new(store))
        },
    }
}
```

---

## Configuration Examples

### Local Development (FREE)

```kdl
// config.kdl
storage {
    backend "local"
    path "/Users/username/.sequins/batches"
    retention-hours 24
}
```

**Storage location:** `/Users/username/.sequins/batches/traces/2025-01-15-14/batch-001.parquet.zst`

### Cloud Production (S3)

```kdl
// config.kdl
storage {
    backend "s3"
    bucket "sequins-prod-telemetry"
    region "us-east-1"
    retention-hours 168  // 7 days

    // AWS credentials via environment variables:
    // AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
}
```

**Storage location:** `s3://sequins-prod-telemetry/traces/2025-01-15-14/batch-001.parquet.zst`

### Self-Hosted MinIO

```kdl
// config.kdl
storage {
    backend "minio"
    endpoint "https://minio.company.com"
    bucket "sequins-telemetry"
    retention-hours 720  // 30 days

    // MinIO credentials via environment variables:
    // AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
}
```

**Storage location:** `https://minio.company.com/sequins-telemetry/traces/2025-01-15-14/batch-001.parquet.zst`

---

## Implementation Patterns

### Writing Parquet Batches

```rust
use object_store::ObjectStore;
use parquet::arrow::ArrowWriter;
use std::sync::Arc;

pub async fn flush_traces_to_storage(
    store: &Arc<dyn ObjectStore>,
    traces: &[Trace],
    time_bucket: i64,
) -> Result<()> {
    // 1. Convert traces to Arrow RecordBatch
    let schema = trace_arrow_schema();
    let batch = traces_to_record_batch(traces)?;

    // 2. Write to in-memory buffer as Parquet
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;

    // 3. Compress with Zstd (level 3 for balance)
    let compressed = zstd::encode_all(&buffer[..], 3)?;

    // 4. Upload to object store (local or S3 - same API!)
    let key = format!(
        "traces/{}/{}.parquet.zst",
        time_bucket,
        uuid::Uuid::new_v4()
    );

    store.put(&key.into(), compressed.into()).await?;

    tracing::info!(
        "Flushed {} traces to {} ({}  bytes compressed)",
        traces.len(),
        key,
        compressed.len()
    );

    Ok(())
}
```

### Reading Parquet Batches

```rust
pub async fn read_traces_from_storage(
    store: &Arc<dyn ObjectStore>,
    time_bucket: i64,
) -> Result<Vec<Trace>> {
    let prefix = format!("traces/{}/", time_bucket);

    // 1. List all files in time bucket (local or S3 - same API!)
    let mut stream = store.list(Some(&prefix.into())).await?;

    let mut all_traces = Vec::new();

    // 2. Fetch and parse each file
    while let Some(meta) = stream.next().await.transpose()? {
        // Download (local or S3 - same API!)
        let data = store.get(&meta.location).await?.bytes().await?;

        // Decompress
        let decompressed = zstd::decode_all(&data[..])?;

        // Parse Parquet to traces
        let traces = parquet_to_traces(&decompressed)?;
        all_traces.extend(traces);
    }

    Ok(all_traces)
}
```

### Caching Strategy

```rust
use lru::LruCache;
use std::sync::Mutex;

pub struct TieredStorage {
    object_store: Arc<dyn ObjectStore>,
    cache: Arc<Mutex<LruCache<String, Vec<u8>>>>,
}

impl TieredStorage {
    pub async fn get_traces_cached(
        &self,
        time_bucket: i64,
    ) -> Result<Vec<Trace>> {
        let key = format!("traces/{}", time_bucket);

        // Check cache first
        if let Some(cached) = self.cache.lock().unwrap().get(&key) {
            tracing::debug!("Cache HIT for {}", key);
            return Ok(deserialize_traces(cached)?);
        }

        // Cache miss - fetch from object store
        tracing::debug!("Cache MISS for {}", key);
        let traces = self.read_traces_from_storage(time_bucket).await?;

        // Update cache
        let serialized = serialize_traces(&traces)?;
        self.cache.lock().unwrap().put(key, serialized);

        Ok(traces)
    }
}
```

---

## Performance Characteristics

### LocalFileSystem Overhead

**Benchmark Results:**
- Direct `std::fs` I/O: ~0-5μs overhead
- `object_store::LocalFileSystem`: ~95μs overhead
- **Source:** Tokio's `spawn_blocking` thread pool dispatch

**When This Matters:**
- Small files (< 1KB): 95μs may be significant portion of total time
- High-frequency operations (> 1000/sec): Overhead accumulates

**When This Doesn't Matter (Sequins's Use Case):**
- Large files (10MB+ compressed Parquet): I/O time >> 95μs
- Infrequent operations (batch every 5-15 minutes): Overhead amortized
- **Verdict:** For multi-MB batch writes every 5 minutes, 95μs is **completely negligible**

### Performance Targets

| Operation | LocalFileSystem | S3/MinIO | Notes |
|-----------|-----------------|----------|-------|
| Write 10MB batch | ~100-200ms | ~500-1000ms | Dominated by I/O, not API overhead |
| Read 10MB batch | ~50-100ms | ~200-500ms | Network latency for S3 |
| List 1000 files | ~10-20ms | ~50-100ms | API calls matter more here |
| Delete batch | ~5-10ms | ~20-50ms | Fast for both |

---

## Testing Strategy

### Unit Tests with `InMemoryStore`

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_flush_and_read_traces() {
        // Use in-memory store for testing
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        // Create test traces
        let traces = vec![
            create_test_trace("trace-1"),
            create_test_trace("trace-2"),
        ];

        // Flush to "storage"
        flush_traces_to_storage(&store, &traces, 1234567890).await.unwrap();

        // Read back
        let read_traces = read_traces_from_storage(&store, 1234567890).await.unwrap();

        assert_eq!(read_traces.len(), 2);
        assert_eq!(read_traces[0].trace_id, traces[0].trace_id);
    }
}
```

### Integration Tests with `LocalFileSystem`

```rust
#[cfg(test)]
mod integration_tests {
    use super::*;
    use object_store::local::LocalFileSystem;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_filesystem_integration() {
        // Create temporary directory
        let temp_dir = TempDir::new().unwrap();

        // Use LocalFileSystem pointing to temp dir
        let store: Arc<dyn ObjectStore> = Arc::new(
            LocalFileSystem::new_with_prefix(temp_dir.path()).unwrap()
        );

        // Run same tests as InMemoryStore
        let traces = vec![create_test_trace("trace-1")];
        flush_traces_to_storage(&store, &traces, 1234567890).await.unwrap();

        // Verify file actually exists on disk
        let files: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .collect();
        assert!(!files.is_empty());
    }
}
```

---

## Migration Path

### Phase 1: Single-Node Local Development

```rust
// Initially: Just use LocalFileSystem
let store = LocalFileSystem::new_with_prefix("/var/lib/sequins/batches")?;
let object_store: Arc<dyn ObjectStore> = Arc::new(store);
```

### Phase 2: Add S3 Backup (Optional)

```rust
// Hybrid: Write to LocalFileSystem + async upload to S3
let local_store = Arc::new(LocalFileSystem::new_with_prefix("/var/lib/sequins/batches")?);
let s3_store = Arc::new(AmazonS3Builder::new()
    .with_bucket_name("sequins-backup")
    .build()?);

// Write to local immediately
flush_traces_to_storage(&local_store, &traces, time_bucket).await?;

// Upload to S3 in background (best-effort)
tokio::spawn(async move {
    let _ = flush_traces_to_storage(&s3_store, &traces, time_bucket).await;
});
```

### Phase 3: Pure Cloud Deployment

```rust
// Cloud-native: Use S3 directly
let store = Arc::new(AmazonS3Builder::new()
    .with_bucket_name("sequins-prod")
    .with_region("us-east-1")
    .build()?);

// Same code, different backend!
flush_traces_to_storage(&store, &traces, time_bucket).await?;
```

---

## API Reference

### Core Operations

```rust
// trait ObjectStore (implemented by LocalFileSystem, AmazonS3, etc.)

// Write object
async fn put(&self, location: &Path, bytes: Bytes) -> Result<PutResult>

// Read object
async fn get(&self, location: &Path) -> Result<GetResult>

// List objects with prefix
fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>>

// List with delimiter (directory-like listing)
async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult>

// Delete object
async fn delete(&self, location: &Path) -> Result<()>

// Copy object (efficient for LocalFileSystem via hard links)
async fn copy(&self, from: &Path, to: &Path) -> Result<()>

// Copy if not exists (atomic create)
async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()>
```

### LocalFileSystem-Specific

```rust
// Create with prefix directory
LocalFileSystem::new_with_prefix<P: AsRef<std::path::Path>>(prefix: P) -> Result<Self>

// Enable automatic cleanup of empty directories
fn with_automatic_cleanup(self) -> Self

// Check if directory is cleaned up
fn automatic_cleanup(&self) -> bool
```

### AmazonS3Builder-Specific

```rust
// Create builder
AmazonS3Builder::new() -> Self

// Configure bucket
fn with_bucket_name(self, bucket: impl Into<String>) -> Self

// Configure region
fn with_region(self, region: impl Into<String>) -> Self

// Configure custom endpoint (for MinIO)
fn with_endpoint(self, endpoint: impl Into<String>) -> Self

// Use default AWS credentials from environment
fn from_env() -> Self

// Build the store
fn build(self) -> Result<AmazonS3>
```

---

## Index Architecture

For trace_id lookups and filtered queries, Sequins uses indexing strategies to avoid scanning all Parquet files.

### Two Index Strategies

**Default: Parquet Built-in Indexes**
- Bloom filters on trace_id, span_id, service_name columns
- Column statistics (min/max values per row group)
- DataFusion's automatic predicate pushdown
- No separate index files needed
- Latency: 15-35ms (DataFusion scans with bloom filter acceleration)

**Optional: RocksDB Index**
- Embedded key-value store on local disk
- Maps trace_id → (file_path, row_group, offset)
- Enables single-file targeted reads
- Persistent across restarts
- Latency: 12-20ms total (3-5ms RocksDB lookup + 10-15ms Parquet read)

### Parquet Built-in Indexes (Default)

**File structure:**
```
/batches/traces/2025-01-15-14/
  ├── batch-001.parquet.zst     ← Contains bloom filters + stats
  └── batch-002.parquet.zst
```

**What's included in Parquet:**
- **Bloom filters**: Probabilistic data structure for trace_id, span_id, service_name
- **Column statistics**: Min/max values per row group
- **Row group metadata**: File offsets, compressed/uncompressed sizes
- **Dictionary encoding**: For repeated string values (service names)

**Writing Parquet with bloom filters:**
```rust
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use parquet::basic::Compression;

pub async fn write_parquet_with_bloom_filters(
    store: &Arc<dyn ObjectStore>,
    traces: &[Trace],
    path: &str,
) -> Result<()> {
    let schema = trace_arrow_schema();
    let batch = traces_to_record_batch(traces)?;

    // Configure Parquet writer with bloom filters
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(3))
        .set_bloom_filter_enabled("trace_id".into(), true)
        .set_bloom_filter_enabled("span_id".into(), true)
        .set_bloom_filter_enabled("service_name".into(), true)
        .set_bloom_filter_fpp(0.01)  // 1% false positive rate
        .set_statistics_enabled(true)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    // Write to object store
    store.put(&path.into(), buffer.into()).await?;
    Ok(())
}
```

**Querying with DataFusion (automatic bloom filter usage):**
```rust
use datafusion::prelude::*;

pub async fn get_trace_with_datafusion(
    ctx: &SessionContext,
    trace_id: TraceId,
) -> Result<Option<Trace>> {
    // DataFusion automatically:
    // 1. Checks bloom filters (eliminates 99% of files instantly)
    // 2. Uses column stats for predicate pushdown
    // 3. Skips row groups that don't contain trace_id

    let df = ctx
        .sql("SELECT * FROM traces WHERE trace_id = $1")
        .bind(trace_id.to_string())
        .await?;

    let batches = df.collect().await?;

    // Convert Arrow batches back to Trace
    for batch in batches {
        let traces = record_batch_to_traces(batch)?;
        if let Some(trace) = traces.into_iter().find(|t| t.trace_id == trace_id) {
            return Ok(Some(trace));
        }
    }

    Ok(None)
}
```

**Benefits:**
- ✅ Zero operational overhead (no separate index files)
- ✅ Works with any object store (S3, local, GCS, Azure)
- ✅ Bloom filters eliminate 99% of files instantly
- ✅ DataFusion handles optimization automatically
- ✅ No separate cleanup needed

**Performance:**
- Bloom filter check: < 1ms per file
- Column stats check: < 1ms
- Parquet read (if bloom filter matches): 10-20ms
- **Total: 15-35ms for trace_id lookup**

### RocksDB Index Implementation

**File structure:**
```
Local disk:
  /var/lib/sequins/index/  ← RocksDB database

S3:
  /batches/traces/2025-01-15-14/
    ├── batch-001.parquet.zst
    └── batch-002.parquet.zst
```

**Index schema:**
```
Key: trace_id (16 bytes)
Value: ParquetLocation (bincode-serialized)

struct ParquetLocation {
    file_path: String,
    row_group: u32,
    row_offset: u64,
}
```

**Writing to RocksDB:**
```rust
pub async fn write_parquet_with_rocksdb(
    store: &Arc<dyn ObjectStore>,
    index: &DB,  // RocksDB
    traces: &[Trace],
    path: &str,
) -> Result<()> {
    // 1. Write Parquet to S3
    let parquet_path = path.into();
    write_parquet(store, &parquet_path, traces).await?;

    // 2. Update RocksDB index
    let mut batch = WriteBatch::default();
    for (i, trace) in traces.iter().enumerate() {
        let location = ParquetLocation {
            file_path: path.to_string(),
            row_group: 0,
            row_offset: i as u64,
        };

        batch.put(
            trace.trace_id.as_bytes(),
            bincode::serialize(&location)?
        );
    }
    index.write(batch)?;  // Atomic batch write

    Ok(())
}
```

**Reading from RocksDB:**
```rust
pub async fn get_trace_with_rocksdb(
    store: &Arc<dyn ObjectStore>,
    index: &DB,
    trace_id: TraceId,
) -> Result<Option<Trace>> {
    // 1. RocksDB lookup (3-5ms)
    let location_bytes = match index.get(trace_id.as_bytes())? {
        Some(bytes) => bytes,
        None => return Ok(None),
    };

    let location: ParquetLocation = bincode::deserialize(&location_bytes)?;

    // 2. Fetch from S3 (10-20ms)
    let parquet_bytes = store.get(&location.file_path.into()).await?.bytes().await?;
    let reader = ParquetRecordBatchReader::try_new(/* ... */)?;
    reader.skip_to_row_group(location.row_group)?;

    // 3. Find exact trace
    for batch in reader {
        let traces = record_batch_to_traces(batch?)?;
        if let Some(trace) = traces.into_iter().find(|t| t.trace_id == trace_id) {
            return Ok(Some(trace));
        }
    }

    Ok(None)
}
```

**Benefits:**
- ✅ Faster than Parquet built-ins (12-20ms vs 15-35ms)
- ✅ Persistent across restarts
- ✅ Single-file targeted reads (no bloom filter scan)
- ✅ Indexes ALL data efficiently

**Drawbacks:**
- ⚠️ Requires local disk (not diskless)
- ⚠️ RocksDB compaction overhead
- ⚠️ Additional operational complexity
- ⚠️ Must clean up stale entries when Parquet files deleted

### Index Cleanup

**Parquet Built-in Indexes:**
- No cleanup needed (indexes are part of Parquet files)
- Deleted automatically when Parquet files are deleted

**RocksDB (if enabled):**
```rust
pub async fn cleanup_stale_index_entries(
    index: &DB,
    store: &Arc<dyn ObjectStore>,
    retention_hours: u32,
) -> Result<()> {
    let cutoff = now() - Duration::hours(retention_hours);

    // Iterate through RocksDB
    let mut iter = index.raw_iterator();
    iter.seek_to_first();

    let mut to_delete = Vec::new();

    while iter.valid() {
        if let Some(value) = iter.value() {
            let location: ParquetLocation = bincode::deserialize(value)?;

            // Check if Parquet file still exists
            match store.head(&location.file_path.into()).await {
                Ok(_) => {
                    // File exists, keep index entry
                }
                Err(_) => {
                    // File deleted, mark for removal
                    to_delete.push(iter.key().unwrap().to_vec());
                }
            }
        }

        iter.next();
    }

    // Delete stale entries
    for key in to_delete {
        index.delete(key)?;
    }

    // Compact RocksDB
    index.compact_range::<&[u8], &[u8]>(None, None);

    Ok(())
}
```

### Configuration

**Diskless mode (default):**
```kdl
storage {
    backend "s3"
    bucket "sequins-traces"
    region "us-east-1"
    hot-ttl-minutes 15

    // Default: Use Parquet built-in indexes (bloom filters)
    // No additional configuration needed
}
```

**With optional RocksDB index:**
```kdl
storage {
    backend "s3"
    bucket "sequins-traces"
    region "us-east-1"
    hot-ttl-minutes 15

    // Optional: Enable RocksDB for faster lookups
    rocksdb {
        enabled true
        path "/var/lib/sequins/index"
    }
}
```

### Performance Comparison

| Feature | Parquet Built-in | RocksDB |
|---------|------------------|---------|
| **Index lookup** | 1-5ms (bloom filters) | 3-5ms |
| **Parquet read** | 10-30ms | 10-20ms |
| **Total latency** | 15-35ms | 12-25ms |
| **Disk requirement** | None | 100MB-1GB |
| **Persistence** | Part of Parquet files | Separate database |
| **Cleanup** | Automatic | Manual compaction needed |
| **Operational overhead** | Zero | Medium |

**Recommendation:**
- **Default**: Use Parquet built-in indexes (bloom filters + column stats)
  - Zero operational overhead
  - Works on diskless deployments (S3-only)
  - Performance adequate for most workloads (15-35ms)
- **Optional**: Enable RocksDB when local disk available and query performance is critical
  - Requires local SSD (100MB-1GB)
  - Slightly faster (12-25ms vs 15-35ms)
  - Additional maintenance overhead

---

## Troubleshooting

### LocalFileSystem Issues

**Problem:** Permission denied errors
**Solution:** Ensure process has write permissions to configured path

**Problem:** Disk full errors
**Solution:** Monitor disk usage, implement retention cleanup, configure smaller batches

**Problem:** Slow local writes
**Solution:** Check disk I/O utilization, use SSD instead of HDD, reduce batch size

### S3 Issues

**Problem:** Authentication errors
**Solution:** Verify AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables

**Problem:** Slow uploads
**Solution:** Enable multipart uploads for large batches, check network bandwidth

**Problem:** High S3 costs
**Solution:** Enable Intelligent-Tiering, implement aggressive retention cleanup

### MinIO Issues

**Problem:** Connection refused
**Solution:** Verify endpoint URL, check MinIO server is running

**Problem:** SSL/TLS errors
**Solution:** Add certificate to system trust store or configure `with_allow_http()` for testing

---

## Related Documentation

- **[scaling-strategy.md](scaling-strategy.md)** - Section 2 explains universal object_store decision
- **[database.md](database.md)** - Storage architecture overview with two tiers (hot + cold)
- **[parquet-schema.md](parquet-schema.md)** - Arrow schema definitions for all data types
- **[technology-decisions.md](technology-decisions.md)** - Why DataFusion + Parquet and index strategies
- **[workspace-and-crates.md](workspace-and-crates.md)** - sequins-storage crate using object_store

---

**Last Updated:** 2025-11-05
