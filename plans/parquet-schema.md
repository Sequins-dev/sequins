# Parquet Schema Design

[← Back to Index](INDEX.md)

**Related Documentation:** [data-models.md](data-models.md) | [database.md](database.md) | [object-store-integration.md](object-store-integration.md) | [technology-decisions.md](technology-decisions.md)

---

## Overview

Sequins uses Apache Parquet as the storage format for all telemetry data. Parquet provides excellent compression (40x typical), efficient columnar analytics, and wide ecosystem support. This document defines the Arrow schemas used to write Parquet files.

---

## Type Mappings

### OpenTelemetry → Arrow Types

| OTLP Type | Rust Type | Arrow Type | Notes |
|-----------|-----------|------------|-------|
| `TraceId` | `[u8; 16]` | `FixedSizeBinary(16)` | 128-bit trace ID |
| `SpanId` | `[u8; 8]` | `FixedSizeBinary(8)` | 64-bit span ID |
| `Timestamp` | `i64` | `Timestamp(Nanosecond, None)` | Nanoseconds since epoch |
| `Duration` | `i64` | `Duration(Nanosecond)` | Nanoseconds |
| `string` | `String` | `Utf8` | Variable-length UTF-8 |
| `bytes` | `Vec<u8>` | `Binary` | Variable-length binary |
| `Attributes` | `HashMap<String, Value>` | `Struct` or `Utf8` (JSON) | See below |
| `repeated` | `Vec<T>` | `List<T>` | Variable-length list |

### Attributes Encoding

**Option A: JSON String (Simple)**
```rust
// Store as JSON string
attributes: Utf8  // {"http.method": "GET", "http.status_code": 200}
```
- ✅ Simple implementation
- ✅ Preserves all attribute types
- ⚠️ Less efficient for filtering (no predicate pushdown)

**Option B: Struct (Efficient)**
```rust
// Store as Arrow struct with known fields
attributes: Struct {
    http_method: Utf8,
    http_status_code: Int64,
    db_system: Utf8,
    ...
}
```
- ✅ Efficient predicate pushdown
- ✅ Better compression
- ⚠️ Requires schema evolution
- ⚠️ Unknown attributes need separate column

**Decision:** Use JSON strings (Option A) for v1.0, consider struct optimization for v2.0.

---

## Trace Schema

```rust
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

pub fn trace_arrow_schema() -> Schema {
    Schema::new(vec![
        // Identity
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("root_span_id", DataType::FixedSizeBinary(8), false),

        // Metadata
        Field::new("service_name", DataType::Utf8, false),
        Field::new("service_instance_id", DataType::Utf8, true),

        // Timing
        Field::new("start_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("end_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("duration_ns", DataType::Duration(TimeUnit::Nanosecond), false),

        // Metrics
        Field::new("span_count", DataType::UInt32, false),

        // Status
        Field::new("status", DataType::Utf8, false),  // "ok", "error", "unset"

        // Attributes (JSON)
        Field::new("attributes", DataType::Utf8, true),
    ])
}
```

**Example Parquet file:**
```
/var/lib/sequins/batches/traces/2025-01-15-14/batch-001.parquet.zst
  Row group 0 (128MB compressed):
    - 100,000 traces
    - Bloom filter on trace_id, service_name
    - Min/max stats on start_time, end_time, duration_ns
```

---

## Span Schema

```rust
pub fn span_arrow_schema() -> Schema {
    Schema::new(vec![
        // Identity
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("parent_span_id", DataType::FixedSizeBinary(8), true),

        // Metadata
        Field::new("service_name", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("kind", DataType::Utf8, false),  // "internal", "server", "client", etc.

        // Timing
        Field::new("start_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("end_time", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("duration_ns", DataType::Duration(TimeUnit::Nanosecond), false),

        // Status
        Field::new("status", DataType::Utf8, false),

        // Data (JSON)
        Field::new("attributes", DataType::Utf8, true),
        Field::new("events", DataType::Utf8, true),     // JSON array of events
        Field::new("links", DataType::Utf8, true),      // JSON array of links
    ])
}
```

**Partitioning strategy:**
- Partition by hour: `/traces/2025-01-15-14/`
- Optional: Partition by hash(trace_id) for better distribution

---

## Log Schema

```rust
pub fn log_arrow_schema() -> Schema {
    Schema::new(vec![
        // Identity
        Field::new("id", DataType::Utf8, false),  // UUID

        // Timing
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("observed_timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),

        // Metadata
        Field::new("service_name", DataType::Utf8, false),
        Field::new("severity", DataType::Utf8, false),  // "trace", "debug", "info", "warn", "error", "fatal"

        // Content
        Field::new("body", DataType::Utf8, false),  // Log message (for full-text search)

        // Tracing context
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),

        // Data (JSON)
        Field::new("attributes", DataType::Utf8, true),
        Field::new("resource", DataType::Utf8, true),
    ])
}
```

**Full-Text Search:**
- Parquet doesn't provide full-text search
- Options:
  1. Scan `body` column with filter (acceptable for 1-2 hours of data)
  2. Integrate Tantivy for dedicated FTS index
  3. Use DataFusion's regex filtering

**Decision for v1.0:** Scan body column (good enough performance with bloom filters)

---

## Metric Schema

### Metric Metadata

```rust
pub fn metric_metadata_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("unit", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, false),  // "gauge", "counter", "histogram", "summary"
        Field::new("service_name", DataType::Utf8, false),
    ])
}
```

### Gauge/Counter Data Points

```rust
pub fn metric_data_point_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("metric_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("value", DataType::Float64, false),
        Field::new("attributes", DataType::Utf8, true),  // JSON
    ])
}
```

### Histogram Data Points

```rust
pub fn histogram_data_point_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("metric_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),

        // Aggregates
        Field::new("count", DataType::UInt64, false),
        Field::new("sum", DataType::Float64, false),

        // Buckets (JSON arrays)
        Field::new("bucket_counts", DataType::Utf8, false),     // [10, 25, 40, 100]
        Field::new("explicit_bounds", DataType::Utf8, false),   // [0.1, 0.5, 1.0, 5.0]

        // Optional
        Field::new("exemplars", DataType::Utf8, true),  // JSON array
        Field::new("attributes", DataType::Utf8, true),
    ])
}
```

---

## Profile Schema

```rust
pub fn profile_arrow_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("service_name", DataType::Utf8, false),

        // Profile metadata
        Field::new("profile_type", DataType::Utf8, false),  // "cpu", "memory", "goroutine"
        Field::new("sample_type", DataType::Utf8, false),
        Field::new("sample_unit", DataType::Utf8, false),

        // Binary data
        Field::new("data", DataType::Binary, false),  // pprof binary format

        // Tracing context
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
    ])
}
```

---

## Compression & Encoding

### Compression Algorithm

**Zstd (level 3)** for all Parquet files:
```rust
use parquet::file::properties::WriterProperties;

let properties = WriterProperties::builder()
    .set_compression(parquet::basic::Compression::ZSTD(parquet::basic::ZstdLevel::try_new(3)?))
    .set_dictionary_enabled(true)
    .set_bloom_filter_enabled(true)
    .build();
```

**Why Zstd level 3:**
- Good balance between compression ratio and speed
- Level 1: Faster but larger files
- Level 5: Slower but smaller files
- Level 3: Sweet spot for observability data

### Dictionary Encoding

**Automatically enabled for:**
- `service_name` (low cardinality)
- `status` (very low cardinality: ok/error/unset)
- `severity` (low cardinality: 6 levels)
- `kind` (low cardinality: internal/server/client/producer/consumer)

**Not used for:**
- `trace_id`, `span_id` (high cardinality, unique per trace)
- `body` (log messages, high cardinality)

### Bloom Filters

**Enable bloom filters for:**
```rust
let properties = WriterProperties::builder()
    .set_column_bloom_filter_enabled("trace_id".into(), true)
    .set_column_bloom_filter_enabled("span_id".into(), true)
    .set_column_bloom_filter_enabled("service_name".into(), true)
    .build();
```

**Why bloom filters:**
- Eliminates row groups without the target value (no false negatives)
- Small overhead: ~100KB per row group
- Massive speedup for point queries (30x faster in research)

---

## Row Group Sizing

### Optimal Row Group Size

**Target:** 128MB compressed per row group

**Rationale:**
- Large enough for efficient compression
- Small enough for fine-grained filtering
- Matches S3 multi-part upload size
- Good for DataFusion query planning

**Rows per row group:**
```
Typical trace size: 1-2KB compressed
128MB / 1.5KB = ~85,000 traces per row group

Typical log entry: 500 bytes compressed
128MB / 500 bytes = ~250,000 logs per row group
```

### Page Size

**Target:** 1MB uncompressed per page

```rust
let properties = WriterProperties::builder()
    .set_data_page_size_limit(1024 * 1024)  // 1MB
    .build();
```

**Why:**
- Fine-grained skipping within row groups
- Good balance for I/O operations
- Enables efficient predicate pushdown

---

## Partitioning Strategy

### Time-Based Partitioning

**Partition by hour:**
```
/batches/traces/2025-01-15-14/batch-001.parquet.zst
                └─ YYYY-MM-DD-HH
```

**Benefits:**
- Natural time-range query optimization
- Simple retention (delete old directories)
- Matches observability query patterns (last 1h, 6h, 24h)

### Optional: Hash-Based Partitioning

**For very high throughput (>10K traces/sec):**
```
/batches/traces/2025-01-15-14/shard-003/batch-001.parquet.zst
                └─ hour     └─ hash(trace_id) % 16
```

**Benefits:**
- Distributes load across multiple files
- Parallel writes without contention
- Know exactly which file to read (hash lookup)

**When to use:**
- >10K traces/sec ingestion rate
- Multi-node deployments
- Load balancing across storage

**Decision for v1.0:** Time-based only, add hash partitioning in v2.0 if needed.

---

## File Naming Convention

```
{data_type}/{time_bucket}/batch-{uuid}.parquet.zst

Examples:
  traces/2025-01-15-14/batch-a1b2c3d4.parquet.zst
  logs/2025-01-15-14/batch-e5f6g7h8.parquet.zst
  metrics/2025-01-15-14/batch-i9j0k1l2.parquet.zst
```

**UUID ensures:**
- No filename collisions (important for S3)
- Can write multiple batches per hour
- Unique identification for index mapping

---

## Conversion Helpers

### Trace → RecordBatch

```rust
use arrow::array::*;
use arrow::record_batch::RecordBatch;

pub fn traces_to_record_batch(traces: &[Trace]) -> Result<RecordBatch> {
    let schema = trace_arrow_schema();

    // Build column arrays
    let trace_ids: FixedSizeBinaryArray = traces
        .iter()
        .map(|t| t.trace_id.as_bytes())
        .collect();

    let service_names: StringArray = traces
        .iter()
        .map(|t| t.service_name.as_str())
        .collect();

    let start_times: TimestampNanosecondArray = traces
        .iter()
        .map(|t| t.start_time)
        .collect();

    // ... more columns ...

    RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(trace_ids),
            Arc::new(service_names),
            Arc::new(start_times),
            // ... more columns ...
        ],
    )
}
```

### RecordBatch → Trace

```rust
pub fn record_batch_to_traces(batch: RecordBatch) -> Result<Vec<Trace>> {
    let mut traces = Vec::with_capacity(batch.num_rows());

    let trace_ids = batch.column(0)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();

    let service_names = batch.column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // ... get more columns ...

    for i in 0..batch.num_rows() {
        let trace_id = TraceId::from_bytes(trace_ids.value(i).try_into()?);
        let service_name = service_names.value(i).to_string();

        traces.push(Trace {
            trace_id,
            service_name,
            // ... more fields ...
        });
    }

    Ok(traces)
}
```

---

## Related Documentation

- **[data-models.md](data-models.md)** - Rust data structures (before Parquet conversion)
- **[database.md](database.md)** - Storage architecture (2-tier hot/cold)
- **[object-store-integration.md](object-store-integration.md)** - Writing/reading Parquet files
- **[technology-decisions.md](technology-decisions.md)** - Why Parquet + DataFusion

---

**Last Updated:** 2025-11-05
