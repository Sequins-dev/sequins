# Checkpoint: Unified DataFusion Queries Across Hot & Cold Tiers

**Date:** 2025-01-07
**Status:** Architecture Research Complete, Ready for Implementation
**Next Phase:** Phase 1 - MemTable Prototype

---

## Executive Summary

After extensive research into production observability systems (InfluxDB IOx, Grafana Tempo, ClickHouse) and DataFusion patterns, we've determined the optimal architecture for unified SQL queries across hot (HashMap) and cold (Parquet) storage tiers.

**Key Decision:** Implement custom DataFusion `TableProvider` for hot tier with lazy conversion, following the InfluxDB IOx pattern.

---

## Completed Work

### 1. Storage Traits Created ✅

**File:** `crates/sequins-core/src/traits/storage.rs` (NEW)

Added three new traits for internal storage abstraction:

```rust
/// Read operations on storage sources
pub trait StorageRead: Send + Sync {
    async fn query_traces(&self, query: &TraceQuery) -> Result<Vec<QueryTrace>>;
    async fn query_logs(&self, query: &LogQuery) -> Result<Vec<LogEntry>>;
    async fn query_metrics(&self, query: &MetricQuery) -> Result<Vec<Metric>>;
    async fn query_profiles(&self, query: &ProfileQuery) -> Result<Vec<Profile>>;
    // ... get methods
}

/// Write operations on storage destinations
pub trait StorageWrite: Send + Sync {
    async fn write_spans(&self, spans: Vec<Span>) -> Result<()>;
    async fn write_logs(&self, logs: Vec<LogEntry>) -> Result<()>;
    async fn write_metrics(&self, metrics: Vec<Metric>) -> Result<()>;
    async fn write_profiles(&self, profiles: Vec<Profile>) -> Result<()>;
}

/// Metadata about a storage tier for query routing
pub trait TierMetadata {
    fn tier_id(&self) -> &str;
    fn priority(&self) -> u8;  // Lower = query first
    fn covers_time_range(&self, start: Timestamp, end: Timestamp) -> bool;
}
```

**Exported:** `sequins-core/src/traits/mod.rs` updated to export these traits.

**Purpose:** Enable future distributed querying and provide clean abstraction for tier implementations.

### 2. Dependencies Added ✅

- ✅ `datafusion = { workspace = true }` - Already in Cargo.toml
- ✅ `url = { workspace = true }` - Added for DataFusion URL parsing
- ✅ `arrow = { workspace = true }` - Already present

### 3. Basic Infrastructure ✅

- ✅ `SessionContext` added to `ColdTier`
- ✅ Object store registered with DataFusion
- ✅ Placeholder query methods in `ColdTier` (return empty results)

---

## Research Findings

### Question 1: HashMap vs Arrow for Hot Tier?

**Answer: HashMap (current approach) is correct ✅**

**Evidence from Production Systems:**

1. **InfluxDB IOx** (Rust, production observability system):
   - Hot tier: `MutableBuffer` (NOT Arrow) - custom row-oriented structure
   - Only converts to Arrow when flushing to Parquet
   - **Exactly our architecture**

2. **Grafana Tempo**:
   - Hot tier: In-memory "live traces" (NOT Arrow)
   - Arrow/Parquet only for cold storage

3. **ClickHouse**:
   - Write path: In-memory row-like parts
   - Conversion to columnar happens asynchronously

**Why Arrow is Wrong for Hot Tier:**

| Factor | HashMap (Papaya) | Arrow RecordBatch |
|--------|------------------|-------------------|
| Write throughput | ⭐⭐⭐⭐⭐ 10k-100k ops/sec | ⭐⭐ Limited by serialization |
| Concurrency | ⭐⭐⭐⭐⭐ Lock-free CAS | ⭐ Requires locks/channels |
| Point lookups | ⭐⭐⭐⭐⭐ O(1) | ⭐⭐ Linear scan or index needed |
| Implementation | ⭐⭐⭐⭐⭐ Simple | ⭐⭐ Complex coordination |

**Arrow arrays are immutable** - cannot append efficiently. Would require:
- Serialization through locks or channels (bottleneck)
- Periodic rebuilding of batches (write amplification)
- Complex coordination layer

**Conclusion:** Keep Papaya HashMap for hot tier writes. ✅

### Question 2: How to Unify Queries Across Hot + Cold?

**Answer: Custom TableProvider with Lazy Conversion**

**Problem:**
- Hot tier: Papaya HashMap (optimal for writes)
- Cold tier: Parquet files (queryable via DataFusion)
- Need: Unified SQL queries across both tiers

**Evaluated Options:**

#### Option A: Dual Query Paths ❌
```rust
let hot_results = hot_tier.query_traces(&query);  // Custom logic
let cold_results = datafusion.sql("SELECT ...").await?;  // SQL
merge(hot_results, cold_results)
```
- ❌ Duplicate query logic (maintain two implementations)
- ❌ Complex merge/deduplication
- ❌ Can't do JOINs or complex queries across tiers

#### Option B: Frequent Flushing ❌
```rust
// Flush every 30s instead of 5 minutes
// Query only cold tier via DataFusion
```
- ❌ 30-60s stale queries
- ❌ High S3 costs (201,600 files/week with 10 services!)
- ❌ Poor query performance (too many files)

#### Option C: Lazy Conversion via TableProvider ✅ **RECOMMENDED**
```rust
// Register hot tier as DataFusion table
impl TableProvider for HotTierTableProvider {
    async fn scan(&self, filters: &[Expr]) -> Result<Arc<dyn ExecutionPlan>> {
        // Return custom exec plan that converts on-demand
    }
}

// Unified SQL query
let df = ctx.sql("
    SELECT * FROM (
        SELECT * FROM hot_spans
        UNION ALL
        SELECT * FROM cold_spans
    )
    WHERE service_name = 'api'
").await?;
```

**Benefits:**
- ✅ Unified SQL queries (single code path)
- ✅ Instant query freshness (no flush delay)
- ✅ Filter pushdown (HashMap filtered before conversion)
- ✅ Lazy conversion (only matching data converted)
- ✅ No duplicate storage (HashMap remains primary)
- ✅ Complex queries work (JOINs, aggregations, etc.)

**Tradeoffs:**
- ⚠️ Implementation complexity (~300 LOC for TableProvider)
- ⚠️ Filter translation needed (DataFusion Expr → HashMap filters)
- ⚠️ Temporary RecordBatch allocation during queries

**Performance Estimates:**

| Metric | Value | Notes |
|--------|-------|-------|
| Write latency | 5-10μs | Unchanged (no overhead) |
| Query latency | 1-10ms | Conversion only for matching data |
| Memory overhead | 0-5% | Temporary RecordBatch during query |
| Query freshness | Instant | Queries see latest data immediately |

**Conversion Cost Analysis:**
```
Scenario: 10,000 spans in hot tier, query filters to 100 spans

Full conversion (naive):
- Convert all 10,000 spans → RecordBatch: ~2-5ms
- DataFusion filters down to 100: ~0.5ms
- Total: ~2.5-5.5ms

Lazy conversion (with filter pushdown):
- Filter HashMap to 100 spans: ~0.2ms
- Convert only 100 spans → RecordBatch: ~0.1ms
- Total: ~0.3ms

Speedup: 8-18x faster
```

#### Option D: Dual Representation ❌
```rust
struct HotTier {
    write_index: HashMap,        // For writes
    query_batches: Vec<RecordBatch>,  // For queries
}
```
- ❌ 100% memory overhead (data stored twice)
- ❌ Slower writes (update both structures)
- ❌ Complex synchronization

**Conclusion:** Implement Option C (Custom TableProvider) ✅

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────┐
│                 Query Request                        │
│        "SELECT * FROM spans WHERE ..."               │
└────────────────────┬────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│            DataFusion SessionContext                 │
│  ┌────────────────────────────────────────────────┐ │
│  │   SQL Parser → Logical Plan → Physical Plan    │ │
│  └────────────────────────────────────────────────┘ │
└──────┬──────────────────────────┬───────────────────┘
       │                          │
       ▼                          ▼
┌──────────────────┐   ┌───────────────────────────┐
│  HotTierTable    │   │  ParquetTable (cold tier) │
│  Provider        │   │                           │
└────────┬─────────┘   └───────────┬───────────────┘
         │                         │
         ▼                         ▼
┌─────────────────┐   ┌──────────────────────────┐
│ HotTierScanExec │   │ ParquetExec (built-in)   │
│  execute()      │   │                          │
└────────┬────────┘   └───────────┬──────────────┘
         │                        │
         │ 1. Filter HashMap      │ 1. Read Parquet
         │ 2. Convert → Batch     │ 2. Apply filters
         │                        │
         ▼                        ▼
┌────────────────────────────────────────────────┐
│        RecordBatchStream (Arrow)                │
│  ┌──────────────┐   ┌──────────────────────┐   │
│  │  Hot Batches │   │   Cold Batches       │   │
│  └──────────────┘   └──────────────────────┘   │
└──────────────────┬─────────────────────────────┘
                   │
                   ▼
┌──────────────────────────────────────────────────┐
│         DataFusion Execution Engine               │
│    (UNION, JOIN, GROUP BY, ORDER, LIMIT)         │
└────────────────────┬─────────────────────────────┘
                     │
                     ▼
┌──────────────────────────────────────────────────┐
│              Final Results                        │
└──────────────────────────────────────────────────┘
```

---

## Implementation Plan

### Phase 1: MemTable Prototype (Week 1) 🎯 **START HERE**

**Goal:** Quick validation of DataFusion integration concept

**Tasks:**
1. Add helper method to convert HotTier → RecordBatch
2. Register hot tier as MemTable with DataFusion
3. Test unified SQL query (UNION ALL)
4. Benchmark performance vs current dual-query approach

**Code Sketch:**
```rust
// In cold_tier.rs
impl ColdTier {
    pub async fn query_traces_unified_prototype(&self, query: &TraceQuery) -> Result<Vec<Span>> {
        // 1. Convert hot tier to RecordBatch (full conversion for now)
        let hot_batch = self.hot_tier_to_record_batch()?;

        // 2. Register as MemTable
        let hot_table = MemTable::try_new(
            arrow_schema::span_schema(),
            vec![vec![hot_batch]],
        )?;
        self.ctx.register_table("hot_spans", Arc::new(hot_table))?;

        // 3. Query both tiers with SQL
        let sql = format!("
            SELECT * FROM (
                SELECT * FROM hot_spans
                UNION ALL
                SELECT * FROM read_parquet('{}/spans/**/*.parquet')
            )
            WHERE start_time_ns >= {}
            LIMIT {}
        ", self.config.uri, query.start_time.as_nanos(), query.limit.unwrap_or(100));

        let df = self.ctx.sql(&sql).await?;
        let batches = df.collect().await?;

        Self::record_batches_to_spans(batches)
    }

    fn hot_tier_to_record_batch(&self) -> Result<RecordBatch> {
        let spans = self.hot_tier.get_all_spans();
        ColdTier::spans_to_record_batch(spans, arrow_schema::span_schema())
    }
}
```

**Success Criteria:**
- ✅ SQL queries work across both tiers
- ✅ Query performance acceptable (target: <20ms for typical queries)
- ✅ Results are correct (no data loss, proper ordering)

**Decision Gate:** If prototype performs poorly or is too complex, abort and keep dual-query approach.

### Phase 2: Custom TableProvider (Week 2-3)

**Goal:** Production implementation with filter pushdown

**Files to Create:**

1. **`crates/sequins-storage/src/hot_tier_provider.rs`** (~150 LOC)
```rust
use datafusion::datasource::{TableProvider, TableType};
use std::sync::Arc;
use crate::hot_tier::HotTier;

pub struct HotTierTableProvider {
    hot_tier: Arc<HotTier>,
    schema: SchemaRef,
}

impl HotTierTableProvider {
    pub fn new(hot_tier: Arc<HotTier>, schema: SchemaRef) -> Self {
        Self { hot_tier, schema }
    }
}

#[async_trait::async_trait]
impl TableProvider for HotTierTableProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    async fn scan(
        &self,
        _state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(HotTierScanExec::new(
            self.hot_tier.clone(),
            self.schema.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
        )))
    }

    fn supports_filters_pushdown(&self, filters: &[&Expr]) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters.iter().map(|f| {
            if Self::can_pushdown_filter(f) {
                TableProviderFilterPushDown::Exact
            } else {
                TableProviderFilterPushDown::Unsupported
            }
        }).collect())
    }
}
```

2. **`crates/sequins-storage/src/hot_tier_exec.rs`** (~250 LOC)
```rust
use datafusion::physical_plan::{ExecutionPlan, SendableRecordBatchStream};

pub struct HotTierScanExec {
    hot_tier: Arc<HotTier>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
}

impl ExecutionPlan for HotTierScanExec {
    fn execute(&self, _partition: usize, _context: Arc<TaskContext>)
        -> Result<SendableRecordBatchStream>
    {
        // 1. Convert filters to hot tier query
        let query = self.filters_to_query();

        // 2. Query hot tier (applies filters at HashMap level)
        let spans = self.hot_tier.query_traces_for_datafusion(&query);

        // 3. Convert matching spans to RecordBatch
        let batch = self.spans_to_record_batch(spans)?;

        // 4. Apply projection if needed
        let batch = if let Some(ref projection) = self.projection {
            batch.project(projection)?
        } else {
            batch
        };

        // 5. Create stream
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream::once(async move { Ok(batch) }),
        )))
    }
}
```

**Key Implementation Details:**

- **Filter Translation:** Convert DataFusion `Expr` to `TraceQuery`/`LogQuery`
  - Support: `service_name = 'api'`, `start_time_ns BETWEEN x AND y`, `duration_ns > x`
  - Unsupported filters handled by DataFusion after conversion

- **Lazy Conversion:** Only convert spans that pass HashMap filters

### Phase 3: RecordBatch Conversion Helpers (3 days)

**Add to `cold_tier.rs`:**

```rust
fn record_batches_to_spans(batches: Vec<RecordBatch>) -> Result<Vec<Span>> {
    let mut spans = Vec::new();

    for batch in batches {
        // Extract columns
        let trace_ids = batch.column(0)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()?;
        let span_ids = batch.column(1)
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()?;
        // ... other columns ...

        // Build Span structs
        for i in 0..batch.num_rows() {
            spans.push(Span {
                trace_id: TraceId::from_bytes(trace_ids.value(i).try_into()?),
                span_id: SpanId::from_bytes(span_ids.value(i).try_into()?),
                // ... other fields ...
            });
        }
    }

    Ok(spans)
}

// Similar for:
// - record_batches_to_logs()
// - record_batches_to_metrics()
// - record_batches_to_profiles()
```

### Phase 4: Optimization (3 days)

**4.1. Enable Bloom Filters** (`cold_tier.rs` line ~194)
```rust
let props = WriterProperties::builder()
    .set_compression(self.config.compression.into())
    .set_row_group_size(self.config.row_group_size)
    .set_bloom_filter_enabled("trace_id".into(), true)
    .set_bloom_filter_enabled("span_id".into(), true)
    .set_bloom_filter_enabled("service_name".into(), true)
    .set_bloom_filter_fpp(0.01)  // 1% false positive
    .set_statistics_enabled(true)
    .build();
```

**Expected Impact:** 90%+ reduction in files scanned for trace_id lookups

**4.2. File Consolidation Job**
```rust
pub async fn consolidate_small_files(&self, time_bucket: &str) -> Result<usize> {
    let prefix = format!("spans/{}/", time_bucket);

    // 1. List files < 128MB
    let small_files = self.list_small_files(&prefix, 128 * 1024 * 1024).await?;

    if small_files.len() < 10 {
        return Ok(0);  // Not worth consolidating
    }

    // 2. Read via DataFusion
    let pattern = format!("{}/**/*.parquet", prefix);
    let df = self.ctx.read_parquet(&pattern, Default::default()).await?;

    // 3. Write consolidated file
    let consolidated_path = format!("{}/consolidated-{}.parquet", prefix, Uuid::new_v4());
    df.write_parquet(&consolidated_path, None).await?;

    // 4. Delete small files
    for file in small_files {
        self.store.delete(&file.location).await?;
    }

    Ok(small_files.len())
}
```

**Schedule:** Run weekly during maintenance window

### Phase 5: Storage Simplification (2 days)

**Simplify `storage.rs` QueryApi implementation:**

Replace lines 236-307 (manual hot/cold branching) with:
```rust
impl QueryApi for Storage {
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<QueryTrace>> {
        // DataFusion handles hot + cold tier unification
        let spans = self.cold_tier.query_traces(&query).await?;

        // Convert Spans → QueryTrace (aggregate by trace_id)
        self.spans_to_query_traces(spans)
    }

    // Similar simplification for query_logs, query_metrics, query_profiles
}
```

**Lines Removed:** ~100 LOC of manual tier coordination
**Lines Added:** ~30 LOC of simple delegation
**Net Reduction:** ~70 LOC (cleaner, more maintainable)

### Phase 6: Testing (1 week)

**6.1. Unit Tests**
- `hot_tier_provider`: TableProvider trait methods
- `hot_tier_exec`: ExecutionPlan trait methods
- Filter translation: DataFusion Expr → query structs
- RecordBatch conversion: Arrow ↔ models

**6.2. Integration Tests** (`crates/sequins-storage/tests/unified_queries.rs`)
```rust
#[tokio::test]
async fn test_unified_query_across_tiers() {
    // 1. Insert data into hot tier
    let storage = create_test_storage().await;
    storage.ingest_spans(create_test_spans()).await?;

    // 2. Flush half to cold tier
    storage.run_maintenance().await?;

    // 3. Query with SQL that spans both tiers
    let query = TraceQuery {
        service: Some("test-service".into()),
        start_time: week_ago,
        end_time: now,
        limit: Some(100),
        ..Default::default()
    };

    let results = storage.query_traces(query).await?;

    // 4. Verify results from both tiers
    assert_eq!(results.len(), 10);
    assert!(results.iter().any(|t| t.from_hot_tier));
    assert!(results.iter().any(|t| t.from_cold_tier));
}
```

**6.3. Performance Benchmarks**
- Write latency: <10μs (unchanged from current)
- Query latency: <10ms for hot+cold queries
- Conversion overhead: <5% of total query time
- Memory overhead: <10% during queries

---

## Current Codebase State

### Modified Files

1. **`crates/sequins-core/src/traits/storage.rs`** - NEW ✅
   - StorageRead, StorageWrite, TierMetadata traits
   - Documented with examples

2. **`crates/sequins-core/src/traits/mod.rs`** - Modified ✅
   - Exports new storage traits

3. **`crates/sequins-storage/Cargo.toml`** - Modified ✅
   - Added `datafusion = { workspace = true }`
   - Added `url = { workspace = true }`

4. **`crates/sequins-storage/src/cold_tier.rs`** - Modified ✅
   - Added `SessionContext` field
   - Added `url::Url` import
   - Registered object store with DataFusion
   - Placeholder query methods (return empty Vec)

5. **`crates/sequins-storage/src/storage.rs`** - Modified ✅
   - Basic cross-tier queries implemented (lines 236-373)
   - Queries hot tier first, then cold tier
   - Merges and deduplicates results
   - **TO BE SIMPLIFIED** in Phase 5

### Test Status

All 40 existing tests passing ✅

```bash
$ cargo test -p sequins-storage
running 40 tests
...
test result: ok. 40 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

---

## Next Actions (START HERE)

### Immediate Next Step: Phase 1 Prototype

**Task 1:** Add MemTable prototype method to `ColdTier`

```rust
// In crates/sequins-storage/src/cold_tier.rs

impl ColdTier {
    /// PROTOTYPE: Test unified queries with MemTable
    /// This is a quick validation - will be replaced with custom TableProvider
    #[cfg(test)]
    pub async fn query_traces_memtable_prototype(
        &self,
        hot_tier: &HotTier,
        query: &TraceQuery,
    ) -> Result<Vec<Span>> {
        use datafusion::datasource::MemTable;

        // Convert hot tier to RecordBatch
        let hot_spans = hot_tier.get_all_spans();
        let hot_batch = Self::spans_to_record_batch(hot_spans, arrow_schema::span_schema())?;

        // Register as MemTable
        let hot_table = MemTable::try_new(
            arrow_schema::span_schema(),
            vec![vec![hot_batch]],
        )?;
        self.ctx.register_table("hot_spans_mem", Arc::new(hot_table))?;

        // Build unified SQL query
        let sql = format!("
            SELECT * FROM (
                SELECT * FROM hot_spans_mem
                UNION ALL
                SELECT * FROM read_parquet('{}/spans/**/*.parquet')
            )
            WHERE start_time_ns >= {}
              AND start_time_ns <= {}
            ORDER BY start_time_ns DESC
            LIMIT {}
        ",
            self.config.uri,
            query.start_time.as_nanos(),
            query.end_time.as_nanos(),
            query.limit.unwrap_or(100)
        );

        // Execute query
        let df = self.ctx.sql(&sql).await
            .map_err(|e| Error::Storage(format!("SQL query failed: {}", e)))?;

        let batches = df.collect().await
            .map_err(|e| Error::Storage(format!("Failed to collect results: {}", e)))?;

        // Convert back to Spans
        Self::record_batches_to_spans(batches)
    }
}
```

**Task 2:** Add method to get all spans from HotTier (for prototype only)

```rust
// In crates/sequins-storage/src/hot_tier.rs

impl HotTier {
    /// PROTOTYPE ONLY: Get all spans for testing
    /// This is inefficient - real implementation will use iterator
    #[cfg(test)]
    pub fn get_all_spans(&self) -> Vec<Span> {
        let pin = self.spans.pin();
        pin.iter()
            .map(|(_, entry)| entry.data.clone())
            .collect()
    }
}
```

**Task 3:** Add RecordBatch → Span conversion stub

```rust
// In crates/sequins-storage/src/cold_tier.rs

fn record_batches_to_spans(batches: Vec<RecordBatch>) -> Result<Vec<Span>> {
    // TODO: Implement full conversion
    // For now, return empty to unblock prototype testing
    tracing::warn!("record_batches_to_spans not yet implemented - returning empty");
    Ok(Vec::new())
}
```

**Task 4:** Write prototype test

```rust
// In crates/sequins-storage/tests/memtable_prototype.rs (NEW FILE)

#[tokio::test]
async fn test_memtable_unified_query() {
    let temp_dir = TempDir::new().unwrap();
    let config = create_test_config(&temp_dir);

    let hot_tier = Arc::new(HotTier::new(config.hot_tier.clone()));
    let cold_tier = ColdTier::new(config.cold_tier.clone()).unwrap();

    // Insert test data
    let span = create_test_span();
    hot_tier.insert_span(span.clone()).unwrap();

    // Query via MemTable
    let query = TraceQuery {
        start_time: Timestamp::MIN,
        end_time: Timestamp::MAX,
        limit: Some(10),
        ..Default::default()
    };

    let results = cold_tier
        .query_traces_memtable_prototype(&hot_tier, &query)
        .await;

    // For now, just verify it doesn't crash
    assert!(results.is_ok());
}
```

### Success Criteria for Phase 1

✅ **Functional:**
- Prototype compiles
- Test runs without panicking
- SQL query executes successfully

✅ **Performance (to measure):**
- Query latency for 1,000 spans
- Query latency for 10,000 spans
- Memory usage during query

✅ **Decision Gate:**
- If latency <20ms → proceed to Phase 2
- If latency >50ms → investigate or abort
- If implementation too complex → abort

---

## Risk Mitigation

### Risk 1: TableProvider Complexity

**Mitigation:** Start with MemTable prototype (Phase 1) to validate concept before investing in custom TableProvider.

**Abort Criteria:** If prototype shows >50ms latency or >50% memory overhead, stop and keep dual-query approach.

### Risk 2: Filter Translation Complexity

**Mitigation:** Start with simple filters (equality, range) in Phase 2. Add complex filters incrementally.

**Fallback:** If filter translation proves too complex, skip pushdown and do post-filtering in DataFusion (still works, just slower).

### Risk 3: Performance Regression

**Mitigation:** Add benchmarks in Phase 1. Compare against current dual-query approach.

**Abort Criteria:** If >2x slower than current approach, stop and investigate.

---

## Useful Commands

```bash
# Build storage crate
cargo build -p sequins-storage

# Run tests
cargo test -p sequins-storage

# Run specific test
cargo test -p sequins-storage test_memtable_unified_query

# Run benchmarks (once added)
cargo bench -p sequins-storage

# Check code
cargo clippy -p sequins-storage

# Format code
cargo fmt -p sequins-storage
```

---

## Reference Links

**Production Systems:**
- [InfluxDB IOx Architecture](https://www.influxdata.com/blog/intro-influxdb-iox/)
- [Grafana Tempo Architecture](https://grafana.com/docs/tempo/latest/operations/architecture/)

**DataFusion:**
- [DataFusion TableProvider](https://docs.rs/datafusion/latest/datafusion/datasource/trait.TableProvider.html)
- [DataFusion ExecutionPlan](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)

**Papaya HashMap:**
- [Designing Papaya](https://ibraheem.ca/posts/designing-papaya/)
- [Papaya Benchmarks](https://github.com/ibraheemdev/papaya/blob/master/BENCHMARKS.md)

---

## Questions & Decisions Log

**Q1:** Should we use Arrow for hot tier?
**A1:** No. HashMap is 10-100x faster for writes. Keep Papaya HashMap. ✅

**Q2:** How to query across hot and cold tiers?
**A2:** Custom DataFusion TableProvider with lazy conversion. ✅

**Q3:** What about frequent flushing instead?
**A3:** No. Creates 200k+ files/week, expensive queries, stale data. ❌

**Q4:** Should we implement dual representation (HashMap + RecordBatch)?
**A4:** No. 100% memory overhead, slower writes, complex sync. ❌

**Q5:** What's the performance target?
**A5:** <10ms queries, <5% overhead, instant freshness. ✅

---

## TODO List (Current State)

```
[✅] 1. Create storage traits (StorageRead, StorageWrite, TierMetadata)
[✅] 2. Add DataFusion dependencies
[✅] 3. Research: HashMap vs Arrow for hot tier
[✅] 4. Research: Query unification strategies
[✅] 5. Decision: Keep HashMap, use TableProvider

[ ] 6. Phase 1: MemTable prototype 🎯 START HERE
[ ] 7. Add hot_tier.get_all_spans() method
[ ] 8. Add cold_tier.query_traces_memtable_prototype()
[ ] 9. Add record_batches_to_spans() stub
[ ] 10. Write prototype test
[ ] 11. Benchmark prototype performance
[ ] 12. Decision gate: proceed or abort?

[ ] 13. Phase 2: Custom TableProvider
[ ] 14. Create hot_tier_provider.rs
[ ] 15. Create hot_tier_exec.rs
[ ] 16. Implement filter translation
[ ] 17. Phase 3: RecordBatch conversions
[ ] 18. Phase 4: Bloom filters + consolidation
[ ] 19. Phase 5: Simplify Storage
[ ] 20. Phase 6: Integration tests
```

---

## Estimated Timeline

| Phase | Duration | Status |
|-------|----------|--------|
| Research | 1 day | ✅ Complete |
| Phase 1: Prototype | 1 week | 🎯 Next |
| Phase 2: TableProvider | 2 weeks | Pending |
| Phase 3: Conversions | 3 days | Pending |
| Phase 4: Optimization | 3 days | Pending |
| Phase 5: Simplification | 2 days | Pending |
| Phase 6: Testing | 1 week | Pending |
| **Total** | **3-4 weeks** | **In Progress** |

---

## Context for Next Session

**What we know:**
1. ✅ HashMap for hot tier is correct (validated against InfluxDB IOx)
2. ✅ Custom TableProvider is the solution (lazy conversion pattern)
3. ✅ Architecture is sound (production-proven pattern)
4. ✅ Storage traits created (useful for future distributed work)

**What we're doing:**
- 🎯 Phase 1: MemTable prototype to validate concept
- 📊 Measure performance before committing to full implementation
- 🚪 Decision gate after prototype

**What's blocking:**
- Nothing! Ready to implement Phase 1

**Next concrete action:**
Add `query_traces_memtable_prototype()` method to `cold_tier.rs` as documented above.

---

**End of Checkpoint Document**

*Resume from here in next session.*
