# Sequins Storage Completion Tasks

**Date:** 2025-01-07
**Status:** Phase 1 infrastructure complete, core functionality pending
**Test Coverage:** 40 tests passing in `sequins-storage`

---

## Executive Summary

The `sequins-storage` crate has **strong foundational infrastructure** in place:
- ✅ Hot tier (Papaya HashMap) fully implemented with eviction
- ✅ Cold tier (Parquet + DataFusion) infrastructure in place
- ✅ All three API traits (OtlpIngest, QueryApi, ManagementApi) defined
- ✅ DataFusion TableProvider and ExecutionPlan scaffolding exists
- ✅ RecordBatch conversion methods implemented (to/from Arrow)
- ✅ Flush mechanisms between hot and cold tiers working
- ✅ 40 passing tests

**What's missing:** Production query implementation, retention management, and background automation.

---

## Critical Missing Features (Must Complete Before Production)

### 1. ❌ **DataFusion Query Implementation** (HIGH PRIORITY)

**Status:** Methods exist but return empty vectors (stubs)

**Location:** `crates/sequins-storage/src/cold_tier.rs`

**Missing implementations:**
```rust
// Line 541
pub async fn query_traces(&self, _query: &TraceQuery) -> Result<Vec<Span>> {
    // TODO: Implement DataFusion query for traces
    Ok(Vec::new())  // Currently returns empty!
}

// Line 552
pub async fn query_logs(&self, _query: &LogQuery) -> Result<Vec<LogEntry>> {
    // TODO: Implement DataFusion query for logs
    Ok(Vec::new())
}

// Line 562
pub async fn query_metrics(&self, _query: &MetricQuery) -> Result<Vec<Metric>> {
    // TODO: Implement DataFusion query for metrics
    Ok(Vec::new())
}

// Line 572
pub async fn query_profiles(&self, _query: &ProfileQuery) -> Result<Vec<Profile>> {
    // TODO: Implement DataFusion query for profiles
    Ok(Vec::new())
}
```

**What needs to be done:**

1. **Implement SQL query generation from query structs**
   ```rust
   fn trace_query_to_sql(query: &TraceQuery) -> String {
       let mut conditions = Vec::new();

       if let Some(service) = &query.service_name {
           conditions.push(format!("service_name = '{}'", service));
       }

       conditions.push(format!("start_time_ns >= {}", query.start_time.as_nanos()));
       conditions.push(format!("start_time_ns <= {}", query.end_time.as_nanos()));

       if let Some(min_duration) = query.min_duration {
           conditions.push(format!("duration_ns >= {}", min_duration.as_nanos()));
       }

       let where_clause = conditions.join(" AND ");
       let limit_clause = query.limit.map(|l| format!(" LIMIT {}", l)).unwrap_or_default();

       format!(
           "SELECT * FROM read_parquet('{}/spans/**/*.parquet') WHERE {} ORDER BY start_time_ns DESC{}",
           self.config.uri, where_clause, limit_clause
       )
   }
   ```

2. **Execute DataFusion query**
   ```rust
   let sql = trace_query_to_sql(query);
   let df = self.ctx.sql(&sql).await?;
   let batches = df.collect().await?;
   ```

3. **Convert RecordBatches back to model types**
   ```rust
   let spans = Self::record_batches_to_spans(batches)?;
   Ok(spans)
   ```

**Effort:** ~2-3 days for all four query types

**Dependencies:** None (RecordBatch conversion methods already implemented)

**Tests to add:**
- `tests/cold_tier_queries.rs` - Test each query type
- Verify filtering works correctly
- Verify pagination/limits work
- Verify time range queries work

---

### 2. ❌ **Span to QueryTrace Aggregation** (HIGH PRIORITY)

**Status:** Conversion stub in place, not implemented

**Location:** `crates/sequins-storage/src/storage.rs:251`

**Missing implementation:**
```rust
// Currently commented out:
// TODO: Convert spans to QueryTrace and merge with hot tier results
```

**What QueryTrace needs:**
```rust
pub struct QueryTrace {
    pub trace_id: TraceId,
    pub root_span: Span,
    pub total_spans: usize,
    pub services: Vec<String>,
    pub duration: Duration,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
    pub error_count: usize,
}
```

**Implementation needed:**
```rust
fn spans_to_query_traces(spans: Vec<Span>) -> Vec<QueryTrace> {
    use std::collections::HashMap;

    // Group spans by trace_id
    let mut traces: HashMap<TraceId, Vec<Span>> = HashMap::new();
    for span in spans {
        traces.entry(span.trace_id).or_default().push(span);
    }

    // Convert each group to QueryTrace
    traces.into_iter().map(|(trace_id, mut spans)| {
        // Find root span (parent_span_id is None)
        let root_span = spans.iter()
            .find(|s| s.parent_span_id.is_none())
            .cloned()
            .unwrap_or_else(|| spans[0].clone());

        // Calculate aggregates
        let services: Vec<String> = spans.iter()
            .map(|s| s.service_name.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let start_time = spans.iter().map(|s| s.start_time).min().unwrap();
        let end_time = spans.iter().map(|s| s.end_time).max().unwrap();
        let duration = end_time - start_time;

        let error_count = spans.iter()
            .filter(|s| s.status_code == StatusCode::Error)
            .count();

        QueryTrace {
            trace_id,
            root_span,
            total_spans: spans.len(),
            services,
            duration,
            start_time,
            end_time,
            error_count,
        }
    }).collect()
}
```

**Effort:** ~1 day

**Dependencies:** Requires `StatusCode` enum in core models

---

### 3. ❌ **Retention Cleanup Implementation** (HIGH PRIORITY)

**Status:** Stubbed out, returns 0

**Location:** `crates/sequins-storage/src/storage.rs:387-390`

**Current stub:**
```rust
async fn run_retention_cleanup(&self) -> Result<usize> {
    // TODO: Implement actual retention cleanup logic
    Ok(0)
}
```

**Implementation needed:**
```rust
async fn run_retention_cleanup(&self) -> Result<usize> {
    let now = Timestamp::now()?;
    let policy = self.get_retention_policy().await?;

    let cold_tier = self.cold_tier.write().await;

    let mut deleted = 0;

    // Delete old spans
    if let Some(span_cutoff) = now.checked_sub(policy.spans_retention) {
        deleted += cold_tier.delete_spans_before(span_cutoff).await?;
    }

    // Delete old logs
    if let Some(log_cutoff) = now.checked_sub(policy.logs_retention) {
        deleted += cold_tier.delete_logs_before(log_cutoff).await?;
    }

    // Delete old metrics
    if let Some(metric_cutoff) = now.checked_sub(policy.metrics_retention) {
        deleted += cold_tier.delete_metrics_before(metric_cutoff).await?;
    }

    // Delete old profiles
    if let Some(profile_cutoff) = now.checked_sub(policy.profiles_retention) {
        deleted += cold_tier.delete_profiles_before(profile_cutoff).await?;
    }

    Ok(deleted)
}
```

**New methods needed in ColdTier:**
```rust
impl ColdTier {
    pub async fn delete_spans_before(&self, cutoff: Timestamp) -> Result<usize> {
        // List Parquet files in spans/**/*.parquet
        let prefix = format!("{}/spans/", self.config.uri);
        let files = self.store.list(Some(&prefix.into())).await?;

        let mut deleted = 0;

        // Read file metadata, check min timestamp
        for file in files {
            if let Some(min_time) = self.get_file_min_timestamp(&file.location).await? {
                if min_time < cutoff {
                    self.store.delete(&file.location).await?;
                    deleted += 1;
                }
            }
        }

        Ok(deleted)
    }

    async fn get_file_min_timestamp(&self, path: &Path) -> Result<Option<Timestamp>> {
        // Read Parquet metadata
        let file = self.store.get(path).await?;
        let reader = ParquetRecordBatchReader::try_new(file, 1024)?;

        // Get min timestamp from statistics
        if let Some(metadata) = reader.metadata().row_group(0).column(4).statistics() {
            // Column 4 is start_time_ns
            if let Some(min) = metadata.min_bytes() {
                let nanos = i64::from_le_bytes(min.try_into()?);
                return Ok(Some(Timestamp::from_nanos(nanos)?));
            }
        }

        Ok(None)
    }
}
```

**Effort:** ~2-3 days

**Dependencies:** Requires Parquet metadata reading

---

### 4. ❌ **Retention Policy Persistence** (MEDIUM PRIORITY)

**Status:** Stubbed out

**Location:** `crates/sequins-storage/src/storage.rs:393-400`

**Current stub:**
```rust
async fn update_retention_policy(&self, _policy: RetentionPolicy) -> Result<()> {
    // TODO: Implement retention policy persistence
    Ok(())
}
```

**Implementation options:**

**Option A: Store in object_store metadata file**
```rust
async fn update_retention_policy(&self, policy: RetentionPolicy) -> Result<()> {
    let json = serde_json::to_string(&policy)?;
    let bytes = json.into_bytes();

    let path = format!("{}/.retention_policy.json", self.config.lifecycle.uri);
    self.cold_tier.write().await.store.put(&path.into(), bytes.into()).await?;

    Ok(())
}

async fn get_retention_policy(&self) -> Result<RetentionPolicy> {
    let path = format!("{}/.retention_policy.json", self.config.lifecycle.uri);

    match self.cold_tier.read().await.store.get(&path.into()).await {
        Ok(data) => {
            let bytes = data.bytes().await?;
            let policy = serde_json::from_slice(&bytes)?;
            Ok(policy)
        }
        Err(_) => {
            // File doesn't exist, return default from config
            Ok(self.config.lifecycle.default_retention_policy())
        }
    }
}
```

**Option B: Store in dedicated SQLite metadata database**
- More structured, easier to query
- Allows storing other metadata (stats, config history, etc.)
- Slightly more complex

**Recommendation:** Option A for simplicity

**Effort:** ~1 day

---

### 5. ❌ **Filter Pushdown Translation** (MEDIUM PRIORITY)

**Status:** Partial implementation, needs completion

**Location:** `crates/sequins-storage/src/hot_tier_exec.rs:96`

**Current stub:**
```rust
// TODO: Implement full Expr -> predicate translation
```

**Implementation needed:**
```rust
fn translate_filters(filters: &[Expr]) -> QueryPredicate {
    let mut predicate = QueryPredicate::default();

    for filter in filters {
        match filter {
            // service_name = 'value'
            Expr::BinaryExpr { left, op, right } if matches!(op, Operator::Eq) => {
                if let (Expr::Column(col), Expr::Literal(ScalarValue::Utf8(Some(val)))) = (&**left, &**right) {
                    match col.name.as_str() {
                        "service_name" => predicate.service_name = Some(val.clone()),
                        _ => {}
                    }
                }
            }

            // start_time_ns >= value
            Expr::BinaryExpr { left, op, right } if matches!(op, Operator::GtEq) => {
                if let (Expr::Column(col), Expr::Literal(ScalarValue::Int64(Some(val)))) = (&**left, &**right) {
                    if col.name == "start_time_ns" {
                        predicate.min_start_time = Some(Timestamp::from_nanos(*val)?);
                    }
                }
            }

            // duration_ns > value
            Expr::BinaryExpr { left, op, right } if matches!(op, Operator::Gt) => {
                if let (Expr::Column(col), Expr::Literal(ScalarValue::Int64(Some(val)))) = (&**left, &**right) {
                    if col.name == "duration_ns" {
                        predicate.min_duration = Some(Duration::from_nanos(*val)?);
                    }
                }
            }

            _ => {
                // Unsupported filters will be applied by DataFusion after conversion
            }
        }
    }

    predicate
}
```

**Effort:** ~2 days

**Dependencies:** Need to define `QueryPredicate` struct

---

## Additional Features (Phase 2-8 from Roadmap)

### 6. ⏸️ **Background Flush Task** (Phase 2)

**Status:** Manual flush methods exist, automatic task missing

**What exists:**
- ✅ `Storage::flush_spans()` - Manual flush
- ✅ `Storage::run_maintenance_internal()` - Periodic maintenance
- ✅ Eviction logic based on age

**What's missing:**
- Background tokio task that runs `run_maintenance_internal()` every N minutes
- Configuration for flush interval (exists in config, not used in background task)

**Implementation:**
```rust
impl Storage {
    pub fn start_background_tasks(self: Arc<Self>) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(
                std::time::Duration::from_secs(self.config.lifecycle.flush_interval.as_secs())
            );

            loop {
                interval.tick().await;

                if let Err(e) = self.run_maintenance_internal().await {
                    tracing::error!("Background maintenance failed: {}", e);
                }
            }
        })
    }
}
```

**Effort:** ~1 day

**Roadmap:** Phase 2 task

---

### 7. ⏸️ **Parquet File Consolidation** (Phase 8)

**Status:** Not implemented

**Purpose:** Merge small Parquet files into larger ones for better query performance

**Implementation:**
```rust
impl ColdTier {
    pub async fn consolidate_small_files(&self, time_bucket: &str, min_size: usize) -> Result<usize> {
        let prefix = format!("{}/spans/{}/", self.config.uri, time_bucket);

        // List files smaller than min_size
        let small_files: Vec<_> = self.store.list(Some(&prefix.into()))
            .await?
            .filter(|f| f.size < min_size)
            .collect()
            .await?;

        if small_files.len() < 10 {
            return Ok(0);  // Not worth consolidating
        }

        // Read all data via DataFusion
        let pattern = format!("{}/**/*.parquet", prefix);
        let df = self.ctx.read_parquet(&pattern, Default::default()).await?;

        // Write consolidated file
        let consolidated_path = format!("{}/consolidated-{}.parquet", prefix, uuid::Uuid::new_v4());
        df.write_parquet(&consolidated_path, None).await?;

        // Delete small files
        for file in small_files {
            self.store.delete(&file.location).await?;
        }

        Ok(small_files.len())
    }
}
```

**Effort:** ~2 days

**Roadmap:** Phase 8 task

---

### 8. ⏸️ **Cloud Storage Support** (Future)

**Status:** LocalFileSystem only

**Location:** `crates/sequins-storage/src/cold_tier.rs:42`

**Current limitation:**
```rust
// TODO: Add S3, GCS, Azure support
let store: Arc<dyn ObjectStore> = Arc::new(LocalFileSystem::new_with_prefix(&path)?);
```

**Implementation:**
```rust
use object_store::aws::AmazonS3Builder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::azure::MicrosoftAzureBuilder;

fn create_object_store(config: &ColdTierConfig) -> Result<Arc<dyn ObjectStore>> {
    match config.storage_type {
        StorageType::Local => {
            Ok(Arc::new(LocalFileSystem::new_with_prefix(&config.uri)?))
        }
        StorageType::S3 => {
            let builder = AmazonS3Builder::from_env()
                .with_bucket_name(&config.bucket)
                .with_region(&config.region);
            Ok(Arc::new(builder.build()?))
        }
        StorageType::GCS => {
            let builder = GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(&config.bucket);
            Ok(Arc::new(builder.build()?))
        }
        StorageType::Azure => {
            let builder = MicrosoftAzureBuilder::from_env()
                .with_container_name(&config.container);
            Ok(Arc::new(builder.build()?))
        }
    }
}
```

**Effort:** ~3-4 days (including testing with MinIO/LocalStack)

**Roadmap:** Not in current phases, but needed for enterprise deployment

---

## Test Coverage Gaps

**Current:** 40 tests passing

**Missing test coverage:**

1. **Integration tests for unified queries** ✅ Planned in checkpoint
   - `tests/unified_queries.rs`
   - Test queries spanning hot + cold tiers
   - Test filtering, pagination, aggregation

2. **Retention cleanup tests**
   - `tests/retention_cleanup.rs`
   - Test file deletion based on age
   - Test policy changes
   - Test partial failures

3. **Cloud storage integration tests**
   - `tests/s3_integration.rs` (requires MinIO/LocalStack)
   - `tests/gcs_integration.rs`
   - `tests/azure_integration.rs`

4. **Performance benchmarks**
   - `benches/query_performance.rs`
   - Benchmark hot vs cold vs unified queries
   - Benchmark conversion overhead
   - Benchmark filter pushdown effectiveness

**Effort:** ~1 week for complete test suite

---

## Priority Roadmap

### Week 1: Core Query Functionality ⭐⭐⭐ CRITICAL
1. Implement DataFusion queries in ColdTier (2-3 days)
2. Implement Span → QueryTrace conversion (1 day)
3. Add integration tests for queries (1 day)
4. **Deliverable:** Working end-to-end queries across hot + cold tiers

### Week 2: Retention Management ⭐⭐ HIGH
1. Implement retention cleanup logic (2 days)
2. Implement retention policy persistence (1 day)
3. Add retention tests (1 day)
4. **Deliverable:** Automatic data deletion based on age

### Week 3: Background Automation ⭐ MEDIUM
1. Implement background flush task (1 day)
2. Complete filter pushdown translation (2 days)
3. Add performance benchmarks (1 day)
4. **Deliverable:** Fully automated storage lifecycle

### Week 4+: Production Hardening ⏸️ FUTURE
1. Parquet file consolidation (2 days)
2. Cloud storage support (3-4 days)
3. Additional optimization (ongoing)

---

## Summary

**Status:** `sequins-storage` has excellent infrastructure (~80% complete) but needs **production query and retention implementations** to be fully functional.

**Critical path to completion:**
1. ✅ Infrastructure (DONE - 40 tests passing)
2. ❌ **Query implementation** (2-3 days) ← START HERE
3. ❌ **QueryTrace aggregation** (1 day)
4. ❌ **Retention cleanup** (2-3 days)
5. ⏸️ Background automation (1-2 days)
6. ⏸️ Production hardening (ongoing)

**Recommended next action:** Implement DataFusion queries in `ColdTier` - this unblocks everything else and makes the storage layer actually queryable.

---

**Last Updated:** 2025-01-07
