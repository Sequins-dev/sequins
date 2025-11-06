# Sequins Storage Pattern Linter

**Purpose:** Ensure all query methods follow the consistent hot → cold tier pattern.

**When to use:**
- After implementing new query methods
- Before committing storage changes
- During code review of Storage implementations

**Invocation:** `sequins-storage-patterns` or automatically after implementing query methods

---

## What This Skill Does

The Storage architecture uses a two-tier query pattern:
1. **Hot tier** - Check in-memory Papaya HashMap (fast, recent data)
2. **Cold tier** - Query Parquet files via DataFusion (slower, historical data)
3. **Merge** - Combine results and deduplicate
4. **Limit** - Apply pagination/limits

This skill verifies that ALL query methods follow this pattern consistently.

---

## The Standard Query Pattern

### Template for Query Methods

```rust
async fn query_TYPE(&self, query: TYPEQuery) -> Result<Vec<TYPE>> {
    // 1. HOT TIER: Check in-memory data first
    let hot_results = self.query_hot_tier(query.clone()).await?;

    // 2. EARLY RETURN: If hot tier has enough results
    if hot_results.len() >= query.limit && query.only_recent() {
        return Ok(hot_results);
    }

    // 3. COLD TIER: Query historical data if needed
    let cold_results = self.query_cold_tier(query.clone()).await?;

    // 4. MERGE: Combine and deduplicate
    let mut all_results = self.merge_and_deduplicate(hot_results, cold_results);

    // 5. SORT: Apply ordering
    all_results.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

    // 6. LIMIT: Apply pagination
    all_results.truncate(query.limit);

    Ok(all_results)
}
```

---

## Required Steps in Every Query Method

### Step 1: Hot Tier Query (< 1ms)

```rust
// ✅ GOOD - Check hot tier first
let hot_results = {
    let guard = self.hot.traces.pin();
    guard
        .iter()
        .filter(|(_, trace)| query.matches(trace))
        .map(|(_, trace)| trace.clone())
        .collect::<Vec<_>>()
};

// ❌ BAD - Skipping hot tier
let results = self.query_parquet(query).await?;
```

**Why:** 90-95% of queries are for recent data. Hot tier is 50-100x faster than cold tier.

**Key patterns:**
- Use `.pin()` to get a guard
- Guard scope should be as small as possible
- Clone results out before guard drops
- Never hold guard across `.await`

---

### Step 2: Early Return Optimization

```rust
// ✅ GOOD - Return early if possible
if hot_results.len() >= query.limit && query.within_hot_window() {
    return Ok(hot_results.into_iter().take(query.limit).collect());
}

// ❌ BAD - Always querying cold tier
let cold_results = self.query_cold_tier(query).await?;  // Wasteful!
```

**Why:** If user asks for 10 recent traces and hot tier has 50, no need to query cold tier.

**Conditions for early return:**
- Hot tier has enough results (`len() >= limit`)
- Query time range is entirely within hot window
- No specific filtering that might miss results

---

### Step 3: Cold Tier Query (15-35ms)

```rust
// ✅ GOOD - Use DataFusion for SQL-like query
let cold_results = {
    let ctx = self.cold.get_datafusion_context().await?;

    // Build SQL query
    let sql = format!(
        "SELECT * FROM traces WHERE timestamp >= {} AND timestamp <= {} LIMIT {}",
        query.start_time.as_nanos(),
        query.end_time.as_nanos(),
        query.limit
    );

    // Or use DataFrame API (preferred)
    let df = ctx
        .table("traces").await?
        .filter(col("timestamp").gt_eq(lit(query.start_time.as_nanos())))?
        .filter(col("timestamp").lt_eq(lit(query.end_time.as_nanos())))?
        .limit(0, Some(query.limit))?;

    // Convert RecordBatch to Rust types
    let batches = df.collect().await?;
    self.record_batches_to_traces(batches)?
};
```

**Why:** DataFusion provides optimized query execution over Parquet files.

**Key patterns:**
- Use DataFrame API (more type-safe than SQL strings)
- Apply filters and limits in DataFusion (pushdown optimization)
- Convert RecordBatch results to Rust types after query
- Handle errors gracefully (cold tier might be empty)

---

### Step 4: Merge and Deduplicate

```rust
// ✅ GOOD - Deduplicate by ID
use std::collections::HashMap;

let mut seen = HashMap::new();
let mut merged = Vec::new();

// Hot tier results first (more recent)
for trace in hot_results {
    seen.insert(trace.trace_id, true);
    merged.push(trace);
}

// Cold tier results, skip duplicates
for trace in cold_results {
    if !seen.contains_key(&trace.trace_id) {
        seen.insert(trace.trace_id, true);
        merged.push(trace);
    }
}

// ❌ BAD - No deduplication
let mut merged = hot_results;
merged.extend(cold_results);  // May have duplicates!
```

**Why:** Data might exist in both tiers during the flush window (5-15 minutes). Without deduplication, users see duplicates.

**Key patterns:**
- Use HashSet or HashMap to track seen IDs
- Process hot tier first (prefer newer data)
- Skip cold tier items that are duplicates

---

### Step 5: Sort Results

```rust
// ✅ GOOD - Sort by timestamp, newest first
merged.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

// ✅ ALSO GOOD - Sort by multiple fields
merged.sort_by(|a, b| {
    b.timestamp
        .cmp(&a.timestamp)
        .then_with(|| a.service.cmp(&b.service))
});

// ❌ BAD - Not sorting
// Results are in random order!
```

**Why:** Users expect results in a predictable order (usually newest first).

**Common orderings:**
- Traces: by start_time desc
- Logs: by timestamp desc
- Metrics: by timestamp asc (for time series)
- Spans: by parent_id, then start_time asc (for tree structure)

---

### Step 6: Apply Limit

```rust
// ✅ GOOD - Truncate to limit
merged.truncate(query.limit);

// ✅ ALSO GOOD - Take iterator
let limited: Vec<_> = merged.into_iter().take(query.limit).collect();

// ❌ BAD - Returning more than requested
Ok(merged)  // Might have 10,000 results when limit was 100!
```

**Why:** Prevents overwhelming UI, controls memory usage.

**Key patterns:**
- Always respect `query.limit`
- Default limit if not provided: 100 or 1000
- Maximum limit: 10,000 (reject higher)

---

## Verification Checklist

For each query method in Storage:

### Basic Pattern
- [ ] Queries hot tier first
- [ ] Has early return optimization (if applicable)
- [ ] Queries cold tier if needed
- [ ] Merges and deduplicates results
- [ ] Sorts results appropriately
- [ ] Applies limit

### Error Handling
- [ ] Returns `Result<T>` with proper error type
- [ ] Handles empty results gracefully
- [ ] Propagates errors with context (using `.context()`?)
- [ ] Cold tier errors don't fail entire query (optional: return hot tier results)

### Performance
- [ ] Hot tier guard scope is minimal
- [ ] No guard held across `.await`
- [ ] Uses DataFusion pushdown (filters in SQL/DataFrame API)
- [ ] Clones only what's needed
- [ ] Limit applied as early as possible

### Correctness
- [ ] Deduplication uses correct ID field
- [ ] Time range filtering is correct (inclusive/exclusive)
- [ ] Edge cases handled (empty hot tier, empty cold tier, both empty)
- [ ] Results match query parameters

---

## Common Violations

### Violation 1: Skipping Hot Tier

```rust
// ❌ BAD
async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>> {
    // Directly querying cold tier!
    self.cold.query_traces(query).await
}
```

**Impact:** 50-100x slower for recent queries (most queries)

**Fix:** Always check hot tier first

---

### Violation 2: No Deduplication

```rust
// ❌ BAD
let mut results = hot_results;
results.extend(cold_results);
results.truncate(query.limit);
```

**Impact:** Duplicate items shown to user, confusing UI

**Fix:** Use HashSet to track seen IDs

---

### Violation 3: Guard Held Across Await

```rust
// ❌ BAD - Deadlock risk!
let guard = self.hot.traces.pin();
let cold_results = self.query_cold_tier(query).await?;  // Guard still alive!
let hot_results: Vec<_> = guard.iter().map(|(_, t)| t.clone()).collect();
```

**Impact:** Blocks hot tier access for 15-35ms (cold query duration), can cause timeouts

**Fix:** Drop guard before any `.await`

```rust
// ✅ GOOD
let hot_results = {
    let guard = self.hot.traces.pin();
    guard.iter().map(|(_, t)| t.clone()).collect::<Vec<_>>()
};  // Guard dropped here

let cold_results = self.query_cold_tier(query).await?;  // Safe!
```

---

### Violation 4: Not Respecting Limit

```rust
// ❌ BAD
async fn query_logs(&self, query: LogQuery) -> Result<Vec<LogRecord>> {
    let hot = self.query_hot_logs(query.clone());
    let cold = self.query_cold_logs(query.clone()).await?;

    let mut all = hot;
    all.extend(cold);
    // Missing: all.truncate(query.limit);

    Ok(all)  // Might return 100,000 logs when limit was 100!
}
```

**Impact:** Memory usage spike, UI freezes, poor user experience

**Fix:** Always truncate to limit

---

### Violation 5: No Early Return

```rust
// ❌ BAD - Always querying cold tier
let hot = self.query_hot(query.clone());
let cold = self.query_cold(query.clone()).await?;  // Wasteful!
merge(hot, cold)
```

**Impact:** Unnecessary cold tier query adds 15-35ms latency when hot tier is sufficient

**Fix:** Check if hot tier has enough results before querying cold

```rust
// ✅ GOOD
let hot = self.query_hot(query.clone());
if hot.len() >= query.limit && query.within_hot_window() {
    return Ok(hot.into_iter().take(query.limit).collect());
}
let cold = self.query_cold(query).await?;
merge(hot, cold)
```

---

## How to Check

### Search for Query Methods

```bash
# Find all query method implementations
rg "async fn (query_|get_)" crates/sequins-storage/src/ --type rust
```

### Verify Each Method

For each method found, check:
1. Does it query hot tier first?
2. Does it have early return logic?
3. Does it query cold tier if needed?
4. Does it deduplicate?
5. Does it sort?
6. Does it apply limit?

---

## Report Format

```markdown
# Storage Pattern Violations

## query_traces (crates/sequins-storage/src/tiered_storage.rs:123)

✅ Hot tier checked first
✅ Early return present
✅ Cold tier queried conditionally
❌ **No deduplication** - Results may contain duplicates
✅ Results sorted by timestamp
✅ Limit applied

**Fix needed:** Add HashSet-based deduplication between hot and cold results

## query_logs (crates/sequins-storage/src/tiered_storage.rs:234)

❌ **Hot tier skipped** - Directly queries cold tier
❌ **No early return** - Always queries cold tier
✅ Cold tier queried
✅ Results sorted
⚠️  **Limit not applied** - May return unlimited results

**Fix needed:**
1. Add hot tier query first
2. Add early return optimization
3. Truncate results to query.limit

## get_services (crates/sequins-storage/src/tiered_storage.rs:345)

✅ All checks pass
✅ Follows standard pattern correctly

## Summary
- Methods checked: 8
- Fully compliant: 4
- Minor issues: 2
- Critical issues: 2
```

---

## Helper Functions

Create reusable helpers to enforce pattern:

```rust
impl Storage {
    /// Standard two-tier query with deduplication
    async fn query_two_tier<T, F, G, K>(
        &self,
        query: &impl Query,
        hot_query: F,
        cold_query: G,
        get_id: K,
    ) -> Result<Vec<T>>
    where
        T: Clone,
        F: FnOnce() -> Vec<T>,
        G: Future<Output = Result<Vec<T>>>,
        K: Fn(&T) -> u64,  // ID extractor
    {
        // 1. Hot tier
        let hot_results = hot_query();

        // 2. Early return
        if hot_results.len() >= query.limit() && query.within_hot_window() {
            return Ok(hot_results.into_iter().take(query.limit()).collect());
        }

        // 3. Cold tier
        let cold_results = cold_query.await?;

        // 4. Merge & deduplicate
        let mut seen = HashMap::new();
        let mut merged = Vec::new();

        for item in hot_results {
            let id = get_id(&item);
            seen.insert(id, true);
            merged.push(item);
        }

        for item in cold_results {
            let id = get_id(&item);
            if !seen.contains_key(&id) {
                seen.insert(id, true);
                merged.push(item);
            }
        }

        // 5. Sort (caller's responsibility)
        // 6. Limit
        merged.truncate(query.limit());

        Ok(merged)
    }
}
```

Then use it:

```rust
async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>> {
    let mut results = self
        .query_two_tier(
            &query,
            || self.query_hot_traces(&query),
            self.query_cold_traces(query.clone()),
            |trace| trace.trace_id.to_u128() as u64,
        )
        .await?;

    // Sort
    results.sort_by(|a, b| b.start_time.cmp(&a.start_time));

    Ok(results)
}
```

---

## Success Criteria

Storage patterns are correct when:

- ✅ All query methods check hot tier first
- ✅ Early return optimization is present where applicable
- ✅ No Papaya guards held across `.await`
- ✅ Deduplication prevents duplicate results
- ✅ Results are sorted consistently
- ✅ Limits are always respected
- ✅ Error handling is robust (cold tier failures don't crash)
- ✅ Performance meets targets (< 1ms for hot hits, < 50ms for cold)

---

**Remember:** The two-tier architecture is a key performance optimization. Consistent implementation of the query pattern ensures reliable sub-millisecond response times for 90%+ of queries.
