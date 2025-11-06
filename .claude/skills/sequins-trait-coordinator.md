# Sequins Trait & Type Change Coordinator

**Purpose:** Coordinate far-reaching changes to traits and types across the entire codebase.

**When to use:**
- Adding a method to a trait (must update all implementations)
- Changing a trait method signature (must update implementations + call sites)
- Modifying a type's methods (must update all usages)
- Renaming types or methods (must update all references)
- Any change that has ripple effects across multiple files

**Invocation:** `sequins-trait-coordinator` when making structural changes

---

## What This Skill Does

Traits and types in Sequins span multiple crates. A single change can require updates in:
1. **Trait definition** (`sequins-core/src/traits/`)
2. **Implementations** (`Storage`, `QueryClient`, `ManagementClient`)
3. **Server endpoints** (HTTP handlers that call trait methods)
4. **UI components** (that use the trait methods)
5. **Tests** (that verify the behavior)
6. **Documentation** (trait docs, impl docs, planning docs)

This skill ensures nothing is forgotten.

---

## Scenario 1: Adding Method to Trait

### Example: Add `get_trace_by_id()` to QueryApi

#### Step 1: Update Trait Definition

```rust
// File: crates/sequins-core/src/traits/query.rs

/// Queries traces by ID.
///
/// This is faster than querying by time range when you have a specific ID.
///
/// # Arguments
///
/// * `trace_id` - The unique trace identifier
///
/// # Returns
///
/// Returns `Some(Trace)` if found, `None` if not found.
///
/// # Examples
///
/// ```
/// let trace = client.get_trace_by_id(trace_id).await?;
/// ```
async fn get_trace_by_id(&self, trace_id: TraceId) -> Result<Option<Trace>>;
```

**Checklist:**
- [ ] Added method signature to trait
- [ ] Added comprehensive doc comment
- [ ] Included example in doc comment
- [ ] Documented errors (if any)

#### Step 2: Find All Implementations

```bash
# Find all impls of QueryApi
rg "impl.*QueryApi" --type rust
```

**Expected results:**
- `crates/sequins-storage/src/tiered_storage.rs` - `impl QueryApi for Storage`
- `crates/sequins-client/src/query_client.rs` - `impl QueryApi for QueryClient`

#### Step 3: Implement in Storage

```rust
// File: crates/sequins-storage/src/tiered_storage.rs

impl QueryApi for Storage {
    async fn get_trace_by_id(&self, trace_id: TraceId) -> Result<Option<Trace>> {
        // 1. Check hot tier first
        let hot = { self.hot.traces.pin().get(&trace_id).cloned() };
        if let Some(trace) = hot {
            return Ok(Some(trace));
        }

        // 2. Check cold tier with index (if available)
        if let Some(index) = &self.cold.index {
            if let Some(file) = index.lookup(trace_id).await? {
                return self.query_parquet_file(&file, trace_id).await;
            }
        }

        // 3. Fall back to scanning all files
        self.scan_cold_tier_for_trace(trace_id).await
    }
}
```

**Checklist:**
- [ ] Implemented method in Storage
- [ ] Follows hot→cold tier pattern
- [ ] Added doc comment (can reference trait doc)
- [ ] Error handling is correct
- [ ] Returns correct type

#### Step 4: Implement in QueryClient

```rust
// File: crates/sequins-client/src/query_client.rs

impl QueryApi for QueryClient {
    async fn get_trace_by_id(&self, trace_id: TraceId) -> Result<Option<Trace>> {
        let url = format!("{}/api/traces/{}", self.base_url, trace_id);
        let response = self.http_client.get(&url).send().await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let trace = response.json::<Trace>().await?;
        Ok(Some(trace))
    }
}
```

**Checklist:**
- [ ] Implemented method in QueryClient
- [ ] Makes HTTP call to correct endpoint
- [ ] Handles 404 → None correctly
- [ ] Error handling for other HTTP errors
- [ ] Returns correct type

#### Step 5: Add Server Endpoint

```rust
// File: crates/sequins-server/src/query/traces.rs

/// GET /api/traces/:trace_id
pub async fn get_trace_by_id<Q: QueryApi>(
    Path(trace_id): Path<String>,
    Extension(query): Extension<Q>,
) -> Result<Json<Option<Trace>>, ServerError> {
    let trace_id = TraceId::from_hex(&trace_id)
        .map_err(|e| ServerError::InvalidInput(e.to_string()))?;

    let trace = query.get_trace_by_id(trace_id).await?;

    Ok(Json(trace))
}

// Update router in mod.rs
pub fn router<Q: QueryApi + Clone + Send + Sync + 'static>() -> Router {
    Router::new()
        .route("/api/traces", get(query_traces))
        .route("/api/traces/:trace_id", get(get_trace_by_id::<Q>))
        // ...
}
```

**Checklist:**
- [ ] Added HTTP handler function
- [ ] Generic over `Q: QueryApi`
- [ ] Path parameter parsing is correct
- [ ] Error handling for invalid input
- [ ] Added route to router

#### Step 6: Update UI Components (If Needed)

Check if any UI components would benefit from the new method:

```bash
# Find components that query traces
rg "query_traces" crates/sequins-app/src/ui/ --type rust
```

**If applicable, update components to use new method:**
```rust
// File: crates/sequins-app/src/ui/trace_detail.rs

impl<Q: QueryApi> TraceDetailView<Q> {
    pub fn load_trace(&mut self, trace_id: TraceId, cx: &mut ViewContext<Self>) {
        let query = self.query.clone();

        cx.spawn(|this, mut cx| async move {
            // ✅ NEW - Use specific method instead of query
            let trace = query.get_trace_by_id(trace_id).await?;

            this.update(&mut cx, |view, cx| {
                view.trace = trace;
                cx.notify();
            })?;

            Ok(())
        }).detach();
    }
}
```

#### Step 7: Add Tests

```rust
// File: crates/sequins-storage/src/tiered_storage.rs

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_get_trace_by_id_hot_tier() {
        let storage = Storage::new_in_memory()?;
        let trace = create_test_trace();

        // Insert into hot tier
        storage.ingest_trace(trace.clone()).await?;

        // Should find in hot tier
        let found = storage.get_trace_by_id(trace.trace_id).await?;
        assert_eq!(found, Some(trace));
    }

    #[tokio::test]
    async fn test_get_trace_by_id_cold_tier() {
        let storage = Storage::new_in_memory()?;
        let trace = create_test_trace();

        // Insert and flush to cold tier
        storage.ingest_trace(trace.clone()).await?;
        storage.flush_hot_to_cold().await?;

        // Should find in cold tier
        let found = storage.get_trace_by_id(trace.trace_id).await?;
        assert_eq!(found, Some(trace));
    }

    #[tokio::test]
    async fn test_get_trace_by_id_not_found() {
        let storage = Storage::new_in_memory()?;
        let trace_id = TraceId::from_hex("00000000000000000000000000000000")?;

        let found = storage.get_trace_by_id(trace_id).await?;
        assert_eq!(found, None);
    }
}
```

**Checklist:**
- [ ] Test finds trace in hot tier
- [ ] Test finds trace in cold tier
- [ ] Test returns None when not found
- [ ] Tests are comprehensive

#### Step 8: Update Documentation

```bash
# Run docs sync skill to update planning docs
# This will check if plans/architecture.md needs updating
```

**Checklist:**
- [ ] Run `sequins-docs-sync` skill
- [ ] Update `plans/architecture.md` if QueryApi is documented there
- [ ] Update `plans/implementation-roadmap.md` if this was a planned feature

---

## Scenario 2: Changing Method Signature

### Example: Add optional filter to `query_traces()`

#### Old signature:
```rust
async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>>;
```

#### New signature:
```rust
async fn query_traces(&self, query: TraceQuery, filter: Option<TraceFilter>) -> Result<Vec<Trace>>;
```

### Step-by-Step Process

#### Step 1: Update Trait Definition

```rust
// File: crates/sequins-core/src/traits/query.rs

/// Queries traces matching the given criteria.
///
/// # Arguments
///
/// * `query` - Time range, service, and limits
/// * `filter` - Optional additional filters (tags, status, etc.)  // NEW
async fn query_traces(
    &self,
    query: TraceQuery,
    filter: Option<TraceFilter>,  // NEW
) -> Result<Vec<Trace>>;
```

#### Step 2: Update All Implementations

```bash
# Find all implementations
rg "fn query_traces" --type rust
```

For each implementation:
1. Add new parameter
2. Update implementation logic
3. Update doc comments
4. Handle None case for filter

#### Step 3: Update All Call Sites

```bash
# Find all usages
rg "\.query_traces\(" --type rust
```

For each call site:
```rust
// Old:
let traces = client.query_traces(query).await?;

// New:
let traces = client.query_traces(query, None).await?;
// Or with filter:
let traces = client.query_traces(query, Some(filter)).await?;
```

#### Step 4: Update Server Endpoints

```rust
// Update HTTP handler to accept optional filter
pub async fn query_traces<Q: QueryApi>(
    Json(request): Json<QueryTracesRequest>,  // Add filter field
    Extension(query): Extension<Q>,
) -> Result<Json<Vec<Trace>>, ServerError> {
    let traces = query
        .query_traces(request.query, request.filter)  // Pass filter
        .await?;
    Ok(Json(traces))
}
```

#### Step 5: Update Tests

For each test that calls `query_traces()`:
```rust
// Update signature
let traces = storage.query_traces(query, None).await?;
```

Add new tests for filter functionality:
```rust
#[tokio::test]
async fn test_query_traces_with_filter() {
    let storage = Storage::new_in_memory()?;
    // ... insert test data ...

    let filter = TraceFilter {
        status: Some(StatusCode::Error),
    };

    let traces = storage.query_traces(query, Some(filter)).await?;
    assert!(traces.iter().all(|t| t.status == StatusCode::Error));
}
```

#### Step 6: Update Documentation

Run `sequins-docs-sync` to update planning docs.

---

## Scenario 3: Renaming Method

### Example: Rename `get_services()` to `list_services()`

#### Step 1: Update Trait

```rust
// OLD
async fn get_services(&self) -> Result<Vec<Service>>;

// NEW
async fn list_services(&self) -> Result<Vec<Service>>;
```

#### Step 2: Update All Implementations

Use find-and-replace carefully:
```bash
# Find all implementations
rg "fn get_services" --type rust

# For each file, rename method
```

#### Step 3: Update All Call Sites

```bash
# Find all usages
rg "\.get_services\(\)" --type rust

# Replace with .list_services()
```

#### Step 4: Update Tests

```bash
rg "get_services" --glob="*_test.rs" --type rust
```

#### Step 5: Update Documentation

- Update doc comments to use new name
- Run `sequins-docs-sync`
- Update any references in CLAUDE.md

---

## Scenario 4: Adding Field to Type

### Example: Add `user_id` field to `Trace`

#### Step 1: Update Type Definition

```rust
// File: crates/sequins-core/src/models/trace.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    pub trace_id: TraceId,
    pub service: ServiceId,
    pub start_time: Timestamp,
    pub duration: Duration,
    pub span_count: usize,
    pub user_id: Option<String>,  // NEW
}
```

#### Step 2: Update Arrow Schema (If Stored)

```rust
// File: crates/sequins-core/src/models/trace.rs

impl Trace {
    pub fn arrow_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Binary, false),
            Field::new("service_id", DataType::Binary, false),
            Field::new("start_time", DataType::Int64, false),
            Field::new("duration", DataType::Int64, false),
            Field::new("span_count", DataType::UInt32, false),
            Field::new("user_id", DataType::Utf8, true),  // NEW (nullable)
        ]))
    }
}
```

#### Step 3: Update Conversions

```rust
// ToArrow conversion
impl ToArrow for Trace {
    fn to_record_batch(traces: &[Trace]) -> Result<RecordBatch> {
        // ... existing columns ...
        let user_ids: StringArray = traces
            .iter()
            .map(|t| t.user_id.as_deref())
            .collect();  // NEW

        // Add to record batch
    }
}

// FromArrow conversion
impl FromArrow for Trace {
    fn from_record_batch(batch: RecordBatch) -> Result<Vec<Trace>> {
        // ... existing columns ...
        let user_ids = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();  // NEW

        // Include in Trace construction
        user_id: user_ids.value(i).map(|s| s.to_string()),
    }
}
```

#### Step 4: Update Builders/Constructors

```rust
// Update test helpers
pub fn create_test_trace() -> Trace {
    Trace {
        trace_id: TraceId::random(),
        service: ServiceId::new(),
        start_time: Timestamp::now(),
        duration: Duration::milliseconds(100),
        span_count: 5,
        user_id: Some("test_user".to_string()),  // NEW
    }
}
```

#### Step 5: Update OTLP Parsing

```rust
// If field comes from OTLP, update parsing
fn parse_otlp_trace(otlp: OtlpTrace) -> Result<Trace> {
    // ... existing parsing ...

    // Extract user_id from resource attributes
    let user_id = otlp
        .resource
        .and_then(|r| r.attributes)
        .and_then(|attrs| {
            attrs
                .iter()
                .find(|a| a.key == "user.id")
                .map(|a| a.value.string_value.clone())
        });  // NEW

    Ok(Trace {
        // ... existing fields ...
        user_id,  // NEW
    })
}
```

#### Step 6: Update Tests

```rust
#[test]
fn test_trace_with_user_id() {
    let trace = Trace {
        // ... fields ...
        user_id: Some("user_123".to_string()),
    };

    // Test serialization
    let json = serde_json::to_string(&trace)?;
    assert!(json.contains("user_123"));

    // Test Arrow conversion
    let batch = Trace::to_record_batch(&[trace])?;
    let parsed = Trace::from_record_batch(batch)?;
    assert_eq!(parsed[0].user_id, Some("user_123".to_string()));
}
```

#### Step 7: Update Documentation

- Add field to doc comments
- Run `sequins-docs-sync` to update `plans/data-models.md`

---

## Checklist Template

Use this for any trait/type change:

### Trait Method Addition/Modification
- [ ] Update trait definition with doc comments
- [ ] Find all implementations (Storage, QueryClient, etc.)
- [ ] Implement in each impl block
- [ ] Update server endpoints (HTTP handlers)
- [ ] Update UI components (if applicable)
- [ ] Update all call sites
- [ ] Add/update tests for new behavior
- [ ] Update doc comments
- [ ] Run `sequins-docs-sync` skill
- [ ] Run `sequins-doc-comments` skill
- [ ] Run `cargo test --workspace`
- [ ] Run `cargo clippy --workspace`

### Type Field Addition/Modification
- [ ] Update struct definition
- [ ] Update Arrow schema (if stored in Parquet)
- [ ] Update ToArrow conversion
- [ ] Update FromArrow conversion
- [ ] Update builders/constructors
- [ ] Update OTLP parsing (if applicable)
- [ ] Update serialization tests
- [ ] Update doc comments
- [ ] Run `sequins-docs-sync` skill
- [ ] Run `cargo test --workspace`

### Method Renaming
- [ ] Update trait definition
- [ ] Update all implementations
- [ ] Find and replace all call sites
- [ ] Update tests
- [ ] Update doc comments and examples
- [ ] Run `sequins-docs-sync` skill
- [ ] Run `cargo test --workspace`

---

## Tools and Commands

### Find Implementations
```bash
# Find trait implementations
rg "impl.*TraitName" --type rust

# Find method definitions
rg "fn method_name" --type rust
```

### Find Usages
```bash
# Find method calls
rg "\.method_name\(" --type rust

# Find type usages
rg "TypeName" --type rust
```

### Verify Compilation
```bash
# Check specific crate
cargo check -p sequins-storage

# Check workspace
cargo check --workspace

# Run tests
cargo test --workspace
```

### Verify Documentation
```bash
# Build docs
cargo doc --no-deps --document-private-items

# Test doc examples
cargo test --doc
```

---

## Automation Hints

This skill can be partially automated:
1. Parse trait definition to extract new method signature
2. Find all `impl TraitName for` blocks
3. Generate stub implementations with TODO
4. Find all `.method()` calls and add to review list
5. Run tests to find compilation errors
6. Generate checklist of files that need updates

---

## Success Criteria

Change is complete when:

- ✅ All implementations of trait have new method
- ✅ All call sites updated to new signature
- ✅ All tests pass (`cargo test --workspace`)
- ✅ No clippy warnings (`cargo clippy --workspace`)
- ✅ Documentation is updated and accurate
- ✅ Planning docs are in sync (`sequins-docs-sync` passed)
- ✅ No compiler errors or warnings
- ✅ All TODO comments resolved

---

**Remember:** Trait changes have far-reaching effects. Systematic checking of implementations, usages, tests, and documentation ensures nothing is missed. When in doubt, compile frequently and run tests to catch issues early.
