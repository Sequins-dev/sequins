# Sequins Zero-Cost Abstraction Checker

**Purpose:** Ensure generics with trait bounds are used instead of trait objects for performance-critical code.

**When to use:**
- After implementing new features
- Before committing performance-sensitive code
- During code review
- When refactoring interfaces

**Invocation:** `sequins-zero-cost-abstractions` or automatically after feature implementation

---

## What This Skill Does

Sequins architecture relies on **zero-cost abstractions** to support both local and remote modes without runtime overhead. This means using **static dispatch via generics** instead of **dynamic dispatch via trait objects**.

This skill scans code for patterns that introduce runtime cost and suggests zero-cost alternatives.

## Static Dispatch vs Dynamic Dispatch

### ❌ Dynamic Dispatch (Runtime Cost)

**Trait Objects:** `dyn Trait`, `Box<dyn Trait>`, `Arc<dyn Trait>`

```rust
// ❌ BAD - Dynamic dispatch, vtable lookup at runtime
fn query_traces(client: &dyn QueryApi, query: TraceQuery) -> Result<Vec<Trace>> {
    client.query_traces(query).await  // Vtable lookup!
}

// ❌ BAD - Heap allocation + dynamic dispatch
fn create_client() -> Box<dyn QueryApi> {
    Box::new(QueryClient::new(url))
}

// ❌ BAD - Reference counting + dynamic dispatch
struct App {
    client: Arc<dyn QueryApi>,  // Runtime overhead!
}
```

**Costs:**
- Vtable pointer stored in fat pointer (2x pointer size)
- Indirect function call through vtable (not inlineable)
- No monomorphization (can't optimize across call boundary)
- Heap allocation required for `Box<dyn>`

### ✅ Static Dispatch (Zero Cost)

**Generics with Trait Bounds:** `impl Trait`, `T: Trait`

```rust
// ✅ GOOD - Static dispatch, monomorphization
async fn query_traces<Q: QueryApi>(client: &Q, query: TraceQuery) -> Result<Vec<Trace>> {
    client.query_traces(query).await  // Direct call, inlined!
}

// ✅ GOOD - Return impl Trait
fn create_client() -> impl QueryApi {
    QueryClient::new(url)
}

// ✅ GOOD - Generic struct
struct App<Q: QueryApi> {
    client: Q,  // Concrete type at compile time!
}
```

**Benefits:**
- No vtable, no indirection
- Compiler knows exact type, can inline
- Monomorphization creates optimized code for each concrete type
- No heap allocation required
- Zero runtime overhead

---

## Patterns to Find and Fix

### Pattern 1: Trait Object Parameters

```rust
// ❌ BAD
pub async fn fetch_logs(storage: &dyn QueryApi) -> Result<Vec<LogRecord>> {
    storage.query_logs(LogQuery::default()).await
}

// ✅ GOOD
pub async fn fetch_logs<Q: QueryApi>(storage: &Q) -> Result<Vec<LogRecord>> {
    storage.query_logs(LogQuery::default()).await
}
```

### Pattern 2: Boxed Trait Objects

```rust
// ❌ BAD
pub struct Component {
    query: Box<dyn QueryApi>,
}

// ✅ GOOD
pub struct Component<Q: QueryApi> {
    query: Q,
}
```

### Pattern 3: Arc'd Trait Objects

```rust
// ❌ BAD
pub struct SharedState {
    storage: Arc<dyn QueryApi>,
}

// ✅ GOOD (if you need Arc for sharing)
pub struct SharedState<Q> {
    storage: Arc<Q>,  // Arc<T>, not Arc<dyn Trait>
}

// ✅ EVEN BETTER (if you can clone)
pub struct SharedState<Q: QueryApi + Clone> {
    storage: Q,  // Clone is cheap for Arc-wrapped clients
}
```

### Pattern 4: Returning Trait Objects

```rust
// ❌ BAD
pub fn create_storage() -> Box<dyn QueryApi> {
    Box::new(Storage::new())
}

// ✅ GOOD
pub fn create_storage() -> impl QueryApi {
    Storage::new()
}

// ✅ ALSO GOOD (explicit type)
pub fn create_storage() -> Storage {
    Storage::new()
}
```

### Pattern 5: Storing Trait Objects in Structs

```rust
// ❌ BAD
pub struct TraceView {
    client: Arc<dyn QueryApi>,
    traces: Vec<Trace>,
}

// ✅ GOOD
pub struct TraceView<Q: QueryApi> {
    client: Q,
    traces: Vec<Trace>,
}

// Usage
let view = TraceView {
    client: storage.clone(),  // Works with Storage or QueryClient!
    traces: vec![],
};
```

---

## When Dynamic Dispatch is OK

There are cases where `dyn Trait` is acceptable:

### 1. **Plugin Systems** (Not applicable to Sequins yet)
```rust
// OK - Truly dynamic loading of unknown implementations
pub struct PluginManager {
    plugins: Vec<Box<dyn Plugin>>,
}
```

### 2. **Heterogeneous Collections** (Rarely needed)
```rust
// OK - Need to store different types implementing same trait
pub struct MultiClient {
    clients: Vec<Box<dyn QueryApi>>,  // Storage AND QueryClient
}
```

**But consider:** Do you really need a heterogeneous collection? Usually you know the type at compile time.

### 3. **Error Types** (Already doing this correctly)
```rust
// OK - Errors are inherently dynamic
#[error("Storage error: {0}")]
StorageError(#[from] Box<dyn std::error::Error + Send + Sync>),
```

### 4. **FFI Boundaries** (Not applicable)

**Rule of thumb:** If you can use generics, use generics. Only use trait objects when you truly need runtime polymorphism.

---

## Verification Checklist

Scan for these patterns and verify they're necessary:

### In Function Signatures
- [ ] No `&dyn Trait` parameters (use `impl Trait` or `<T: Trait>`)
- [ ] No `Box<dyn Trait>` return types (use `impl Trait`)
- [ ] No `Arc<dyn Trait>` parameters (use `Arc<T>` with generic)

### In Struct Definitions
- [ ] No `Box<dyn Trait>` fields (make struct generic)
- [ ] No `Arc<dyn Trait>` fields (use `Arc<T>` or make struct generic)
- [ ] Component structs are generic: `struct Component<Q: QueryApi>`

### In Hot Paths (Critical!)
- [ ] OTLP ingestion handlers use concrete types or generics
- [ ] Query execution uses static dispatch
- [ ] Hot tier lookups don't use trait objects
- [ ] Rendering loops in GPUI don't use trait objects

### Acceptable Uses
- [ ] Error types with `Box<dyn Error>` are OK
- [ ] True plugin systems (if added later)
- [ ] Document WHY if using trait object

---

## How to Check

### Search for Trait Object Patterns

```bash
# Find dyn Trait usage
rg "dyn\s+(OtlpIngest|QueryApi|ManagementApi)" --type rust

# Find Box<dyn
rg "Box<dyn" --type rust

# Find Arc<dyn
rg "Arc<dyn" --type rust

# Find &dyn in function params
rg "fn\s+\w+.*&dyn" --type rust
```

### Use Clippy Lint

Enable this lint to catch trait objects:

```rust
// In each lib.rs
#![warn(clippy::borrowed_box)]  // Warns about &Box<T>
```

Consider adding to workspace Cargo.toml:
```toml
[workspace.lints.clippy]
borrowed_box = "warn"
```

---

## Report Format

```markdown
# Zero-Cost Abstraction Violations

## Critical (Hot Paths)

### `crates/sequins-storage/src/tiered_storage.rs:45`
```rust
// ❌ Current
pub async fn query(&self, client: &dyn QueryApi) -> Result<Vec<Trace>>

// ✅ Should be
pub async fn query<Q: QueryApi>(&self, client: &Q) -> Result<Vec<Trace>>
```
**Impact:** Called in query hot path, indirect dispatch on every call
**Fix:** Convert to generic with trait bound

## Important (Public APIs)

### `crates/sequins-app/src/ui/traces.rs:123`
```rust
// ❌ Current
pub struct TraceListView {
    query: Arc<dyn QueryApi>,
}

// ✅ Should be
pub struct TraceListView<Q: QueryApi> {
    query: Q,
}
```
**Impact:** Prevents compiler optimizations, adds vtable overhead
**Fix:** Make struct generic over QueryApi

## Minor (Non-Critical Paths)

### `crates/sequins-server/src/config.rs:67`
```rust
// ❌ Current (error type wrapper)
pub enum ConfigError {
    Parse(Box<dyn Error>),
}
```
**Impact:** Minimal, errors are cold path
**Decision:** OK to keep, errors are naturally dynamic

## Summary
- Critical issues: 2 (fix immediately)
- Important issues: 5 (fix soon)
- Minor issues: 3 (evaluate case-by-case)
- False positives: 1 (documented as intentional)
```

---

## Converting to Generics

### Step-by-Step Conversion

#### Before (Trait Object)
```rust
pub struct TraceView {
    client: Arc<dyn QueryApi>,
}

impl TraceView {
    pub fn new(client: Arc<dyn QueryApi>) -> Self {
        Self { client }
    }

    pub async fn refresh(&self) -> Result<()> {
        let traces = self.client.query_traces(TraceQuery::default()).await?;
        // ...
        Ok(())
    }
}
```

#### After (Generic)
```rust
pub struct TraceView<Q: QueryApi> {
    client: Q,
}

impl<Q: QueryApi> TraceView<Q> {
    pub fn new(client: Q) -> Self {
        Self { client }
    }

    pub async fn refresh(&self) -> Result<()> {
        let traces = self.client.query_traces(TraceQuery::default()).await?;
        // ...
        Ok(())
    }
}
```

#### Usage (No change needed!)
```rust
// Works with Storage
let view = TraceView::new(storage.clone());

// Works with QueryClient
let view = TraceView::new(client.clone());

// Compiler creates two monomorphized versions, both zero-cost!
```

---

## Advanced: When You Need Both

Sometimes you need to store different implementations:

### Option 1: Enum (Better)
```rust
pub enum QuerySource {
    Local(Storage),
    Remote(QueryClient),
}

impl QueryApi for QuerySource {
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>> {
        match self {
            Self::Local(storage) => storage.query_traces(query).await,
            Self::Remote(client) => client.query_traces(query).await,
        }
    }
}
```

**Benefits:** Still zero-cost! Enum dispatch is a simple match, compiler optimizes it.

### Option 2: Type Parameter (Even Better)
```rust
// Just accept that App is generic over Q
pub struct App<Q: QueryApi> {
    query: Q,
}

// Instantiate with concrete type at startup
fn main() {
    if remote_mode {
        let app = App::new(QueryClient::new(url));
        run(app);
    } else {
        let app = App::new(Storage::new());
        run(app);
    }
}
```

**Benefits:** Maximum performance, compiler knows exact type in each binary.

---

## Measuring Impact

### Benchmark Before and After

```rust
use criterion::{black_box, criterion_group, criterion_main, Criterion};

// Benchmark dynamic dispatch
async fn bench_dyn(c: &mut Criterion) {
    let client: Arc<dyn QueryApi> = Arc::new(QueryClient::new(url));
    c.bench_function("query_dyn", |b| {
        b.iter(|| {
            client.query_traces(black_box(query.clone()))
        });
    });
}

// Benchmark static dispatch
async fn bench_static(c: &mut Criterion) {
    let client = QueryClient::new(url);
    c.bench_function("query_static", |b| {
        b.iter(|| {
            client.query_traces(black_box(query.clone()))
        });
    });
}
```

Expected improvement: 5-15% for simple calls, up to 50% for hot paths with inlining.

---

## Success Criteria

Code is zero-cost when:

- ✅ No `dyn Trait` in hot paths (query execution, OTLP ingestion)
- ✅ Structs are generic over traits: `Component<Q: QueryApi>`
- ✅ Functions use `impl Trait` or `<T: Trait>` instead of `&dyn Trait`
- ✅ All trait objects are documented with reasoning
- ✅ Benchmarks show no performance regression vs. concrete types

---

**Remember:** The entire Sequins architecture depends on zero-cost abstractions to support local and remote modes without compromising performance. Every `dyn Trait` is a missed optimization opportunity.
