# Sequins Architecture Guide

**Purpose:** Conceptual understanding of the three-trait architecture and how to think about data flow.

**When to use:**
- When designing new features that span multiple crates
- When deciding which trait a new interface belongs to
- When implementing changes that affect local and remote modes
- When considering concurrency and performance patterns
- Manually when refreshing on architecture thinking

**Invocation:** Automatically when editing traits, or manually via `sequins-architecture-guide`

---

## Core Architectural Philosophy

**Sequins separates concerns into three conceptual categories, each represented by a trait.**

This separation enables:
- **Free local mode** - Direct trait usage, zero network overhead
- **Paid remote mode** - HTTP clients implement same traits
- **Zero-cost abstractions** - Generics compile to specific types
- **Clear boundaries** - Each trait has a single, well-defined purpose

```
┌─────────────────────────────────────────────────────────────┐
│                   Three Conceptual Categories               │
│                                                             │
│  WRITE               READ                  MANAGE           │
│  (OtlpIngest)        (QueryApi)           (ManagementApi)   │
│                                                             │
│  Data coming IN      Data going OUT       System control    │
│  from apps           to UI/users          and maintenance   │
└─────────────────────────────────────────────────────────────┘
                              ▲
                              │
               ┌──────────────┴──────────────┐
               │                             │
    ┌──────────▼─────────┐       ┌──────────▼─────────────┐
    │  Storage           │       │  QueryClient           │
    │  (Local mode)      │       │  ManagementClient      │
    │                    │       │  (Remote mode)         │
    │  ✓ All three       │       │  ✓ Query + Management  │
    │  Direct DB access  │       │  ✗ OtlpIngest          │
    │  Zero latency      │       │  HTTP calls            │
    └────────────────────┘       └────────────────────────┘
```

---

## Radical Simplicity Principle

**Default to the simplest solution that could possibly work.**

Building an advanced observability product doesn't require advanced complexity. In fact, complexity is the enemy:
- **Complexity compounds** - Each complex decision multiplies future complexity
- **Complexity hides bugs** - More moving parts = more places for things to break
- **Complexity slows iteration** - Harder to understand, harder to change
- **Complexity increases maintenance burden** - Future you will curse past you

### Before Adding Complexity, Ask:

**1. Is this complexity necessary?**
- What problem does it solve?
- What's the simplest alternative?
- Can we defer this decision?

**2. What's the cost of this complexity?**
- How many lines of code?
- How many new concepts to learn?
- How many edge cases to handle?
- How much harder to test?

**3. Can we solve this more simply?**
- Remove features instead of adding abstraction
- Use existing patterns instead of inventing new ones
- Inline code instead of creating frameworks
- Hard-code reasonable defaults instead of making everything configurable

### Examples of Choosing Simplicity

#### ✅ Good: Two-Tier Storage (Not Three)

**Complex approach:** Hot → Warm → Cold tiers
- Three systems to manage
- Two flush boundaries
- Complex policies for tier movement

**Simple approach:** Hot → Cold tiers
- Two systems (Papaya + Parquet)
- One flush boundary
- Clear 5-15 minute hot window
- **Result:** 95% of queries hit hot tier, good enough!

#### ✅ Good: Enum for ObjectStore (Not Plugin System)

**Complex approach:** Plugin architecture for storage backends
- Plugin loader with dynamic libraries
- Configuration DSL for plugins
- Versioning and compatibility matrix
- Error handling for plugin failures

**Simple approach:** Enum with two variants
```rust
pub enum ObjectStoreType {
    Local(LocalFileSystem),
    S3(AmazonS3),
}
```
- **Result:** Static dispatch, zero overhead, we only support what we have config for anyway

#### ✅ Good: Three Traits (Not Microservices)

**Complex approach:** Separate microservices for ingest, query, management
- Service discovery
- Inter-service communication
- Distributed transactions
- Network partitions to handle

**Simple approach:** Three traits, same process
- Same binary, different trait implementations
- Local mode: direct calls
- Remote mode: HTTP clients
- **Result:** Zero network overhead in local mode, simple HTTP in remote mode

#### ❌ Bad: Over-Abstraction

```rust
// ❌ COMPLEX - Framework for everything
trait StorageBackend {
    type Transaction;
    type Query;
    fn begin_transaction(&self) -> Self::Transaction;
    fn execute_query(&self, query: Self::Query) -> Result<Vec<Row>>;
}

trait QueryBuilder {
    fn build(&self) -> Box<dyn Query>;
}

trait TransactionManager {
    fn commit(&mut self) -> Result<()>;
    fn rollback(&mut self) -> Result<()>;
}

// ✅ SIMPLE - Direct calls
impl Storage {
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>> {
        // Just query directly, no abstraction layers
    }
}
```

### When Complexity IS Justified

Sometimes complexity is necessary. Accept it when:

1. **Core business requirement** - The business model requires it (e.g., local vs remote modes)
2. **Proven performance benefit** - Measured 10x+ improvement with real workload
3. **Industry standard** - Using established patterns (e.g., OTLP protocol, Parquet format)
4. **Future-proof without cost** - Zero-cost abstractions (generics, not trait objects)

**But even then, use the simplest version:**
- Don't build a plugin system, use an enum
- Don't build a query language, use SQL (DataFusion)
- Don't build a wire protocol, use HTTP + JSON
- Don't build a database, use Parquet

### Red Flags (Complexity Smells)

Watch out for these signs of unnecessary complexity:

- 🚩 "We might need this later" - YAGNI (You Ain't Gonna Need It)
- 🚩 "This makes it more flexible" - Flexibility costs, is it worth it?
- 🚩 "Other projects do it this way" - Other projects have different constraints
- 🚩 "It's only a few more lines" - Lines compound exponentially
- 🚩 "Let's make it configurable" - Configuration is complexity
- 🚩 "We need a framework for..." - Frameworks are heavyweight, use patterns
- 🚩 "This is more elegant" - Elegance ≠ Simplicity

### Simplicity Checklist

Before merging any feature, ask:

- [ ] Is this the simplest solution that works?
- [ ] Did I remove features instead of adding abstraction?
- [ ] Am I reusing existing patterns?
- [ ] Can a new contributor understand this in 5 minutes?
- [ ] Will I understand this in 6 months?
- [ ] What can I delete instead of add?

### The Simplicity Mantra

**"Can we do less?"**

Not "Can we make this more powerful?" but "Can we solve this with less code, fewer concepts, simpler patterns?"

The best code is code you don't have to write.

---

## The Three Categories

### 1. OtlpIngest: Write Path (Data Coming In)

**Purpose:** Receive telemetry data from applications

**Conceptual boundary:**
- OTLP protocol data (traces, logs, metrics, profiles)
- Protobuf parsing and validation
- Data enrichment (service resolution, timestamp normalization)
- Writing to storage (hot tier insertion)

**When to add methods here:**
- New OTLP signal type (e.g., future profiling support)
- Never for reading/querying data
- Never for system management

**Implementation:**
- ✅ Storage implements (parses OTLP → writes to hot tier)
- ❌ QueryClient/ManagementClient do NOT implement
- Why? OTLP goes directly to daemon's OTLP endpoints (ports 4317/4318), not through Query API

---

### 2. QueryApi: Read Path (Data Going Out)

**Purpose:** Retrieve and search telemetry data for users/UI

**Conceptual boundary:**
- Querying traces, logs, metrics, profiles
- Full-text search and filtering
- Aggregations and time-series queries
- Metadata retrieval (services, counts, schemas)
- Read-only operations

**When to add methods here:**
- New search/filter capability
- New aggregation or analytics
- Metadata queries
- Never for write operations
- Never for system administration

**Implementation:**
- ✅ Storage implements (queries hot+cold tiers, merges results)
- ✅ QueryClient implements (makes HTTP calls to Query API)
- Both work identically from caller's perspective (zero-cost abstraction)

---

### 3. ManagementApi: Control Plane (System Management)

**Purpose:** Administrative operations and system configuration

**Conceptual boundary:**
- Retention policy management
- Storage optimization (compaction, indexing)
- System statistics and health checks
- Live configuration changes
- User access control (future)
- Backup/restore operations (future)

**When to add methods here:**
- System administration tasks
- Configuration that affects storage behavior
- Maintenance operations (cleanup, optimization)
- Never for data queries
- Never for data ingestion

**Implementation:**
- ✅ Storage implements (direct database/storage operations)
- ✅ ManagementClient implements (makes HTTP calls to Management API)
- Requires authentication (admin credentials)

---

## Decision Framework: Which Trait?

When adding a new interface, ask:

### 1. Is it receiving data from external systems?
- YES → **OtlpIngest**
- NO → Continue...

### 2. Is it reading/querying/searching data for users?
- YES → **QueryApi**
- NO → Continue...

### 3. Is it managing/configuring the system itself?
- YES → **ManagementApi**
- NO → Reconsider the feature

### Examples:

| Feature | Trait | Why |
|---------|-------|-----|
| Receive trace data from app | OtlpIngest | Data ingestion |
| Search logs by keyword | QueryApi | Data retrieval |
| Get list of services | QueryApi | Metadata query |
| Trigger retention cleanup | ManagementApi | System maintenance |
| Update hot tier duration | ManagementApi | Configuration change |
| Get storage statistics | ManagementApi | System metrics |
| Export data to file | ManagementApi | Admin operation |

---

## End-to-End Data Flow Thinking

When designing a feature, trace the data flow through all layers:

### Example: Adding "Get Trace by ID"

**1. Identify the category:**
- Retrieving data for user → **QueryApi**

**2. Define the trait method:**
```rust
// In sequins-core/src/traits/query.rs
async fn get_trace_by_id(&self, trace_id: TraceId) -> Result<Option<Trace>>;
```

**3. Implement in Storage (local mode):**
```rust
// In sequins-storage/src/storage.rs
impl QueryApi for Storage {
    async fn get_trace_by_id(&self, trace_id: TraceId) -> Result<Option<Trace>> {
        // 1. Check hot tier (fast)
        // 2. Check cold tier with index (if available)
        // 3. Return result
    }
}
```

**4. Implement in QueryClient (remote mode):**
```rust
// In sequins-client/src/query_client.rs
impl QueryApi for QueryClient {
    async fn get_trace_by_id(&self, trace_id: TraceId) -> Result<Option<Trace>> {
        let url = format!("{}/api/traces/{}", self.base_url, trace_id);
        let response = self.http_client.get(&url).send().await?;
        // Handle 404, parse JSON, return result
    }
}
```

**5. Add server endpoint:**
```rust
// In sequins-server/src/query/traces.rs
pub async fn get_trace_by_id_handler<Q: QueryApi>(
    Path(trace_id): Path<String>,
    Extension(query): Extension<Q>,
) -> Result<Json<Option<Trace>>, ServerError> {
    let trace_id = TraceId::from_hex(&trace_id)?;
    let trace = query.get_trace_by_id(trace_id).await?;
    Ok(Json(trace))
}
```

**6. Use in app (works for both modes!):**
```rust
// In sequins-app/src/ui/trace_detail.rs
pub struct TraceDetailView<Q: QueryApi> {
    query: Q,
}

impl<Q: QueryApi> TraceDetailView<Q> {
    pub fn load_trace(&mut self, trace_id: TraceId, cx: &mut ViewContext<Self>) {
        let query = self.query.clone();
        cx.spawn(|this, mut cx| async move {
            let trace = query.get_trace_by_id(trace_id).await?;
            // Update UI...
        }).detach();
    }
}
```

**Key insight:** Same app code works whether `query` is `Storage` or `QueryClient`!

---

## Zero-Cost Abstraction Pattern

**Critical rule: Use generics with trait bounds, NOT trait objects.**

### ❌ Wrong: Runtime Cost
```rust
pub struct TraceView {
    query: Arc<dyn QueryApi>,  // Dynamic dispatch, vtable lookup
}
```

### ✅ Right: Zero Cost
```rust
pub struct TraceView<Q: QueryApi> {
    query: Q,  // Concrete type at compile time, inlined calls
}
```

**Why?**
- Compiler knows exact type
- Can inline method calls
- No vtable indirection
- Zero runtime overhead

**Usage:**
```rust
// Local mode
let view = TraceView { query: storage.clone() };

// Remote mode
let view = TraceView { query: client.clone() };

// Compiler creates two optimized versions, both zero-cost!
```

---

## Performance Patterns

### Pattern 1: Avoid Unnecessary Task Spawning

**Problem:** Tokio's multi-threaded runtime dispatches tasks anywhere, causing sync overhead.

```rust
// ❌ BAD - Spawning adds overhead
async fn process_request(&self, data: Data) -> Result<Response> {
    let storage = self.storage.clone();
    tokio::spawn(async move {
        storage.insert(data).await
    }).await??  // Extra synchronization cost!
}

// ✅ GOOD - Stay on same thread
async fn process_request(&self, data: Data) -> Result<Response> {
    self.storage.insert(data).await  // Direct call, same thread
}
```

**When spawning IS appropriate:**
- Background tasks (flush, retention) that run independently
- CPU-intensive work that would block event loop
- Tasks that need to outlive the request

**Rule of thumb:** Don't spawn within a request/response path unless necessary.

---

### Pattern 2: Fiercely Avoid Cloning (Except Arc/Rc)

**Cloning is expensive.** Only clone Arc/Rc types (cheap ref-count increment).

```rust
// ❌ BAD - Cloning large struct
struct Trace {
    spans: Vec<Span>,  // Could be 1000s of spans!
}

fn process(trace: Trace) {
    let clone = trace.clone();  // Copies entire Vec!
}

// ✅ GOOD - Borrow when possible
fn process(trace: &Trace) {
    // Use reference, no copy
}

// ✅ GOOD - Wrap in Arc for sharing
struct Trace {
    spans: Arc<Vec<Span>>,  // Clone is cheap!
}

fn process(trace: Trace) {
    let clone = trace.clone();  // Just increments Arc ref count
}
```

**Rules:**
1. **Borrow first** - Use `&T` when you don't need ownership
2. **Arc for sharing** - Wrap large/complex types in Arc
3. **Only clone Arc/Rc** - Cloning these is cheap (just ref count)
4. **Avoid deep clones** - Never clone `Vec<T>`, `HashMap<K,V>`, etc. if avoidable

---

### Pattern 3: Choose the Right Synchronization Primitive

**Not everything needs a Mutex** (heavy hammer).

#### When to use Mutex
```rust
// Mutex: Exclusive access, read OR write
let counter = Arc<Mutex<i32>>;

// Both reads and writes lock exclusively
let value = counter.lock().unwrap();  // Blocks all other access
```
**Use when:** Writes are common, or data structure doesn't support concurrent reads

#### When to use RwLock
```rust
// RwLock: Multiple readers OR one writer
let config = Arc<RwLock<Config>>;

// Many readers can access simultaneously
let cfg = config.read().unwrap();  // Doesn't block other readers

// Writer has exclusive access
let mut cfg = config.write().unwrap();  // Blocks everyone
```
**Use when:** Read-heavy workloads (10:1 or higher read:write ratio)

#### When to use Channels
```rust
// Channels: Producer-consumer pattern
let (tx, rx) = mpsc::channel(100);

// Producer sends
tx.send(data).await?;

// Consumer receives
while let Some(data) = rx.recv().await {
    process(data);
}
```
**Use when:**
- Decoupling producers from consumers
- Backpressure needed (bounded channels)
- Work queue patterns

#### When to use Atomics
```rust
// Atomics: Lock-free counter/flag
let counter = Arc<AtomicU64::new(0)>;

counter.fetch_add(1, Ordering::Relaxed);  // No locks!
```
**Use when:**
- Simple values (integers, bools)
- High contention expected
- Need best possible performance

**Performance hierarchy (fastest to slowest):**
1. **Atomics** - No locks, CAS operations
2. **RwLock (read)** - Multiple readers allowed
3. **Mutex** - Exclusive access always
4. **Channels** - Includes queueing overhead

---

### Pattern 4: Lock-Free When Possible

The hot tier uses **Papaya** (lock-free HashMap) specifically to avoid locks:

```rust
// Papaya: Lock-free, epoch-based reclamation
pub struct HotTier {
    traces: papaya::HashMap<TraceId, Trace>,
}

// No locks, CAS-based operations
self.hot.traces.pin().insert(trace_id, trace);
```

**Benefits:**
- No deadlocks (no locks!)
- No thread blocking
- Unlimited concurrency
- Async-friendly (guards are Send + Sync)

**See:** `sequins-lock-free-guide` skill for detailed patterns

---

## Architecture Checklist

When adding a feature, verify:

### Trait Separation
- [ ] Identified correct trait (OtlpIngest, QueryApi, or ManagementApi)
- [ ] Method signature fits conceptual boundary
- [ ] Not mixing concerns (e.g., management method in QueryApi)

### Implementation
- [ ] Implemented in Storage (local mode)
- [ ] Implemented in appropriate Client (remote mode)
- [ ] Both implementations have same behavior from caller's perspective
- [ ] Server endpoint added (for remote mode)

### Zero-Cost Abstractions
- [ ] Using generics (`<Q: QueryApi>`) not trait objects (`dyn QueryApi`)
- [ ] App components are generic over traits
- [ ] Server types are generic over traits
- [ ] No unnecessary Arc<dyn Trait>

### Performance
- [ ] Not spawning tasks unnecessarily within request path
- [ ] Only cloning Arc/Rc types
- [ ] Using appropriate sync primitive (Atomic > RwLock > Mutex > Channel)
- [ ] Borrowing when possible instead of cloning

### Data Flow
- [ ] Traced data flow end-to-end (trait → impl → server → client → app)
- [ ] Works in both local and remote modes
- [ ] No mode-specific code in app layer

---

## Common Mistakes

### Mistake 1: Wrong Trait Category

```rust
// ❌ BAD - Management method in QueryApi
trait QueryApi {
    async fn run_retention_cleanup(&self) -> Result<()>;  // Wrong trait!
}

// ✅ GOOD - In ManagementApi
trait ManagementApi {
    async fn run_retention_cleanup(&self) -> Result<()>;
}
```

### Mistake 2: Using Trait Objects

```rust
// ❌ BAD
pub struct App {
    query: Arc<dyn QueryApi>,  // Runtime cost
}

// ✅ GOOD
pub struct App<Q: QueryApi> {
    query: Q,  // Zero cost
}
```

### Mistake 3: Spawning in Request Path

```rust
// ❌ BAD
async fn handle_query(&self, query: TraceQuery) -> Result<Vec<Trace>> {
    let storage = self.storage.clone();
    tokio::spawn(async move {
        storage.query_traces(query).await
    }).await?  // Unnecessary spawn!
}

// ✅ GOOD
async fn handle_query(&self, query: TraceQuery) -> Result<Vec<Trace>> {
    self.storage.query_traces(query).await  // Direct call
}
```

### Mistake 4: Cloning Large Structures

```rust
// ❌ BAD
let traces: Vec<Trace> = /* ... */;
let cloned = traces.clone();  // Deep copy!

// ✅ GOOD - Borrow
fn process(traces: &[Trace]) { /* ... */ }

// ✅ GOOD - Arc
let traces = Arc::new(traces);
let cloned = traces.clone();  // Just ref count
```

### Mistake 5: Wrong Sync Primitive

```rust
// ❌ BAD - Mutex for read-heavy data
let config = Arc<Mutex<Config>>;  // Reads block each other!

// ✅ GOOD - RwLock for read-heavy
let config = Arc<RwLock<Config>>;  // Multiple readers OK

// ✅ BETTER - Atomic for simple values
let counter = Arc<AtomicU64::new(0)>;  // Lock-free!
```

---

## Mental Model

Think of the architecture as **three independent pipelines**:

```
WRITE Pipeline (OtlpIngest):
App → OTLP → Storage (hot tier)
                ↓
         Background flush to cold tier

READ Pipeline (QueryApi):
UI ← Query results ← Storage (hot + cold tiers)

MANAGEMENT Pipeline (ManagementApi):
Admin → Management command → Storage (system operations)
```

**Key principles:**
1. **Each pipeline is independent** - Changes to one don't affect others
2. **Same interface, different implementations** - Local (Storage) or Remote (Client)
3. **Zero-cost abstractions** - Generics compile to specific code
4. **Performance matters** - Avoid spawning, cloning, heavy locks

---

## Success Criteria

Architecture is correct when:

- ✅ Each trait has clear, single conceptual purpose
- ✅ New features fit cleanly into one of three categories
- ✅ Same app code works for local and remote modes
- ✅ No trait objects in production code (except errors)
- ✅ No unnecessary spawning in request paths
- ✅ Only Arc/Rc cloned, everything else borrowed
- ✅ Appropriate sync primitives chosen (not defaulting to Mutex)
- ✅ Zero runtime overhead from abstractions

---

**Remember:** The three-trait architecture exists to support both local (free) and remote (paid) modes with zero compromise on performance or developer experience. Every decision should maintain this property.
