# Sequins Lock-Free Concurrency Guide

**Purpose:** Correct usage of Papaya lock-free HashMap in the hot tier.

**When to use:**
- When working with hot tier code (`HotTier` struct)
- When using Papaya HashMap operations
- When debugging hangs or performance issues in hot tier
- Manually when refreshing on lock-free patterns

**Invocation:** Automatically when editing hot tier code, or manually via `sequins-lock-free-guide`

---

## Why Papaya?

Sequins uses **Papaya**, a lock-free concurrent HashMap, for the hot tier. This is a critical architectural decision.

### Why Not Traditional Locks?

**Problems with traditional mutexes:**
- ❌ Deadlock risk (lock ordering issues)
- ❌ Thread blocking (holding lock blocks all other threads)
- ❌ Not async-friendly (can't hold MutexGuard across `.await`)
- ❌ Limited concurrency (single writer at a time)

**Problems with DashMap (sharded locks):**
- ❌ Still uses locks (RwLocks per shard)
- ❌ Limited by shard count (typically 8-16 shards)
- ❌ Lock contention under high concurrency
- ❌ Guards not Send + Sync (can't cross await)

### Why Papaya?

**Papaya advantages:**
- ✅ **Lock-free** - CAS-based operations, no thread blocking ever
- ✅ **Async-safe** - Guards are `Send + Sync`, safe to hold across `.await`
- ✅ **No deadlocks** - No locks = no deadlock possibility
- ✅ **Unlimited concurrency** - Not limited by shard count
- ✅ **Better tail latency** - No worst-case lock contention spikes
- ✅ **Epoch-based reclamation** - Efficient memory management

**Tradeoffs:**
- ⚠️ Clone-on-read semantics (must clone values)
- ⚠️ Higher memory usage (epoch-based reclamation overhead)
- ⚠️ Learning curve (different mental model than locks)

---

## Core Concepts

### Pinning and Epochs

Papaya uses **epoch-based memory reclamation**:

```
┌─────────────────────────────────────────────────────┐
│                  Epoch Timeline                     │
├─────────────────────────────────────────────────────┤
│                                                     │
│  Epoch 1        Epoch 2        Epoch 3              │
│  ┌──────┐      ┌──────┐       ┌──────┐             │
│  │ T1   │──────│ T1   │───────│ T1   │             │
│  └──────┘      └──────┘       └──────┘             │
│                                                     │
│  ┌──────┐                                           │
│  │ T2   │  (done)                                   │
│  └──────┘                                           │
│     ▲                                               │
│     └─── Memory freed when all threads              │
│          have moved past this epoch                 │
└─────────────────────────────────────────────────────┘
```

**Key idea:**
- Each `.pin()` call enters current epoch
- Guard keeps thread in that epoch
- Memory is only freed when ALL threads have moved past an epoch
- Dropping guard advances thread to next epoch

---

## Guard Patterns

### Pattern 1: Read and Clone

```rust
// ✅ GOOD - Pin, get, clone, drop
let trace = {
    let guard = self.hot.traces.pin();
    guard.get(&trace_id).cloned()
};
// Guard dropped here, epoch can advance

if let Some(trace) = trace {
    // Use cloned trace safely
    process_trace(trace);
}
```

**Why clone?**
- Can't return reference (lifetime tied to guard)
- Clone is typically cheap (Arc-wrapped fields)
- Allows guard to drop immediately

### Pattern 2: Insert

```rust
// ✅ GOOD - Simple insert
self.hot.traces.pin().insert(trace_id, trace);

// ✅ ALSO GOOD - Check before insert
let guard = self.hot.traces.pin();
if !guard.contains_key(&trace_id) {
    guard.insert(trace_id, trace);
}
// Guard auto-dropped
```

**Note:** Insert doesn't need to clone, value is moved into map.

### Pattern 3: Iterate and Collect

```rust
// ✅ GOOD - Collect to Vec, drop guard
let matching: Vec<Trace> = {
    let guard = self.hot.traces.pin();
    guard
        .iter()
        .filter(|(_, trace)| matches_query(trace))
        .map(|(_, trace)| trace.clone())
        .collect()
};
// Guard dropped, epoch advances

// Process collected results
for trace in matching {
    send_to_ui(trace);
}
```

**Key points:**
- Iterator holds guard internally
- `.collect()` forces iteration to complete
- Guard drops after collection
- Results are owned, safe to use after guard drops

### Pattern 4: Remove

```rust
// ✅ GOOD - Remove returns owned value
let trace = self.hot.traces.pin().remove(&trace_id);

// ✅ ALSO GOOD - Batch remove
let guard = self.hot.traces.pin();
for id in ids_to_remove {
    guard.remove(&id);
}
// Guard auto-dropped
```

---

## Anti-Patterns (Don't Do This!)

### Anti-Pattern 1: Holding Guard Across Await ⚠️⚠️⚠️

```rust
// ❌ VERY BAD - Guard held across await!
let guard = self.hot.traces.pin();
let trace = guard.get(&trace_id).cloned();
self.flush_to_cold().await?;  // Guard still alive!
drop(guard);  // Finally dropped

// Why bad?
// - Blocks epoch advancement for 15-35ms (flush duration)
// - Prevents memory reclamation
// - Other threads wait for this thread's epoch to advance
// - Causes cascading delays
```

**Fix:**
```rust
// ✅ GOOD - Drop guard before await
let trace = {
    let guard = self.hot.traces.pin();
    guard.get(&trace_id).cloned()
};  // Guard dropped

self.flush_to_cold().await?;  // Safe!
```

### Anti-Pattern 2: Long-Lived Guards

```rust
// ❌ BAD - Guard lives too long
let guard = self.hot.traces.pin();
let trace1 = guard.get(&id1).cloned();
do_expensive_sync_computation();  // 100ms!
let trace2 = guard.get(&id2).cloned();
drop(guard);

// Why bad?
// - Blocks epoch advancement for 100ms
// - Prevents memory reclamation
// - Unnecessary - pin() is cheap (~10-20ns)
```

**Fix:**
```rust
// ✅ GOOD - Separate pin() calls
let trace1 = { self.hot.traces.pin().get(&id1).cloned() };
do_expensive_sync_computation();
let trace2 = { self.hot.traces.pin().get(&id2).cloned() };
```

### Anti-Pattern 3: Returning References

```rust
// ❌ DOESN'T COMPILE - Can't return reference
fn get_trace(&self, id: TraceId) -> Option<&Trace> {
    let guard = self.hot.traces.pin();
    guard.get(&id)  // Lifetime error! Reference tied to guard
}

// Why bad?
// - Guard drops at end of function
// - Reference becomes invalid
// - Rust prevents this at compile time (good!)
```

**Fix:**
```rust
// ✅ GOOD - Return owned value
fn get_trace(&self, id: TraceId) -> Option<Trace> {
    let guard = self.hot.traces.pin();
    guard.get(&id).cloned()
}
```

### Anti-Pattern 4: Nested Guards

```rust
// ❌ BAD - Multiple guards alive simultaneously
let guard1 = self.hot.traces.pin();
let guard2 = self.hot.logs.pin();
let trace = guard1.get(&trace_id).cloned();
let log = guard2.get(&log_id).cloned();
// Both guards still alive!

// Why bad?
// - Unnecessarily pins two epochs
// - More memory pressure
// - No benefit over sequential pins
```

**Fix:**
```rust
// ✅ GOOD - Sequential pins
let trace = { self.hot.traces.pin().get(&trace_id).cloned() };
let log = { self.hot.logs.pin().get(&log_id).cloned() };
```

---

## Common Operations

### Operation 1: Check if Exists

```rust
// ✅ GOOD
let exists = self.hot.traces.pin().contains_key(&trace_id);

// ✅ ALSO GOOD - If you need value too
let trace = self.hot.traces.pin().get(&trace_id).cloned();
let exists = trace.is_some();
```

### Operation 2: Update Existing

```rust
// ✅ GOOD - Remove, modify, insert
let mut trace = self.hot.traces.pin().remove(&trace_id);
if let Some(ref mut trace) = trace {
    trace.span_count += 1;
    self.hot.traces.pin().insert(trace.trace_id, trace.clone());
}

// ⚠️ ALTERNATIVE - Compute and swap (if supported)
// Check Papaya docs for compare_exchange operations
```

### Operation 3: Batch Operations

```rust
// ✅ GOOD - Reuse guard for batch
let guard = self.hot.traces.pin();
for trace in traces_to_insert {
    guard.insert(trace.trace_id, trace);
}
// Guard auto-dropped after scope
```

### Operation 4: Count Items

```rust
// ✅ GOOD - Use len()
let count = self.hot.traces.pin().len();

// ❌ DON'T - Iterate just to count
let guard = self.hot.traces.pin();
let count = guard.iter().count();  // Slower!
```

---

## Async Patterns

### Pattern: Query Hot Tier in Async Function

```rust
async fn query_hot_traces(&self, query: &TraceQuery) -> Vec<Trace> {
    // ✅ GOOD - Pin, collect, drop guard, then await if needed
    let results: Vec<Trace> = {
        let guard = self.hot.traces.pin();
        guard
            .iter()
            .filter(|(_, t)| query.matches(t))
            .map(|(_, t)| t.clone())
            .take(query.limit)
            .collect()
    };  // Guard dropped before any await points

    // Now safe to do async operations with results
    results
}
```

### Pattern: Spawn Task with Cloned Data

```rust
async fn process_traces_async(&self) {
    // Collect data from hot tier
    let traces: Vec<Trace> = {
        let guard = self.hot.traces.pin();
        guard.iter().map(|(_, t)| t.clone()).collect()
    };  // Guard dropped

    // Spawn tasks that process cloned data
    for trace in traces {
        tokio::spawn(async move {
            expensive_async_processing(trace).await;
        });
    }
}
```

---

## Performance Characteristics

### Pin Cost: ~10-20 nanoseconds
```rust
// Very cheap - don't avoid pinning
let t1 = { self.hot.traces.pin().get(&id1).cloned() };
let t2 = { self.hot.traces.pin().get(&id2).cloned() };
// Two pins = 20-40ns total (negligible!)
```

### Clone Cost: Depends on Type
```rust
// Cheap clones (Arc-wrapped):
#[derive(Clone)]
struct Trace {
    trace_id: TraceId,  // Copy
    spans: Arc<Vec<Span>>,  // Cheap clone (just Arc::clone)
    service: Arc<String>,  // Cheap clone
}

// Expensive clones (avoid if possible):
struct Trace {
    spans: Vec<Span>,  // Deep clone! Could be 1000s of spans
}
```

**Optimization:** Wrap large collections in Arc:
```rust
// ✅ GOOD
struct Trace {
    spans: Arc<Vec<Span>>,  // Clone is cheap
}

// ❌ BAD (for hot tier)
struct Trace {
    spans: Vec<Span>,  // Clone is expensive
}
```

### Memory Overhead: Epoch-Based Reclamation

Papaya holds onto deleted values until all threads have advanced past their epoch.

**Worst case:**
- One slow thread pins old epoch
- Deleted values pile up
- Memory usage grows

**Mitigation:**
- Keep guards short-lived
- Don't hold guards across slow operations
- Monitor memory usage

---

## Debugging Papaya Issues

### Issue: Application Hangs

**Symptom:** App becomes unresponsive, CPU usage low

**Possible causes:**
1. Guard held across blocking operation
2. Guard held across long await
3. Infinite loop with guard inside

**Debug steps:**
```bash
# Attach lldb to hung process
lldb -p <pid>

# Get all backtraces
(lldb) thread backtrace all

# Look for:
# - papaya::HashMap::pin in many threads
# - Threads waiting on epoch advancement
# - Guard held during slow operation
```

**Fix:** Find guard, drop it before slow operation.

### Issue: Memory Growth

**Symptom:** Hot tier memory usage grows continuously

**Possible causes:**
1. Not removing flushed items from hot tier
2. Long-lived guard preventing reclamation
3. Memory leak (unrelated to Papaya)

**Debug steps:**
```rust
// Add instrumentation
tracing::info!(
    "Hot tier: {} traces, {} bytes",
    self.hot.traces.pin().len(),
    estimate_memory_usage()
);
```

**Fix:**
- Ensure flush removes items: `guard.remove(&id)`
- Check for long-lived guards
- Use memory profiler to identify leak

### Issue: Slower Than Expected

**Symptom:** Hot tier queries taking > 1ms

**Possible causes:**
1. Too many items in hot tier (should be 1K-100K max)
2. Expensive clones (large vectors not Arc-wrapped)
3. Iterating entire map when specific lookup would work

**Debug steps:**
```rust
use std::time::Instant;

let start = Instant::now();
let results = { self.hot.traces.pin().iter().collect() };
tracing::info!("Hot tier query took {:?}", start.elapsed());
```

**Fix:**
- Adjust hot tier duration (flush more frequently)
- Wrap large collections in Arc
- Use `get()` instead of `iter()` when possible

---

## Testing Papaya Code

### Test 1: Concurrent Inserts

```rust
#[tokio::test]
async fn test_concurrent_inserts() {
    let hot = Arc::new(HotTier::new());

    // Spawn 100 tasks that insert concurrently
    let tasks: Vec<_> = (0..100)
        .map(|i| {
            let hot = hot.clone();
            tokio::spawn(async move {
                let trace = create_test_trace(i);
                hot.traces.pin().insert(trace.trace_id, trace);
            })
        })
        .collect();

    // Wait for all tasks
    for task in tasks {
        task.await.unwrap();
    }

    // Verify all inserted
    assert_eq!(hot.traces.pin().len(), 100);
}
```

### Test 2: Guard Doesn't Block Async

```rust
#[tokio::test]
async fn test_guard_across_await() {
    let hot = HotTier::new();
    let trace = create_test_trace();
    hot.traces.pin().insert(trace.trace_id, trace.clone());

    // This should compile and work (guard is Send + Sync)
    let retrieved = {
        let guard = hot.traces.pin();
        let t = guard.get(&trace.trace_id).cloned();
        // Could even await here (though not recommended)
        tokio::time::sleep(Duration::from_millis(1)).await;
        t
    };

    assert_eq!(retrieved, Some(trace));
}
```

### Test 3: Clone is Cheap

```rust
#[test]
fn test_trace_clone_is_cheap() {
    let trace = create_trace_with_100_spans();

    let start = Instant::now();
    for _ in 0..1000 {
        let _cloned = trace.clone();
    }
    let elapsed = start.elapsed();

    // Should be < 1ms for 1000 clones
    assert!(elapsed < Duration::from_millis(1));
}
```

---

## Migration from Locks

If migrating from `Mutex` or `RwLock`:

### Before (Mutex)
```rust
struct HotTier {
    traces: Arc<Mutex<HashMap<TraceId, Trace>>>,
}

fn get_trace(&self, id: TraceId) -> Option<Trace> {
    let guard = self.traces.lock().unwrap();
    guard.get(&id).cloned()
}  // Mutex guard dropped
```

### After (Papaya)
```rust
struct HotTier {
    traces: papaya::HashMap<TraceId, Trace>,
}

fn get_trace(&self, id: TraceId) -> Option<Trace> {
    let guard = self.traces.pin();
    guard.get(&id).cloned()
}  // Papaya guard dropped
```

**Key differences:**
- No `Arc<Mutex<...>>` wrapper needed
- `.lock()` → `.pin()`
- `.unwrap()` → (no unwrap needed)
- Otherwise very similar!

---

## Success Criteria

Papaya usage is correct when:

- ✅ No guards held across `.await` points
- ✅ Guard scopes are minimal (< 1ms lifetime)
- ✅ Values are cloned out of guards
- ✅ Large collections are Arc-wrapped (cheap clones)
- ✅ Flushed items are removed from hot tier
- ✅ No memory leaks from epoch buildup
- ✅ Hot tier queries are < 1ms
- ✅ No hangs or deadlocks (impossible with Papaya!)

---

**Remember:** Papaya provides lock-free concurrency, but requires discipline around guard lifetimes. The key rule: **Drop guards as quickly as possible**, especially before any `.await` or expensive computation. When in doubt, scope the guard with `{ ... }`.
