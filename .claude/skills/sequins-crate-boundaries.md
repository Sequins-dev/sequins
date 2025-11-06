# Sequins Crate Boundary Guardian

**Purpose:** Enforce clean crate architecture and prevent dependency violations.

**When to use:**
- Before committing code changes
- After adding new dependencies to Cargo.toml
- When modifying import statements
- During code review

**Invocation:** `sequins-crate-boundaries` or automatically when editing Cargo.toml files

---

## What This Skill Does

The Sequins architecture relies on strict crate boundaries to maintain:
- Clean separation of concerns
- Zero-cost abstractions via generics
- Clear ownership of functionality
- Future-proof architecture for enterprise deployment

This skill verifies that crate dependencies follow the architectural rules.

## Architecture Rules

### Rule 1: Core is Dependency-Free ⭐⭐⭐

**`sequins-core` must NOT import any other sequins crates.**

```toml
# ❌ FORBIDDEN in sequins-core/Cargo.toml
[dependencies]
sequins-storage = { path = "../sequins-storage" }
sequins-client = { path = "../sequins-client" }
sequins-server = { path = "../sequins-server" }
sequins-app = { path = "../sequins-app" }
```

**Why:** Core defines the contract (traits and types). It must be the foundation that all other crates build upon. If core imports other crates, you create circular dependencies or tight coupling.

**What core CAN import:**
- External crates: `serde`, `chrono`, `opentelemetry`, `uuid`, etc.
- Standard library only

**Check:**
```bash
cd crates/sequins-core
grep "sequins-" Cargo.toml
# Should return NO results
```

---

### Rule 2: Storage is Self-Contained ⭐⭐⭐

**`sequins-storage` must NOT import `sequins-app`, `sequins-client`, or `sequins-server`.**

```toml
# ✅ ALLOWED in sequins-storage/Cargo.toml
[dependencies]
sequins-core = { path = "../sequins-core" }
libsql = "0.4"
arrow = "53"
parquet = "53"
datafusion = "42"
papaya = "0.1"
# ... other external crates

# ❌ FORBIDDEN
sequins-app = { path = "../sequins-app" }
sequins-client = { path = "../sequins-client" }
sequins-server = { path = "../sequins-server" }
```

**Why:** Storage owns the data layer and implements all three traits (OtlpIngest, QueryApi, ManagementApi). It should not know about:
- How data is served (server's job)
- How data is accessed remotely (client's job)
- How data is displayed (app's job)

**What storage CAN import:**
- `sequins-core` (for traits and types)
- Database/storage libraries
- OTLP/OpenTelemetry libraries (for parsing)

---

### Rule 3: Server Uses Generics, Not Concrete Types ⭐⭐⭐

**`sequins-server` must be generic over traits, NOT import concrete storage types.**

```rust
// ❌ BAD - Imports concrete Storage
use sequins_storage::Storage;

pub struct OtlpServer {
    storage: Storage,  // Coupled to specific implementation!
}

// ✅ GOOD - Generic over trait
use sequins_core::OtlpIngest;

pub struct OtlpServer<I: OtlpIngest> {
    ingest: I,  // Works with ANY OtlpIngest implementation
}
```

**Cargo.toml check:**
```toml
# ✅ ALLOWED in sequins-server/Cargo.toml
[dependencies]
sequins-core = { path = "../sequins-core" }
tonic = "0.12"
axum = "0.7"
# ... other networking/protocol crates

# ❌ FORBIDDEN (should not need these)
sequins-storage = { path = "../sequins-storage" }
sequins-client = { path = "../sequins-client" }
```

**Exception:** Server MAY import storage in examples or integration tests:
```toml
[dev-dependencies]
sequins-storage = { path = "../sequins-storage" }  # OK for tests
```

**Why:** This maintains the zero-cost abstraction pattern and allows server to work with ANY implementation of the traits.

---

### Rule 4: Client Implements Traits, Not Storage ⭐⭐⭐

**`sequins-client` must NOT import `sequins-storage` or `sequins-server`.**

```toml
# ✅ ALLOWED in sequins-client/Cargo.toml
[dependencies]
sequins-core = { path = "../sequins-core" }
reqwest = "0.12"
serde_json = "1.0"

# ❌ FORBIDDEN
sequins-storage = { path = "../sequins-storage" }
sequins-server = { path = "../sequins-server" }
```

**Why:** Client provides remote implementations of QueryApi and ManagementApi via HTTP. It should not:
- Know about local storage (that's Storage's job)
- Import server code (just uses HTTP endpoints)

**Check client implementation:**
```rust
// ✅ GOOD - Implements traits via HTTP
impl QueryApi for QueryClient {
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>> {
        let url = format!("{}/api/traces", self.base_url);
        let response = self.client.post(&url).json(&query).send().await?;
        response.json().await.map_err(Into::into)
    }
}

// ❌ BAD - Using storage directly
impl QueryApi for QueryClient {
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>> {
        self.storage.query_traces(query).await  // Wrong! Not remote!
    }
}
```

---

### Rule 5: App is Top-Level, Can Import All ✅

**`sequins-app` CAN import all other sequins crates.**

```toml
# ✅ ALLOWED in sequins-app/Cargo.toml
[dependencies]
sequins-core = { path = "../sequins-core" }
sequins-storage = { path = "../sequins-storage" }
sequins-server = { path = "../sequins-server" }
sequins-client = { path = "../sequins-client" }
gpui = "0.2"
```

**Why:** App is the composition layer. It:
- Embeds OtlpServer (via server crate)
- Uses Storage locally OR QueryClient remotely
- Renders UI with GPUI

**But app must still use generics:**
```rust
// ✅ GOOD - Generic over QueryApi trait
pub struct TraceListView<Q: QueryApi> {
    query: Q,
    traces: Vec<Trace>,
}

// ❌ BAD - Hardcoded to one implementation
pub struct TraceListView {
    storage: Storage,  // What about remote mode?
}
```

---

### Rule 6: Daemon is Top-Level, Imports Storage + Server ✅

**`sequins-daemon` CAN import storage and server crates.**

```toml
# ✅ ALLOWED in sequins-daemon/Cargo.toml
[dependencies]
sequins-core = { path = "../sequins-core" }
sequins-storage = { path = "../sequins-storage" }
sequins-server = { path = "../sequins-server" }
tokio = { version = "1", features = ["full"] }
```

**Why:** Daemon composes Storage with OtlpServer, QueryServer, and ManagementServer.

**But daemon should NOT import:**
- `sequins-app` (daemon is headless)
- `sequins-client` (daemon is the server, not the client)

---

## Verification Checklist

For each crate, verify:

### `sequins-core`
- [ ] No sequins-* dependencies in Cargo.toml
- [ ] Only defines traits and types
- [ ] No concrete implementations

### `sequins-storage`
- [ ] Only depends on sequins-core
- [ ] Does not depend on server, client, or app
- [ ] Implements all three traits (OtlpIngest, QueryApi, ManagementApi)

### `sequins-server`
- [ ] Only depends on sequins-core
- [ ] All server types are generic: `OtlpServer<I: OtlpIngest>`
- [ ] No imports of concrete storage types in src/
- [ ] May import storage in dev-dependencies for tests

### `sequins-client`
- [ ] Only depends on sequins-core
- [ ] Does not depend on storage or server
- [ ] Implements QueryApi and ManagementApi via HTTP

### `sequins-app`
- [ ] Can depend on any sequins crate
- [ ] Uses generics with trait bounds (not concrete types in components)

### `sequins-daemon`
- [ ] Depends on core, storage, and server
- [ ] Does NOT depend on app or client

---

## How to Check

### Automated Check

Run this script to verify dependencies:

```bash
#!/bin/bash

echo "🔍 Checking crate boundaries..."

# Check core has no sequins deps
core_deps=$(grep -c "sequins-" crates/sequins-core/Cargo.toml || echo "0")
if [ "$core_deps" != "0" ]; then
    echo "❌ sequins-core has sequins-* dependencies!"
    grep "sequins-" crates/sequins-core/Cargo.toml
else
    echo "✅ sequins-core is dependency-free"
fi

# Check storage doesn't import app/client/server
storage_bad=$(grep -E "sequins-(app|client|server)" crates/sequins-storage/Cargo.toml || echo "")
if [ -n "$storage_bad" ]; then
    echo "❌ sequins-storage imports forbidden crates!"
    echo "$storage_bad"
else
    echo "✅ sequins-storage boundaries correct"
fi

# Check server doesn't import storage (except dev-deps)
server_storage=$(grep "sequins-storage" crates/sequins-server/Cargo.toml | grep -v "dev-dependencies" || echo "")
if [ -n "$server_storage" ]; then
    echo "❌ sequins-server imports storage in main deps!"
    echo "$server_storage"
else
    echo "✅ sequins-server uses generics"
fi

# Check client doesn't import storage or server
client_bad=$(grep -E "sequins-(storage|server)" crates/sequins-client/Cargo.toml | grep -v "dev-dependencies" || echo "")
if [ -n "$client_bad" ]; then
    echo "❌ sequins-client imports forbidden crates!"
    echo "$client_bad"
else
    echo "✅ sequins-client boundaries correct"
fi

echo "✨ Crate boundary check complete"
```

### Manual Check

1. Review each Cargo.toml file
2. Look for `use sequins_*` statements in src/ files
3. Verify generic patterns: `impl<Q: QueryApi>` not `Storage`
4. Check dev-dependencies separately (they can be more relaxed)

---

## Common Violations & Fixes

### Violation 1: Server Importing Storage

```rust
// ❌ BAD
use sequins_storage::Storage;

pub struct OtlpServer {
    storage: Storage,
}

impl OtlpServer {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }
}
```

**Fix:**
```rust
// ✅ GOOD
use sequins_core::OtlpIngest;

pub struct OtlpServer<I: OtlpIngest> {
    ingest: I,
}

impl<I: OtlpIngest> OtlpServer<I> {
    pub fn new(ingest: I) -> Self {
        Self { ingest }
    }
}
```

### Violation 2: Client Using Storage

```rust
// ❌ BAD
use sequins_storage::Storage;

pub struct QueryClient {
    storage: Option<Storage>,  // Trying to support both?
    http_client: reqwest::Client,
}
```

**Fix:** Client should ONLY do HTTP, storage should ONLY do local:
```rust
// ✅ GOOD
pub struct QueryClient {
    base_url: String,
    http_client: reqwest::Client,
}
```

### Violation 3: App Using Concrete Types

```rust
// ❌ BAD
pub struct TraceListView {
    storage: Arc<Storage>,  // What about remote mode?
}
```

**Fix:**
```rust
// ✅ GOOD
pub struct TraceListView<Q: QueryApi> {
    query: Q,
}
```

---

## Benefits of Clean Boundaries

When boundaries are maintained:

✅ **Zero-cost abstractions** - Generics compile to specific types, no runtime overhead
✅ **Testable** - Can mock trait implementations easily
✅ **Flexible** - Support local and remote modes transparently
✅ **Maintainable** - Clear ownership, easy to reason about
✅ **Future-proof** - Can add new implementations without changing existing code

When boundaries break:

❌ **Tight coupling** - Changes cascade across crates
❌ **Circular dependencies** - Can't compile or hard to reason about
❌ **Runtime overhead** - May force use of trait objects (`dyn`)
❌ **Architectural drift** - Defeats the purpose of the three-trait design

---

## Success Criteria

Architecture is clean when:

- ✅ `cargo tree` shows no unexpected dependencies
- ✅ Core crate has zero sequins-* dependencies
- ✅ Server crate uses generics, not concrete types
- ✅ Each crate has a clear, single responsibility
- ✅ No circular dependencies exist
- ✅ Build times are reasonable (small crates = parallel builds)

---

**Remember:** The crate boundaries exist to support the business model (free local + paid enterprise) and maintain zero-cost abstractions. Breaking them undermines the entire architecture.
