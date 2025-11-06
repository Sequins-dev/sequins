# Sequins Doc Comment Enforcer

**Purpose:** Ensure all public APIs have proper Rust documentation comments.

**When to use:**
- Before committing code changes
- After adding new public APIs
- During code review
- When running pre-commit checks

**Invocation:** `sequins-doc-comments` or as part of pre-commit hook

---

## What This Skill Does

Scans the codebase for public items (modules, structs, enums, traits, functions, constants) and verifies they have proper `///` documentation comments following Rust best practices.

## Documentation Standards

### Required Elements

Every public item must have:
1. **Summary line** - One-line description of what it is/does
2. **Detailed description** - Additional context (if needed)
3. **Examples** - For complex APIs (especially traits and public functions)
4. **Type-specific sections** - See below

### Trait Documentation Template

```rust
/// Provides read-only access to telemetry data stored in Sequins.
///
/// This trait abstracts the storage layer, allowing the UI and query API
/// to work with either local storage (Storage) or remote storage
/// (QueryClient) without code changes.
///
/// # Implementation Notes
///
/// - All query methods are async and return `Result<T>`
/// - Queries check hot tier first, then fall back to cold tier
/// - Results are automatically deduplicated and limited
///
/// # Examples
///
/// ```
/// use sequins_core::{QueryApi, TraceQuery};
///
/// async fn find_recent_traces<Q: QueryApi>(client: &Q) -> Result<Vec<Trace>> {
///     let query = TraceQuery {
///         service: Some("api-gateway".to_string()),
///         start_time: Timestamp::now() - Duration::minutes(5),
///         end_time: Timestamp::now(),
///         limit: 100,
///     };
///     client.query_traces(query).await
/// }
/// ```
pub trait QueryApi: Send + Sync {
    // ...
}
```

### Function Documentation Template

```rust
/// Queries traces matching the given criteria.
///
/// This method searches both the hot tier (Papaya HashMap) and cold tier
/// (Parquet files via DataFusion), merging results and removing duplicates.
///
/// # Arguments
///
/// * `query` - Query parameters including service name, time range, and limit
///
/// # Returns
///
/// Returns a `Vec<Trace>` sorted by start time (newest first), limited to
/// `query.limit` results.
///
/// # Errors
///
/// Returns error if:
/// - Database query fails
/// - Time range is invalid (start > end)
/// - Limit exceeds maximum allowed (10,000)
///
/// # Examples
///
/// ```
/// let query = TraceQuery {
///     service: Some("api-gateway".to_string()),
///     start_time: Timestamp::now() - Duration::hours(1),
///     end_time: Timestamp::now(),
///     limit: 50,
/// };
/// let traces = storage.query_traces(query).await?;
/// ```
async fn query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>>;
```

### Struct Documentation Template

```rust
/// Represents a distributed trace consisting of multiple spans.
///
/// A trace captures the entire lifecycle of a request as it flows through
/// multiple services in a distributed system. Each trace has a unique ID
/// and contains one or more spans representing individual operations.
///
/// # Fields
///
/// - `trace_id`: Unique identifier (from OpenTelemetry spec)
/// - `service`: Primary service that initiated the trace
/// - `start_time`: When the first span started
/// - `duration`: Total time from first span start to last span end
/// - `span_count`: Number of spans in this trace
///
/// # Examples
///
/// ```
/// use sequins_core::Trace;
///
/// let trace = Trace {
///     trace_id: TraceId::from_hex("1234...")?,
///     service: ServiceId::new(),
///     start_time: Timestamp::now(),
///     duration: Duration::milliseconds(150),
///     span_count: 5,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trace {
    // ...
}
```

### Error Type Documentation Template

```rust
/// Errors that can occur during storage operations.
///
/// This enum covers errors from database access, data parsing,
/// validation failures, and resource limits.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Database connection or query failed
    #[error("Database error: {0}")]
    Database(#[from] libsql::Error),

    /// Invalid query parameters provided
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    /// Requested resource not found
    #[error("Not found: {0}")]
    NotFound(String),
}
```

## Verification Checklist

For each crate, check:

### `sequins-core`
- [ ] All trait definitions have doc comments with examples
- [ ] All trait methods have doc comments (can be brief)
- [ ] All public structs have doc comments explaining their purpose
- [ ] All public enums have doc comments, variants documented if non-obvious
- [ ] Error types have doc comments for each variant
- [ ] Public type aliases are documented

### `sequins-storage`
- [ ] `Storage` struct is documented
- [ ] All trait implementations have example in trait (not repeated here)
- [ ] Public helper functions are documented
- [ ] Configuration structs have field-level docs
- [ ] Internal modules have module-level docs (`//!`)

### `sequins-server`
- [ ] `OtlpServer` and `QueryServer` are documented
- [ ] Builder pattern methods are documented
- [ ] Public configuration types are documented
- [ ] Server lifecycle (start/stop) is explained

### `sequins-client`
- [ ] `QueryClient` and `ManagementClient` are documented
- [ ] Connection/configuration options are explained
- [ ] Error handling is documented

### `sequins-app`
- [ ] Public UI components are documented (even though private to crate)
- [ ] Main entry point has helpful module-level docs
- [ ] Configuration structures are documented

## What Doesn't Need Docs

You can skip doc comments for:
- Private items (unless complex)
- Test functions (the test name should be descriptive)
- Obvious getter/setter methods
- Trait implementations that just delegate to inner types
- Internal helper macros

## How to Check

### Manual Review
Look for items that start with `pub` without `///` above them:

```bash
# Find public items without doc comments (rough check)
rg "^pub (fn|struct|enum|trait|type|const|mod)" --type rust
```

### Use Clippy
Enable the missing_docs lint:

```rust
// In lib.rs
#![warn(missing_docs)]
```

Then run:
```bash
cargo clippy -- -W missing-docs
```

### Generate Docs
Try to build documentation:

```bash
cargo doc --no-deps --document-private-items
```

This will warn about missing documentation.

## Report Format

Report missing documentation in this format:

```markdown
# Missing Documentation Report

## sequins-core

### Critical (public traits/APIs)
- `crates/sequins-core/src/traits/query.rs:45` - `pub trait QueryApi` - Missing trait-level doc comment
- `crates/sequins-core/src/traits/query.rs:67` - `fn get_services()` - Missing method doc comment

### Important (public types)
- `crates/sequins-core/src/models/trace.rs:23` - `pub struct Trace` - Has doc comment but missing examples
- `crates/sequins-core/src/models/time.rs:15` - `pub struct Timestamp` - Missing doc comment

## sequins-storage

### Important
- `crates/sequins-storage/src/lib.rs:34` - `pub struct Storage` - Missing doc comment
- `crates/sequins-storage/src/config.rs:12` - `pub struct StorageConfig` - Missing field docs

## Summary

- Total public items: 156
- Documented: 142 (91%)
- Missing docs: 14 (9%)
- Critical issues: 2
- Important issues: 12
```

## Fixing Missing Docs

For each missing doc comment:

1. **Understand the item** - Read the code, understand what it does
2. **Write clear summary** - One line describing purpose
3. **Add details** - Explain non-obvious behavior
4. **Include examples** - Especially for complex APIs
5. **Document errors** - What can go wrong?
6. **Cross-reference** - Link to related types/docs if helpful

### Use Links

Link to related types:
```rust
/// Queries traces using [`TraceQuery`] parameters.
///
/// Returns a [`Vec<Trace>`] or [`StorageError`] on failure.
///
/// See also: [`get_spans`], [`get_services`]
```

## Testing Doc Examples

All doc examples should compile and run. Test them:

```bash
cargo test --doc
```

Fix any examples that don't compile or produce wrong results.

## Quality Checks

Good documentation:
- ✅ Explains **why**, not just what
- ✅ Includes examples for non-trivial APIs
- ✅ Documents errors and edge cases
- ✅ Uses proper formatting (code blocks, links)
- ✅ Is concise but complete
- ✅ Stays in sync with code (verify examples work)

Poor documentation:
- ❌ Just repeats the function name ("Queries traces" for `query_traces`)
- ❌ Missing crucial details (errors, panics, edge cases)
- ❌ Outdated examples that don't compile
- ❌ Too verbose (essay instead of API docs)
- ❌ Missing links to related functionality

## Success Criteria

Documentation is complete when:
- ✅ `cargo doc` builds without warnings
- ✅ `cargo clippy -W missing-docs` passes
- ✅ All trait methods have at least one example (in trait or impl)
- ✅ All public structs explain their purpose
- ✅ All error types document when they occur
- ✅ `cargo test --doc` passes (all examples compile and run)

---

**Remember:** Good documentation is an investment. It helps users (including future you) understand the API without reading implementation details. It's especially critical for traits since they define the public interface.
