# Sequins Development Guidelines

Sequins is a cross-platform OpenTelemetry visualization tool built with GPUI. It provides an embedded OTLP endpoint that local applications can report to, with rich visualizations for traces, logs, metrics, and profiles.

> **📋 Also see:** [`~/Code/rust/CLAUDE.md`](../CLAUDE.md) for Rust-specific guidelines and [`~/Code/CLAUDE.md`](../../CLAUDE.md) for general coding practices.

## Planning Documentation

This project's architecture, design decisions, and implementation roadmap are documented in detail in the `plans/` directory.

**📋 Start here:** [plans/INDEX.md](plans/INDEX.md) - Navigation hub for all planning documents

These planning documents are the **source of truth** for all development work:
- Before implementing a feature, consult the relevant plan document
- If plans change during development, update the corresponding documentation
- All architectural decisions should be reflected in the plans
- Cross-reference related documents for complete context

The planning docs cover:
- **System architecture** - Three-layer design, component communication ([architecture.md](plans/architecture.md))
- **Workspace & crates** - Cargo structure, three-trait architecture ([workspace-and-crates.md](plans/workspace-and-crates.md))
- **Data models** - Trace, Span, Log, Metric, Profile types ([data-models.md](plans/data-models.md))
- **Database schema** - Complete DDL, queries, FTS5 ([database.md](plans/database.md))
- **OTLP ingestion** - gRPC/HTTP endpoints, parsing ([otlp-ingestion.md](plans/otlp-ingestion.md))
- **UI design** - Component hierarchy, views ([ui-design.md](plans/ui-design.md))
- **Deployment modes** - Local vs enterprise ([deployment.md](plans/deployment.md))
- **Scaling strategy** - Multi-node distributed architecture ([scaling-strategy.md](plans/scaling-strategy.md))
- **Implementation roadmap** - 10 phases to v1.0 ([implementation-roadmap.md](plans/implementation-roadmap.md))
- **Technology decisions** - Why GPUI, Turso, Tokio, etc. ([technology-decisions.md](plans/technology-decisions.md))

## Custom Development Skills

Sequins has a comprehensive set of custom skills in `.claude/skills/` that assist with development. **Use these skills proactively** - they exist to maintain quality and consistency.

See [.claude/skills/README.md](.claude/skills/README.md) for complete documentation.

### 📚 Documentation Skills (Use Before Commits)

**`sequins-docs-sync`** - Keep planning docs synchronized with code
- **When:** Before commits, after implementing features
- **Use to:** Check if plans/ docs match actual implementation
- **Example:** "Use sequins-docs-sync to check documentation"

**`sequins-doc-comments`** - Ensure all public APIs have proper doc comments
- **When:** Before commits, during code review
- **Use to:** Verify all `pub` items have `///` doc comments
- **Example:** "Use sequins-doc-comments to verify documentation"

### 🏗️ Architecture Skills (Use During Development)

**`sequins-architecture-guide`** - Conceptual framework for three-trait architecture
- **When:** Designing features, deciding which trait for new methods, considering performance
- **Use to:** Understand OtlpIngest/QueryApi/ManagementApi separation, zero-cost abstractions, radical simplicity
- **Example:** "Use sequins-architecture-guide to understand where this feature belongs"

**`sequins-crate-boundaries`** - Enforce clean crate separation
- **When:** Before commits, when editing Cargo.toml, adding dependencies
- **Use to:** Verify crate dependencies follow architectural rules (core is dependency-free, server uses generics, etc.)
- **Example:** "Use sequins-crate-boundaries to verify architecture"

**`sequins-zero-cost-abstractions`** - Check for trait objects vs generics
- **When:** After implementing features, code review
- **Use to:** Find `dyn Trait` usage and convert to generics (`impl<Q: QueryApi>`)
- **Example:** "Use sequins-zero-cost-abstractions to check for trait objects"

### 🔧 Implementation Skills (Use When Building)

**`sequins-storage-guide`** - Patterns for Storage and two-tier architecture
- **When:** Implementing storage features, optimizing queries, working on data lifecycle
- **Use to:** Understand hot tier (Papaya) and cold tier (Parquet+DataFusion) patterns
- **Example:** "Use sequins-storage-guide for hot/cold tier implementation"

**`sequins-storage-patterns`** - Lint hot→cold→merge→limit query pattern
- **When:** After implementing query methods, before committing storage changes
- **Use to:** Ensure all queries follow consistent hot→cold tier pattern
- **Example:** "Use sequins-storage-patterns to verify query implementation"

**`sequins-lock-free-guide`** - Correct Papaya HashMap usage
- **When:** Working with hot tier, debugging hangs, performance issues
- **Use to:** Understand Papaya guard lifecycle, avoid holding guards across `.await`
- **Example:** "Use sequins-lock-free-guide to check Papaya usage"

**`sequins-trait-coordinator`** - Coordinate far-reaching trait/type changes
- **When:** Adding/modifying trait methods, changing type signatures
- **Use to:** Ensure all implementations, usages, tests, and docs are updated
- **Example:** "Use sequins-trait-coordinator to add method to QueryApi"

### 🐛 Debugging Skills (Use When Needed)

**`sequins-lldb-debugger`** - Debug crashes, deadlocks, and hangs with LLDB
- **When:** Application crashes (segfault/panic) or becomes unresponsive
- **Use to:** Systematic debugging workflows for common failure modes
- **Example:** "Use sequins-lldb-debugger to debug the hang"

### Skill Usage Workflow

**Starting a new feature:**
1. `sequins-architecture-guide` - Understand which trait the feature belongs to
2. `sequins-storage-guide` or `sequins-lock-free-guide` - Implementation patterns
3. `sequins-trait-coordinator` - If adding trait methods, coordinate changes across layers

**Before committing:**
1. `sequins-storage-patterns` - Verify query patterns (if storage work)
2. `sequins-zero-cost-abstractions` - Check for trait objects
3. `sequins-crate-boundaries` - Verify dependencies
4. `sequins-doc-comments` - Ensure doc comments exist
5. `sequins-docs-sync` - Update planning docs

**When things break:**
1. `sequins-lldb-debugger` - Debug crashes and hangs

**General principle:** Use skills proactively, not reactively. They prevent issues rather than just finding them.

## Project Overview

**Purpose:** A local-first observability tool that provides:
- Embedded OTLP endpoint (gRPC, HTTP, HTTP+JSON)
- Service map visualization
- Distributed trace visualization
- Log search and viewing with structured data support
- Metrics dashboards
- Profile flame graphs

**Architecture:**
- UI Layer: GPUI (cross-platform GPU-accelerated UI framework)
- Business Logic: Async Rust with Tokio
- Data Layer: Turso (libSQL) for persistent storage
- Network: OTLP endpoints for telemetry ingestion

## Business Model

Sequins follows a **free local, paid enterprise** model:

**FREE (Local Development):**
- Desktop app with full UI
- Embedded OTLP server (gRPC + HTTP) for local services
- Direct database access (no network overhead)
- Perfect for individual developers

**PAID (Enterprise):**
- Deploy `sequins-daemon` in your network/cloud
- Centralized telemetry from all environments
- Multiple developers connect their apps remotely
- Query API with authentication
- Team collaboration features

**Architecture enables this by:**
- Composable `sequins-server` crate (OTLP ingest + optional Query API)
- App embeds OTLP-only server (free)
- Daemon runs full server with Query API (paid)
- Three traits separate concerns: OtlpIngest, QueryApi, ManagementApi (transparent)

See [plans/deployment.md](plans/deployment.md) for deployment scenarios and [plans/architecture.md](plans/architecture.md) for detailed architecture.

## Key Dependencies

See [plans/technology-decisions.md](plans/technology-decisions.md) for detailed rationale.

- **`gpui`** - GPU-accelerated UI framework (cross-platform)
- **`opentelemetry`** + **`opentelemetry-proto`** - OTLP types and protobuf definitions
- **`tonic`** + **`axum`** - gRPC and HTTP servers for OTLP endpoints
- **`libsql`** - Turso's SQLite-compatible embedded database
- **`tokio`** - Async runtime
- **`serde`** + **`serde_json`** - Serialization

## Project Structure

**Workspace crates:**
- **sequins-core** - Shared types and three traits (OtlpIngest, QueryApi, ManagementApi)
- **sequins-storage** - Complete data layer; TursoStorage implements all three traits
- **sequins-server** - Protocol adapters; generic over traits (OtlpServer<I>, QueryServer<Q>)
- **sequins-client** - RemoteClient implements QueryApi + ManagementApi via HTTP
- **sequins-app** - GPUI desktop UI; uses generics for zero-cost abstractions
- **sequins-daemon** - Enterprise server binary

See [plans/workspace-and-crates.md](plans/workspace-and-crates.md) for detailed structure and [plans/architecture.md](plans/architecture.md) for the three-trait design philosophy

## Building & Running

### Workspace Commands

Build all crates:
```bash
cargo build
```

Build specific crate:
```bash
cargo build -p sequins-app
cargo build -p sequins-core
cargo build -p sequins-storage
cargo build -p sequins-server
cargo build -p sequins-client
cargo build -p sequins-daemon
```

Run the desktop app:
```bash
cargo run -p sequins-app
```

Run with logging:
```bash
RUST_LOG=debug cargo run -p sequins-app
```

### Release Build
```bash
cargo build --release -p sequins-app
./target/release/sequins-app
```

## Testing

**Run tests:**
```bash
cargo test --workspace              # All tests
cargo test -p sequins-storage       # Specific crate
cargo test --workspace -- --nocapture  # Show output
```

**Test categories:**
- Unit tests with `#[cfg(test)]`
- Integration tests in `tests/`
- GPUI components: Use `gpui::TestAppContext`, test state separately from rendering

## Code Quality

**Before committing:**
1. `cargo fmt --all`
2. `cargo clippy --workspace -- -D warnings`
3. `cargo test --workspace`
4. Use skills: `sequins-storage-patterns`, `sequins-zero-cost-abstractions`, `sequins-crate-boundaries`, `sequins-doc-comments`, `sequins-docs-sync`

## Workspace Guidelines

**Crate boundaries** (use `sequins-crate-boundaries` skill to verify):
- **sequins-core**: Types and traits only. No business logic. No other sequins crate dependencies.
- **sequins-storage**: Owns data lifecycle. Implements all three traits. Never imports app/client/server.
- **sequins-server**: Protocol adapters. Uses generics over traits. Never imports app/client/storage.
- **sequins-client**: Remote HTTP client. Implements QueryApi + ManagementApi. No OtlpIngest.
- **sequins-app** & **sequins-daemon**: Top-level binaries.

**Dependencies:**
- Add to specific crate's `Cargo.toml`, not workspace root: `cd crates/sequins-storage && cargo add libsql`
- Use workspace inheritance for shared deps (see root `Cargo.toml`)

## GPUI Guidelines

**Key patterns:**
- Component structure: `impl Render for MyComponent`
- State: `Model<T>` for shared state, `View<T>` for UI components
- Events: `.on_click()`, `.on_key_down()`, etc.
- Performance: Use `cx.observe()` for reactive updates; avoid heavy computation in `render()`

See [plans/ui-design.md](plans/ui-design.md) for component hierarchy and detailed UI patterns.

## OpenTelemetry & Data Handling

**Type mapping:**
- Use `opentelemetry::trace::{TraceId, SpanId}` in code
- Store as hex strings (TEXT) in database: `trace_id.to_string()` / `TraceId::from_str(&hex)?`
- Timestamps: nanoseconds since epoch (i64), UTC in storage

**Data flow:**
1. OTLP endpoint receives data
2. Parse from protobuf/JSON
3. Validate and enrich
4. Insert into database
5. Query for UI visualization

See [plans/data-models.md](plans/data-models.md) for type definitions, [plans/otlp-ingestion.md](plans/otlp-ingestion.md) for ingestion pipeline, and [plans/database.md](plans/database.md) for schema.

## Data Retention

**RetentionManager** in `sequins-storage/src/retention.rs`:
- Owned by `TursoStorage`
- Background task runs periodic cleanup
- Per-type policies (traces, logs, metrics, profiles)

**Basic usage:**
```rust
let storage = TursoStorage::with_defaults(&db_path)?;  // 24h default
storage.start_retention();  // Start background task
storage.shutdown().await;   // Graceful shutdown
```

See `sequins-storage/src/retention.rs` for `RetentionPolicy` configuration and manual cleanup via `run_retention_cleanup()`.

## Error Handling & Performance

**Errors:**
- Application: `anyhow::Result` with `.context()`
- Libraries: `thiserror` for typed errors
- UI: Display actionable errors, don't crash on ingestion failures

**Performance:**
- Database: WAL mode (libSQL default), indexes, prepared statements, batch inserts
- UI: Virtualize lists, lazy-load details, debounce inputs, render viewport only
- Memory: Stream results, pagination for large sets

See [plans/database.md](plans/database.md) for query optimization and indexing strategies.

## Configuration

**Default OTLP ports:**
- gRPC: `4317`
- HTTP: `4318`

**Settings UI should include:**
- OTLP endpoint ports
- Data retention period
- Theme (light/dark)
- Database location

See [plans/configuration.md](plans/configuration.md) for configuration file format and environment variables.

## Common Patterns

**Three-trait architecture:**
- **OtlpIngest**: Ingestion methods (only TursoStorage)
- **QueryApi**: Query methods (TursoStorage + RemoteClient)
- **ManagementApi**: Admin methods (TursoStorage + RemoteClient)

**Critical rules:**
- Use generics with trait bounds (`impl<Q: QueryApi>`), NOT trait objects (`dyn QueryApi`)
- TursoStorage implements all three traits (local mode)
- RemoteClient implements QueryApi + ManagementApi only (remote mode via HTTP)
- Servers are generic: `OtlpServer<I: OtlpIngest>`, `QueryServer<Q: QueryApi>`

**For implementation examples, use skills:**
- `sequins-architecture-guide` - Understanding trait separation and zero-cost abstractions
- `sequins-storage-guide` - TursoStorage patterns and hot/cold tier
- `sequins-zero-cost-abstractions` - Verify no `dyn Trait` usage
- `sequins-trait-coordinator` - When adding/modifying trait methods

See [plans/architecture.md](plans/architecture.md) and [plans/workspace-and-crates.md](plans/workspace-and-crates.md) for detailed architectural patterns.

## Development Workflow

**Standard workflow:**
1. Feature branch from main
2. Implement in appropriate crate (respect boundaries - use `sequins-crate-boundaries` skill)
3. Write tests: `cargo test --workspace`
4. Format & lint: `cargo fmt --all && cargo clippy --workspace -- -D warnings`
5. Use skills before committing (see "Code Quality" section above)
6. Manual test: `cargo run -p sequins-app`
7. Commit with descriptive messages

**Cross-crate features** (use `sequins-trait-coordinator` skill):
1. **sequins-core** - Define types and trait methods
2. **sequins-storage** - Implement in TursoStorage
3. **sequins-client** - Implement in RemoteClient (if QueryApi/ManagementApi)
4. **sequins-server** - Add HTTP endpoints (generic handlers)
5. **sequins-app** - Add UI components (use generics with trait bounds)

## Resources

- [GPUI Documentation](https://www.gpui.rs/)
- [OpenTelemetry Protocol Spec](https://opentelemetry.io/docs/specs/otlp/)
- [Tokio Tutorial](https://tokio.rs/tokio/tutorial)
- [Turso Documentation](https://docs.turso.tech/)
- [libSQL Documentation](https://github.com/tursodatabase/libsql)

## Questions?

When in doubt:
- Check [plans/INDEX.md](plans/INDEX.md) for architecture decisions and design documentation
- Look for similar patterns in the codebase
- Refer to parent CLAUDE.md files for general Rust guidelines
- Ask for clarification on design decisions
