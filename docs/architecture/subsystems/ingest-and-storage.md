# Ingest and Storage

Crates covered: `sequins-otlp`, `sequins-attribute-codec`, `sequins-wal`,
`sequins-batch-chain`, `sequins-hot-tier`, `sequins-cold-tier`,
`sequins-storage`, and `sequins-live-query`.

## Intended Purpose

The ingest/storage subsystem should separate these concerns while preserving
Sequins' OTEL domain model:

- OTLP decoding and Arrow conversion.
- Attribute encoding and promoted-column handling.
- OTLP-native durable append/replay log.
- Hot in-memory batches.
- Cold object-store layout and compaction.
- Storage orchestration and management APIs.
- Live query subscription and stream coordination.

Intended public contracts:

- `sequins-otlp`: OTLP-to-Arrow conversion and OTEL-oriented batch outputs.
- `sequins-attribute-codec`: promoted/overflow attribute encoding helpers and
  explicit DataFusion UDF registration.
- `sequins-wal`: durable OTLP-aligned append, replay, watermark, and
  subscription behavior.
- `sequins-storage`: storage-level orchestration and query-facing access ports,
  not a public umbrella for tier internals.

## Current Weaknesses

### OTLP Conversion and Domain Model Conversion Are Mixed

`sequins-otlp` describes itself as direct Arrow conversion without domain types,
but publicly exports helpers that return `sequins-types` models.

Why it matters:

- A conversion crate owns both physical Arrow ingestion and metadata registry
  hydration.
- Callers cannot choose only one layer cleanly.

Direction:

- Keep OTLP-to-Arrow conversion as the core responsibility.
- Move domain model adapters behind a feature or into a separate adapter module.

Priority: P1.

### DataFusion Feature Is Effectively Bypassed in OTLP/Codec

`sequins-otlp` depends on `sequins-attribute-codec` with the codec's
`datafusion` feature enabled unconditionally, despite having its own optional
feature.

Why it matters:

- Plain OTLP conversion consumers inherit DataFusion dependencies.

Direction:

- Make `sequins-attribute-codec` dependency feature-neutral by default.
- Wire `sequins-otlp/datafusion` to enable both `dep:datafusion` and
  `sequins-attribute-codec/datafusion`.

Priority: P1.

### Attribute Codec Exposes OTLP Plumbing Too Broadly

`sequins-attribute-codec` depends on `opentelemetry-proto` and public APIs accept
OTLP `KeyValue` types.

Why it matters:

- Low-level protobuf plumbing becomes part of the codec's broad public surface.
- Callers that only need OTEL-shaped promoted/overflow attributes inherit raw
  OTLP request semantics.

Direction:

- Keep the codec OTEL-oriented, but expose a narrower attribute value/view API
  rather than raw protobuf builders everywhere.
- Move protobuf adapter functions into `sequins-otlp`.
- Keep promoted-attribute routing in the codec crate behind explicit APIs.

Priority: P1.

### Attribute Encoding Errors Are Hidden

CBOR encoding helpers and builder finalization paths can return empty bytes or
ignore errors.

Why it matters:

- Encoding corruption can be stored as apparently valid empty binary data.

Direction:

- Introduce `AttributeCodecError`.
- Return `Result` from encode helpers and array builders.
- Propagate errors to ingest callers instead of silently filling invalid values.

Priority: P1.

### WAL Exposes OTLP Internals Instead of an OTEL Ingest Contract

`sequins-wal::WalPayload` stores OTLP protobuf export requests directly.

Why it matters:

- OTLP-native WAL alignment is intentional, but generated protobuf details leak
  into code that should only need append/replay/watermark contracts.
- WAL evolution and recovery policy are harder to manage when callers depend on
  the concrete request representation.

Direction:

- Preserve OTLP-native append/replay semantics.
- Add versioned OTEL ingest entries or a narrow payload wrapper so generated
  protobuf details are hidden behind WAL APIs.
- Keep replay, migration, and recovery behavior explicit without making the WAL
  a generic event log.

Priority: P1.

### WAL Recovery Silently Skips Errors

Startup discovery scans segment contents and skips list/read errors.

Why it matters:

- Corruption, permissions, and transient object-store failures can look like an
  empty or partial WAL.

Direction:

- Persist segment metadata or trailers.
- Distinguish not-found from read/corruption errors.
- Make recovery policy explicit in config.

Priority: P1.

### Batch Chain Exposes Unsafe Internals

`BatchNode` is public and `head_arc()` exposes
`Arc<crossbeam_epoch::Atomic<BatchNode>>`.

Why it matters:

- Hot-tier must understand batch-chain internals to compact or flush.
- Unsafe implementation details become public API.

Direction:

- Expose a compactor or `spawn_compactor` API.
- Hide raw epoch pointers and nodes.
- Split or feature-gate DataFusion `TableProvider` and async compaction from
  core chain storage if the crate is meant to be reusable.

Priority: P1.

### Hot-Tier Re-Exports Batch Chain as Compatibility Surface

`sequins-hot-tier` exposes `sequins-batch-chain` through both root re-exports and
a `batch_chain` module.

Why it matters:

- Multiple public paths identify the same lower-level API.
- Callers can depend on internals through the hot-tier crate.

Direction:

- Deprecate compatibility paths.
- Make hot-tier expose signal-aware append/query APIs rather than batch-chain
  internals.

Priority: P1.

### Hot-Tier Resource and Scope IDs Are Raw Hashes

Resource and scope IDs are raw `u32` FNV hashes.

Why it matters:

- Identity derivation is a storage implementation detail.
- Collision risk and placeholder IDs leak into other crates.

Direction:

- Move identity derivation into typed domain/model code or a metadata index.
- Use typed newtypes and wider stable hashes.

Priority: P1.

### Cold-Tier Public Surface Is Too Wide

`sequins-cold-tier` publicly exposes implementation modules and public fields on
`ColdTier`, including config, object store, and series index.

Why it matters:

- External callers can bypass invariants around path layout, index persistence,
  and object-store behavior.
- Implementation modules become stable public API.

Direction:

- Keep `ColdTier`, config, query/write entrypoints, and selected rollup types
  public.
- Make fields private with focused accessors or constructor injection for tests.
- Stop re-exporting index/layout crates wholesale.

Priority: P1.

### Cold-Tier Layout and Query Logic Are Duplicated

Signal path mapping is repeated across write, compaction, helper, and partition
code. Query paths reconstruct schemas and duplicate scan/filter loops.

Why it matters:

- Storage path and schema changes require edits in multiple modules.
- Object-store list failures can become empty query results.

Direction:

- Centralize a `SignalLayout` table using the shared catalog.
- Reuse shared schemas from `sequins-arrow-schema`.
- Add a reusable Vortex scan helper.
- Return or trace storage errors intentionally.

Priority: P1.

### `sequins-storage` Is a Re-Export Umbrella and Local Lifecycle Owner

`sequins-storage` re-exports tier crates and WAL, while `Storage` owns ingest
conversion, WAL, hot/cold tiers, live broadcast, retention, health config, and
background lifecycle.

Why it matters:

- Storage is both a facade and an engine.
- Callers can reach through it into every layer.
- Local lifecycle behavior is hard to change independently from ingest or
  management.

Direction:

- Expose storage-level APIs only.
- Split concepts into storage engine, ingest pipeline, management store, and
  local background supervisor.
- Move low-level access to direct crate dependencies for code that truly needs
  them.

Priority: P1.

### Live Query Crate Has Shrunk Below a Clear Boundary

`sequins-live-query` currently owns subscription accounting and heartbeat while
execution lives in storage/DataFusion backend.

Why it matters:

- The crate is too small to justify its dependency surface unless it becomes the
  protocol owner for live query behavior.

Direction:

- Either fold subscription accounting into storage/runtime, or expand the crate
  into a real live-query protocol/execution abstraction.
- Move heartbeat encoding next to Flight/query protocol if it remains
  transport-specific.

Priority: P1.

## Boundary Decisions

- Keep WAL separate and OTLP-native, while hiding generated protobuf details
  behind append/replay contracts.
- Keep cold-tier, hot-tier, and batch-chain separate, but hide internals.
- Keep OTLP conversion separate from storage.
- Treat `sequins-storage` as orchestration, not a public umbrella for all tier
  crates.
- Re-evaluate `sequins-live-query` after the query protocol boundary is cleaned.

## Acceptance Checks

- OTLP conversion can build without DataFusion.
- Attribute codec exposes narrow OTEL-oriented attribute APIs without forcing
  broad protobuf plumbing on all callers.
- WAL tests cover not-found, corruption, and read failure recovery behavior.
- Hot-tier callers no longer need batch-chain internals to append signal batches.
- Cold-tier path/schema behavior is driven by one layout/catalog definition.
