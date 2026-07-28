# Workspace API Surface Audit

This document summarizes a read-only review of the public API surface of each
workspace crate and the downstream users inside this workspace. It is intended
to guide API narrowing work, not to preserve the current shape.

The core architectural goal is to make each component replaceable behind a
small contract. Public exports should describe what a crate promises, not the
current internal module layout.

OpenTelemetry is Sequins' ingest/domain model. This audit treats OTLP ingest,
OTLP-aligned WAL behavior, and OTEL-shaped Arrow schemas as intentional product
architecture. The API issue is when generated protobuf plumbing, broad facade
exports, or protocol-specific helper modules leak through crates that claim a
smaller contract.

## Audit Method

The review used `cargo metadata`, crate root exports, public item searches, and
workspace call-site searches. Findings were grouped by subsystem and checked
against downstream Rust, Swift/FFI, CLI, and app usage where visible.

This audit is scoped to in-workspace usage. These crates are treated as internal
workspace components, so cleanup work may migrate downstream workspace users and
remove old crate surfaces in the same change set.

## Summary Findings

The largest API-contract problems are umbrella facades and implementation module
leaks:

- `sequins-query` is a broad facade over `seql-*`, `sequins-flight`, and
  `sequins-traits`, while stale duplicate source files remain in the crate.
- `sequins-types` re-exports schema and trait crates, making domain models a
  dependency hub.
- `sequins-storage` re-exports hot tier, cold tier, WAL, and live-query crates,
  allowing downstream code to bypass storage boundaries.
- `sequins-cold-tier`, `sequins-hot-tier`, `sequins-companion-index`, and
  `sequins-vortex-indexed-layout` expose implementation modules as public API.
- `sequins-traits` claims to be light but exposes Flight retrieval and generated
  OTLP protobuf types through a protocol-neutral-looking facade.
- `sequins-ffi` exports typed query stubs and model surfaces that appear to
  exist primarily for header generation.

## Query and Language Crates

Crates covered: `seql-ast`, `seql-parser`, `seql-substrait`,
`sequins-query`, `sequins-flight`, and `sequins-traits`.

### Intended Contracts

- `seql-ast`: syntax tree, query modes, signal names, and AST value types.
- `seql-parser`: `parse(&str) -> QueryAst` plus parse error metadata.
- `seql-substrait`: SeQL AST/text to Substrait plan compilation entrypoints.
- `sequins-flight`: query stream wire metadata, FlightData builders, and IPC
  frame encoding/decoding.
- `sequins-traits`: protocol-neutral contracts only.
- `sequins-query`: eliminate as a broad umbrella, or reduce to a deliberately
  narrow query contract crate with no re-exported lower-level implementation
  surfaces.

### Current Surface Issues

- `seql-ast/src/lib.rs` wildcard-exports `ast`, `correlation`, and `schema`.
  Correlation helpers are only needed by compiler planning. Result schema types
  such as `ResponseShape`, `ColumnDef`, `ColumnRole`, and `DataType` are used by
  clients, but they are not pure AST syntax.
- `seql-parser` has a clean external surface, but its manifest declares
  `sequins-types` even though current parser sources do not use it.
- `seql-substrait` publicly exports stage-level compiler internals such as
  `apply_filter`, `apply_project`, `apply_compute`, `apply_aggregate`,
  `apply_sort`, `apply_limit`, `apply_unique`, `ast_expr_to_df_expr`, and
  `ast_to_logical_plan`. Production callers mainly need `compile`,
  `compile_ast`, `schema_context`, and possibly `time_column_for_signal`.
- `seql-substrait` exposes the stream reducer even though reducer behavior is
  Flight/client-side stream handling, not Substrait compilation. This is also
  why the compiler crate depends on `sequins-flight`.
- `sequins-query` is mostly an umbrella facade. Downstream users currently
  import `sequins_query::parser`, `sequins_query::ast`, `sequins_query::schema`,
  `sequins_query::flight`, `sequins_query::frame`, query errors, query traits,
  and compiler entrypoints.
- `sequins-query` still contains stale duplicate parser, schema, correlation,
  and generated proto files that are not the active exports.
- `sequins-flight` wildcard-exports all frame and Flight helpers. It also mixes
  canonical wire metadata with callback/data-frame DTOs.
- `sequins-flight::batch_to_ipc` can hide Arrow IPC failures by returning empty
  bytes instead of a result.
- `sequins-traits::SeqlStream` is `FlightData`, binding generic query traits to
  Arrow Flight. `OtlpIngest` is bound to generated OTLP protobuf
  request/response types even though OTLP ingest should be an explicit OTEL
  contract.

### Narrowing Actions

1. Keep `seql-parser` as the model parser contract and remove its unused
   `sequins-types` dependency.
2. Decide whether `sequins-query` owns a narrow query contract at all. If not,
   migrate all downstream workspace users to `seql-parser`, `seql-ast`,
   `seql-substrait`, `sequins-flight`, and the real trait crate directly, then
   remove the umbrella exports and stale duplicate modules.
3. Narrow `seql-substrait` exports to compiler entrypoints and move reducer code
   to `sequins-flight`, `sequins-view`, or a dedicated query-client crate.
4. Replace wildcard exports in `seql-ast` and `sequins-flight` with explicit
   root exports.
5. Move Flight-coupled query traits out of `sequins-traits`; move OTLP ingest
   traits behind an explicit OTEL ingest contract or feature.
6. Make IPC encoding helpers return `Result` and propagate encoding errors
   through query execution.

## Contracts, Schemas, and Ingest

Crates covered: `sequins-types`, `sequins-arrow-schema`,
`sequins-attribute-codec`, `sequins-otlp`, and `sequins-pprof`.

### Intended Contracts

- `sequins-types`: passive domain models, IDs, time values, health config, and
  model serialization.
- `sequins-arrow-schema`: canonical table schemas, `SchemaCatalog`,
  `PromotedAttribute`, and logical `SignalType`.
- `sequins-attribute-codec`: overflow attribute map construction and optional
  DataFusion UDF registration.
- `sequins-otlp`: OTLP protobuf to Arrow `RecordBatch` conversion.
- `sequins-pprof`: pprof parser entrypoint, if the crate remains integrated.

### Current Surface Issues

- `sequins-types` re-exports all Arrow schema modules and `sequins-traits`.
  Many downstream users still import schemas through
  `sequins_types::arrow_schema`, `sequins_types::schema_catalog`, and
  `sequins_types::SignalType`.
- `sequins-types::metric_grouping` appears to have no non-test Rust callers in
  the scan. It is behavior/policy code rather than core model definition.
- `sequins-arrow-schema` exposes every module publicly. `column_names` and
  `ext_dtypes` are implementation/policy helpers, and several schema helper
  functions look legacy or internal.
- `sequins-arrow-schema::SignalType` includes hot-tier array dispatch details
  such as `COUNT` and `index`, which are storage implementation concerns.
- `sequins-attribute-codec` exposes raw CBOR encode/decode helpers,
  `OverflowMapBuilder`, and domain-model overflow construction. In-workspace
  production usage is routed through `sequins-otlp`.
- `sequins-otlp` exposes converter implementation modules and helper internals.
  Storage mainly needs top-level conversion functions and limited
  resource/scope conversion helpers.
- `sequins-otlp` currently routes DataFusion overflow UDF registration, causing
  query execution to depend on OTLP conversion.
- `sequins-pprof` exposes a module containing one parser function and appears
  unused by downstream workspace crates.

### Narrowing Actions

1. Migrate schema imports from `sequins-types` to `sequins-arrow-schema`, and
   trait imports from `sequins-types` to the correct contract crate.
2. Remove `sequins-types` schema/trait re-exports after call-site migration.
3. Keep `sequins-arrow-schema` as a root-level schema facade. Make helper
   modules private or `pub(crate)` unless they are explicitly intended as
   public policy.
4. Move hot-tier indexing behavior out of `SignalType`.
5. Narrow `sequins-attribute-codec` to overflow map construction and optional
   UDF registration. Hide raw CBOR and builder internals.
6. Gate `sequins-attribute-codec/datafusion` through the corresponding
   `sequins-otlp/datafusion` feature instead of enabling it unconditionally.
7. Import overflow UDF registration directly from `sequins-attribute-codec` in
   query execution code.
8. Make `sequins-otlp` modules private and expose conversion functions plus
   `ProfileBatches`.
9. Either root-export `parse_pprof_to_samples` and hide `pprof_parser`, or defer
   `sequins-pprof` until there is an integrated caller.

## Storage, WAL, Tiers, and Indexes

Crates covered: `sequins-storage`, `sequins-wal`, `sequins-live-query`,
`sequins-batch-chain`, `sequins-hot-tier`, `sequins-cold-tier`,
`sequins-series-index`, `sequins-companion-index`, and
`sequins-vortex-indexed-layout`.

### Intended Contracts

- `sequins-storage`: unified storage engine facade plus management and query
  access ports.
- `sequins-wal`: durable append/subscribe API, not segment writer internals.
- `sequins-live-query`: heartbeat/subscription support if kept standalone.
- `sequins-batch-chain`: appendable hot table provider abstraction.
- `sequins-hot-tier`: hot storage operations and stats.
- `sequins-cold-tier`: cold storage operations, config, and query file format.
- `sequins-series-index`: series ID and lookup operations.
- `sequins-companion-index`: companion index builders and serialized payloads.
- `sequins-vortex-indexed-layout`: indexed layout registration/strategy.

### Current Surface Issues

- `sequins-storage` publicly re-exports `cold_tier`, `hot_tier`, `live_query`,
  and `wal`, then re-exports many of their internals. DataFusion backend uses
  storage accessors such as `hot_tier_arc`, `cold_tier_arc`, `wal`, and
  `live_broadcast_tx`, which means query execution depends on storage internals.
- `Storage::generate_test_data` is part of the main storage surface despite
  being test/demo behavior.
- `sequins-wal` exports `WalSegment`, `WalWriter`, `WriterConfig`, and segment
  metadata. Production users mainly need `Wal`, `WalConfig`, `WalPayload`,
  subscription, and watermark/event behavior.
- `sequins-live-query` exposes implementation modules. Production usage is
  mostly `HeartbeatEmitter`; `LiveQueryManager` is held by storage but does not
  appear to be a broad standalone contract.
- `sequins-batch-chain::BatchNode` is public because `head_arc` exposes
  `Arc<Atomic<BatchNode>>`, leaking lock-free compaction details.
- `sequins-hot-tier` exposes `batch_chain`, `core`, `config`, and `error`
  modules, and re-exports `SignalType` from another crate.
- `sequins-cold-tier` publicly exposes helpers, partitioning, record-batch
  writing, all write modules, compaction, query, rollups, and index crates.
  `ColdTier` also has public fields for config, store, and series index.
- `sequins-series-index::SeriesId(pub u64)` permits arbitrary construction even
  though constructor/accessor methods exist. Persistence methods are public
  though only cold tier appears to own object-store persistence.
- `sequins-companion-index` exposes bloom, trigram, log, and span modules.
  Production cold-tier usage primarily builds indexes and converts them into
  serialized `CompanionIndexBytes`.
- `sequins-vortex-indexed-layout` exposes `reader`, public metadata fields, and
  re-exports `CompanionIndexBytes` through the strategy module.

### Narrowing Actions

1. Define a `sequins-storage` query-facing facade for the DataFusion backend:
   hot table provider lookup, cold query/file-format access, WAL watermark or
   event subscription, and live broadcast subscription.
2. Migrate backend/tests away from `sequins_storage::{hot_tier,cold_tier,wal}`
   module paths before making those modules private.
3. Move `generate_test_data` behind a dev/test feature or out of the main
   storage contract.
4. Hide WAL segment and writer internals unless explicit recovery tooling needs
   them.
5. Hide batch-chain nodes and compaction internals behind crate-private handles.
6. Make hot-tier and cold-tier modules private while preserving root exports for
   real facade types such as `HotTier`, `HotTierConfig`, `ColdTier`,
   `ColdTierConfig`, `CompanionIndexConfig`, stats, errors, and storage
   operations.
7. Make `ColdTier` fields private and expose accessors only where they are part
   of the intended storage/query contract.
8. Make `SeriesId` field private and keep `new`/`as_u64`.
9. Hide companion index implementation modules unless bloom/trigram are meant to
   be standalone libraries.
10. Hide indexed-layout reader internals and expose root-level registration and
    strategy APIs.

## Query Execution, View, Client, Server, and Apps

Crates/apps covered: `sequins-datafusion-backend`, `sequins-view`,
`sequins-client`, `sequins-server`, `sequins-ffi`, `sequins-cli`, `apps/linux`,
and `example`.

### Intended Contracts

- `sequins-datafusion-backend`: query execution facade over a storage query
  input.
- `sequins-view`: transport-neutral view strategies and deltas.
- `sequins-client`: remote query client, with transport separated from optional
  query compilation over time.
- `sequins-server`: server constructors/types, not handler state internals.
- `sequins-ffi`: stable C ABI for data source, management, SeQL streams, views,
  and memory release functions.
- `sequins-cli`, `apps/linux`, and `example`: internal apps, not reusable crate
  APIs.

### Current Surface Issues

- `sequins-datafusion-backend` is relatively tight. The main design issue is
  that `DataFusionBackend::new` requires concrete `Storage`, coupling execution
  to the storage implementation.
- `sequins-view` exports a concise root API but leaves `delta`, `strategy`, and
  `strategies` modules public. Downstream users only need root exports:
  `ViewDelta`, `ViewDeltaStream`, `ViewStrategy`, `TableStrategy`,
  `AggregateStrategy`, and `FlamegraphStrategy`.
- `sequins-view` also consumes `SeqlStream` directly and decodes Flight/Arrow
  frames, coupling view logic to transport.
- `sequins-client` has a small API, but implementing/exposing raw `QueryExec`
  lets callers send plan bytes through a client that may only need a SeQL text
  contract.
- `sequins-server` exposes whole modules plus root re-exports. `FlightSqlState`
  is public with a public `query_exec` field despite being an internal service
  wrapper.
- `sequins-ffi` has true public ABI through `#[no_mangle] extern "C"` and the
  generated header. Stable usage centers on data source lifecycle, management,
  SeQL parse/query/live stream, view creation/cancel/free, and free functions.
- `sequins-ffi` also exports typed query stubs such as
  `sequins_query_logs_stub`, `sequins_query_metrics_stub`,
  `sequins_query_profiles_stub`, and `sequins_query_spans_stub`; these return
  empty results and appear to exist for C header/model generation.
- `sequins-cli` and `apps/linux` expose incidental `pub` items inside binary
  crates. These are not workspace library API surfaces.

### Narrowing Actions

1. Keep `sequins-datafusion-backend` public API mostly intact while moving its
   input from concrete `Storage` toward a narrow storage query trait/facade.
2. Make `sequins-view` modules private and keep root-level re-exports.
3. Add a transport-neutral `ViewInputFrame` before trying to fully decouple view
   strategies from Flight.
4. Keep `RemoteClient` as the main `sequins-client` type. Split lower-level
   Flight plan transport from text-query compilation if dependency reduction is
   needed.
5. Make `sequins-server` modules private and re-export only intended server
   constructors/types. Hide `FlightSqlState`.
6. Split `sequins-ffi` into stable ABI and legacy/test ABI. Feature-gate or
   remove typed query stubs once Swift/header generation no longer requires
   them.
7. Treat `sequins-cli`, `apps/linux`, and `example` as app internals. Narrow
   incidental `pub` items opportunistically after library contracts are cleaned
   up.

## Recommended Cleanup Sequence

1. Remove low-risk manifest and stale-code leaks:
   - Remove `seql-parser -> sequins-types`.
   - Delete inactive duplicate modules in `sequins-query` after verifying they
     are not referenced.
   - Replace wildcard root exports where downstream imports already use the
     intended direct crates.
2. Migrate umbrella call sites:
   - Move schema imports from `sequins_types::*` to `sequins_arrow_schema::*`.
   - Move trait imports from `sequins_types::*` to their real contract crate.
   - Move Flight/frame imports from `sequins_query::*` to `sequins_flight::*`
     and parser/AST/compiler imports to the owning `seql-*` crates.
   - Move storage tier imports away from `sequins_storage::{hot_tier,cold_tier,
     wal,live_query}`.
3. Introduce missing narrow contracts:
   - Storage query facade for DataFusion.
   - Query stream/Flight contract outside `sequins-traits`.
   - Optional transport-neutral view frame.
4. Make implementation modules private:
   - Cold tier write/query/helper modules.
   - Hot tier core/batch-chain modules.
   - WAL segment/writer modules.
   - Companion index bloom/trigram internals.
   - Indexed layout reader internals.
   - View/server implementation modules.
5. Tighten FFI as a separate ABI cleanup:
   - Identify Swift-required symbols from `apps/macos`.
   - Move typed query stubs and unused result surfaces behind a legacy feature
     or remove them once current Swift users are migrated.

## Acceptance Checks

Run these checks after each migration phase:

- `cargo check --workspace --all-targets`
- `rg "sequins_types::arrow_schema|sequins_types::schema_catalog|sequins_types::SignalType"`
- `rg "sequins_storage::(hot_tier|cold_tier|wal|live_query)"`
- `rg "sequins_query::(flight|frame|schema|parser|ast)"`
- `cargo tree -i sequins-attribute-codec --workspace`

Expected end state:

- Schema users import `sequins-arrow-schema` directly.
- Trait users import the true contract crate directly.
- Storage users do not depend on hot/cold/WAL modules unless they are tests or
  explicit low-level tooling.
- Query execution does not depend on `sequins-otlp` just to register overflow
  UDFs.
- `sequins-traits` no longer requires Arrow Flight or OTLP protobuf for
  protocol-neutral consumers.
- Public crate roots expose explicit facade types and functions, not wildcard
  module trees.

## Assumptions

- No compatibility window is required for Rust workspace crates; migrate
  downstream workspace users and remove old surfaces in the same cleanup.
- FFI is still a generated ABI consumed by the macOS app, so removals there
  should be coordinated with Swift call-site/header generation updates.
- Binary crates are internal apps and should not drive library API design.
