# Sequins Code Quality Roadmap

This document summarizes a read-only, crate-by-crate audit of the Sequins
workspace. It focuses on maintainability, API contracts, module boundaries, and
the places where subsystem purpose has become unclear.

The companion contract audit is
[`docs/architecture/api-surface-audit.md`](api-surface-audit.md). Use that audit
as the source of truth for public API narrowing: it identifies implementation
module leaks, umbrella exports, stale facades, and protocol-specific types that
cross crate boundaries in ways that make later refactors harder.

The cleanup goal is not to make Sequins protocol-generic. OpenTelemetry is the
product domain model: OTLP ingest, OTLP-native WAL alignment, and OTEL-shaped
relational tables are intentional architecture. Apache Flight is the separate
retrieval/query plane because query results are Arrow/DataFusion-native.

## Phase 0: Stabilize This PR

Subsystem quality work:

- Keep the CI fix for ephemeral OTLP test ports.
- Keep documentation scoped to sequencing and contract hardening.
- Do not propose non-OTEL ingest or generic telemetry support.
- Do not introduce a reusable production runtime crate in this cleanup plan.

API/interface hardening:

- Treat `api-surface-audit.md` as the contract-hardening companion to these
  subsystem notes.
- Use the audit to check whether a proposed cleanup removes implementation
  leaks or just moves them to a different umbrella crate.

Acceptance:

- Docs describe OTLP/OTEL alignment as product architecture, not accidental
  coupling.
- Flight remains the retrieval/query interface for Arrow/DataFusion results.
- Runtime extraction is out of scope except for a possible future local-only
  embedded setup helper below FFI/Linux app boundaries.

## Phase 1: Low-Risk API Surface Tightening

Subsystem quality work:

- Remove unused or misleading manifest dependencies, such as stale
  parser/query/type coupling.
- Delete inactive duplicate implementation files only after workspace imports
  have moved to the owning crates.
- Replace broad wildcard root exports with explicit exports where a crate has a
  clear public contract.

API/interface hardening:

- Remove or deprecate umbrella exports from `sequins-types`, `sequins-query`,
  and `sequins-storage` where they only forward implementation details.
- Migrate call sites so schema, parser, AST, Flight, and trait contracts are
  imported from their owning crates.
- Prefer direct purpose crates over compatibility paths documented in
  `api-surface-audit.md`.

Acceptance:

- `seql-parser` no longer depends on broad domain/type crates for parser-only
  behavior.
- Schema users import `sequins-arrow-schema` directly.
- Query users import parser, AST, compiler, Flight, and frame types from their
  owning crates rather than through `sequins-query`.
- Storage users do not reach through `sequins-storage::{hot_tier,cold_tier,wal,
  live_query}` unless they are explicit low-level tooling or tests.

## Phase 2: Query and Retrieval Boundary

Subsystem quality work:

- Narrow `sequins-query` to either a small query contract crate or eliminate it
  as a facade.
- Keep `seql-ast`, `seql-parser`, and `seql-substrait` focused on language
  syntax, parsing, validation, and compilation.
- Keep `sequins-flight` focused on Flight/Arrow retrieval framing, stream
  metadata, and IPC encoding.

API/interface hardening:

- Move reducer and query-result helpers out of language/compiler crates when
  they belong to Flight, view, or query-client behavior.
- Move Flight-coupled stream traits out of protocol-neutral trait crates.
- Make Flight IPC encoding failures observable through `Result`-returning APIs.

Acceptance:

- Compiler consumers do not inherit client-side Flight reduction behavior.
- View/client behavior can consume Flight frames without depending on parser or
  Substrait internals unless they explicitly compile text queries.
- Public query crate roots expose explicit facade types and functions, not
  wildcard module trees.

## Phase 3: OTEL Ingest and Schema Boundary

Subsystem quality work:

- Preserve OTLP/OTEL-native ingest contracts as intentional product
  architecture.
- Keep OTLP-to-Arrow conversion as the core ingest responsibility.
- Keep `sequins-arrow-schema` as the schema authority for OTEL-shaped tables.
- Keep DataFusion-specific UDF registration explicit.

API/interface hardening:

- Hide low-level protobuf plumbing, attribute codec builders, raw CBOR helpers,
  and converter implementation modules behind narrow OTEL-oriented conversion
  APIs.
- Migrate users away from schema re-exports through `sequins-types`.
- Avoid making query execution depend on broad OTLP internals just to access
  attribute overflow behavior.

Acceptance:

- OTLP conversion APIs are visible at the OTEL ingest boundary, while raw
  builders and generated protobuf plumbing are not broad workspace API.
- `sequins-arrow-schema` is the direct source for schema catalog and signal
  schema imports.
- Query execution imports attribute-overflow UDF registration from the codec
  layer that owns it, not from the OTLP conversion facade.

## Phase 4: Storage, WAL, and Index Boundary

Subsystem quality work:

- Fix P0 indexed-layout pruning correctness for equality predicates on fields
  that are not bloom-indexed.
- Keep storage optimized for OTEL-shaped data rather than general-purpose event
  storage.
- Preserve OTLP-native WAL alignment while narrowing the public WAL contract to
  durable append, replay, watermarks, and subscription behavior.

API/interface hardening:

- Define the query-facing `sequins-storage` facade around what DataFusion needs:
  hot table lookup, cold access, WAL watermarks/events, and live subscriptions.
- Hide hot-tier, cold-tier, WAL segment/writer, live-query, batch-chain,
  compaction, and indexed-layout implementation modules.
- Make structural internals such as `ColdTier` fields and `SeriesId`
  representation private unless there is a concrete external contract.

Acceptance:

- Indexed-layout pruning has regression tests covering equality predicates on
  indexed and non-indexed fields.
- DataFusion uses a storage query facade instead of reaching through storage
  implementation modules.
- Companion index writer/reader behavior is owned by the same encoded bundle
  contract.
- Storage docs and APIs retain OTEL-shaped table/WAL assumptions.

## Phase 5: App, FFI, and Local Embedded Boundary

Subsystem quality work:

- Keep FFI as the stable ABI surface.
- Treat CLI, Linux app, and examples as app surfaces, not reusable production
  server construction APIs.
- Keep remote/local behavior explicit so unsupported remote capabilities are
  not implied by stable ABI constructors.

API/interface hardening:

- Avoid exporting typed query/test stubs as product API.
- Split legacy/test ABI from stable ABI when Swift/header generation allows.
- Narrow incidental `pub` items in binary/app crates after library contracts are
  cleaned up.
- If shared setup code is needed, keep it local-only and below the FFI/Linux app
  layer rather than introducing a production runtime crate.

Acceptance:

- Swift/header-required symbols are identified before FFI removals.
- Typed query stubs are feature-gated or removed once current Swift/header
  generation no longer requires them.
- Local embedded setup is not presented as a general server/runtime API.

## Verification Gates

Run these checks after each migration phase:

- `cargo check --workspace --all-targets`
- `cargo test -p sequins-ffi`
- `swift test` in `apps/macos/SequinsData`
- `rg "sequins_types::arrow_schema|sequins_types::schema_catalog|sequins_types::SignalType"`
- `rg "sequins_storage::(hot_tier|cold_tier|wal|live_query)"`
- `rg "sequins_query::(flight|frame|schema|parser|ast)"`
- `cargo tree -i sequins-attribute-codec --workspace`

Documentation acceptance: every subsystem section should name its intended
public contract, identify current implementation leaks, and list the smallest
refactor that improves maintainability without weakening OTEL alignment.

## Assumptions

- This PR documents and sequences cleanup work; it does not perform every crate
  refactor.
- No compatibility window is required for Rust workspace crates; migrate
  downstream workspace users and remove old surfaces in the same cleanup.
- FFI is a generated ABI consumed by the macOS app, so removals there should be
  coordinated with Swift call-site/header generation updates.
- Binary crates are internal apps and should not drive library API design.
