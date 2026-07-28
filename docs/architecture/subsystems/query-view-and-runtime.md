# Query Execution, View, and Local Apps

Crates/apps covered: `sequins-datafusion-backend`, `sequins-view`,
`sequins-client`, `sequins-server`, `sequins-cli`, `sequins-ffi`, `apps/linux`,
and `example`.

## Intended Purpose

This layer should separate:

- Query execution over storage.
- Flight query stream wire protocol and client transport.
- View-model updates from query stream frames.
- Local embedded setup for app/FFI surfaces.
- CLI, FFI, and Linux UI adapters.

Intended public contracts:

- `sequins-datafusion-backend`: query execution over a narrow storage query
  input.
- `sequins-flight`: Flight/Arrow retrieval framing for query results.
- `sequins-view`: transport-neutral view strategies and deltas, with Flight
  decoding in an adapter.
- `sequins-client`: remote query and management clients, with text compilation
  optional over time.
- `sequins-ffi`: stable C ABI; typed test/header stubs are not product API.

## Current Weaknesses

### DataFusion Execution Crosses Too Many Boundaries

`sequins-datafusion-backend/src/execution.rs` handles plan decoding, snapshot
execution, live append/replace execution, WAL policy, filtering, expiry, and
Flight framing.

Why it matters:

- Different policies change for different reasons but share one large module.
- Live query behavior is hard to reason about independently from DataFusion plan
  execution or wire framing.

Direction:

- Split into modules for plan decoding, snapshot execution, live append, live
  replace, filtering, and framing.
- Keep `DataFusionBackend` as the facade that wires those pieces together.

Priority: P1.

### Backend Signal/Table Names Are Stringly and Leaky

Execution maps strings back to signals, including fallback behavior that routes
unknown names to spans. Registration uses physical names such as
`profile_stacks`, while query/view code may use aliases such as `stacks`.

Why it matters:

- Unknown or renamed signals can subscribe to the wrong table.
- Compiler, backend, and view aliases drift independently.

Direction:

- Use the shared signal catalog for query aliases, physical table names, and
  hot-tier signal mapping.
- Return structured errors for unknown names.

Priority: P1.

### Registration Mixes Catalog, Compatibility Policy, and Logging

DataFusion registration embeds schema functions, storage paths, schema
compatibility fallback, and direct `eprintln!` output.

Why it matters:

- Table identity and operational fallback policy are coupled.
- Schema evolution behavior is hard to test or observe.

Direction:

- Move table catalog data to the shared catalog.
- Isolate cold schema compatibility into a helper that emits structured
  `tracing` warnings.

Priority: P2.

### Arrow-to-Query Schema Conversion Loses Semantics

Backend schema conversion marks every column as `Field` and maps unknown Arrow
types to string-like query schema types.

Why it matters:

- Clients cannot distinguish group keys, row IDs, aggregates, or unsupported
  complex values reliably.

Direction:

- Return `Result` for unsupported types.
- Let compiler/plan metadata provide column roles instead of inferring all roles
  from Arrow alone.

Priority: P2.

### View Strategies Are Coupled to Flight/Arrow Transport

`sequins-view` strategy APIs accept `SeqlStream`, decode Flight metadata, and
read IPC batches directly.

Why it matters:

- View logic cannot be reused or tested independently from Flight.
- The crate inherits broad query/transport dependencies.

Direction:

- Introduce a transport-neutral `ViewInputFrame`.
- Keep pure view strategy state over typed batches/deltas.
- Put Flight decoding in an adapter module.

Priority: P1.

### Expiry Semantics Are Duplicated and Lossy

Backend emits expiry metadata with one row ID, while table strategy hardcodes an
expired count of one.

Why it matters:

- Batch or range expiry can be represented incorrectly to frontends.

Direction:

- Define a shared expiry contract: row range, row count, or predicate.
- Make backend and view use the same metadata fields.

Priority: P1.

### Flamegraph View Hardcodes Query Shape and Drops Bad Rows

Flamegraph strategy only recognizes `stacks` and `frames` aliases and silently
continues on missing/null/type-mismatched required columns.

Why it matters:

- Compiler alias changes or schema evolution can produce empty views without a
  clear error.

Direction:

- Supply aliases from query metadata or strategy configuration.
- Add typed row extractors that emit warnings/errors for required-column
  failures.

Priority: P1.

### Client Mixes Compilation and Flight Transport

`RemoteClient` stores a DataFusion `SessionContext`, compiles SeQL, and owns
Flight `GetFlightInfo`/`DoGet` calls.

Why it matters:

- Thin remote clients inherit heavy compiler/DataFusion dependencies.
- Callers that already have plan bytes cannot depend only on transport.

Direction:

- Split plan transport from optional query compilation.
- Keep a convenience `RemoteQueryClient` over a lower-level `FlightPlanClient`.

Priority: P1.

### Server Lifecycle Is Shaped by Embedding Needs

`OtlpServer` combines service construction, readiness signaling, gRPC handlers,
HTTP handlers, content negotiation, limits, and tests in one module. Lifecycle
APIs accept `std::sync::mpsc::Sender` readiness channels for embedders.

Why it matters:

- Protocol changes risk lifecycle behavior.
- FFI/Linux embedding concerns leak into the server crate.

Direction:

- Split OTLP server modules into lifecycle, gRPC, HTTP, and codec pieces.
- Expose lower-level build/bind APIs returning listeners/services.
- Let app/adapter setup own readiness signaling.

Priority: P1.

### Management Protocol Has No Client Counterpart

Server exposes REST management routes, but FFI remote management reports
unsupported.

Why it matters:

- Remote and local data sources have different capabilities behind similar
  shapes.

Direction:

- Add a management client to `sequins-client` or define a small management
  protocol crate shared by server/client/FFI.

Priority: P2.

### CLI, FFI, and Linux Duplicate Local Embedded Setup

CLI daemon setup, FFI `DataSourceImpl`, and Linux `LocalServer` each own storage,
backend, and server assembly.

Why it matters:

- Local embedded behavior has multiple assembly roots.
- Config, port, readiness, and local/remote behavior can drift.

Direction:

- Do not introduce a reusable production runtime crate as part of this cleanup.
- If duplication becomes a blocking issue, create a local-only embedded setup
  helper below the FFI/Linux app boundary.
- Keep CLI, FFI, and Linux responsible for their adapter-specific config,
  pointer/string/callback conversion, and UI integration.

Priority: P1.

### FFI Remote Mode Is Structurally Present but Unsupported

FFI stores a remote client, but snapshot/live query and management return null or
unsupported for remote mode.

Why it matters:

- The API suggests remote support that is not functionally present.

Direction:

- Either implement remote dispatch through query and management clients, or
  remove/feature-gate remote constructors until supported.

Priority: P1.

### Linux UI Owns Domain Analysis

Linux components contain health scoring, trace grouping/tree building, Arrow IPC
decoding, and broad root app state.

Why it matters:

- UI components become domain model and local service adapters.
- The logic cannot be reused by FFI or other frontends.

Direction:

- Move health analysis, trace/log row projection, and typed stream adapters into
  `sequins-view` or a new app-model crate.
- Split the root app into environment/profile management, OTLP controls, service
  sidebar, and tab host components.

Priority: P1/P2.

### Example Duplicates Protocol Constants

The example hardcodes endpoints, OTLP routes, and profile encoding behavior.

Why it matters:

- Examples become stale when protocol paths or ports change.

Direction:

- Read endpoint from environment once.
- Share protocol constants.
- Consider moving pprof-to-OTLP demo conversion into a helper if profiles remain
  a supported demo path.

Priority: P1.

## Boundary Decisions

- Backend should not join view or client; fix protocol coupling instead.
- Client transport should be usable without compiler dependencies.
- Server should expose services; apps/adapters should own embedding lifecycle.
- Do not add a production runtime crate in this cleanup. A future shared helper
  may exist only for local embedded setup below app/FFI boundaries.
- UI components should render and adapt, not own domain analysis.

## Acceptance Checks

- Unknown live-query table names produce errors, not span fallback.
- View strategy tests can run over transport-neutral frames.
- Expiry metadata round-trips from backend to view with row count/range
  preserved.
- Any shared CLI/FFI/Linux setup remains local-only and does not become a public
  production runtime API.
- FFI remote mode is either implemented or not exposed as supported.
