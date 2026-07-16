# Contracts and Schemas

Crates covered: `sequins-types`, `sequins-traits`, `sequins-arrow-schema`, and
`sequins-flight`.

## Intended Purpose

The foundation crates should be narrow:

- `sequins-types`: passive domain models and value types.
- `sequins-traits`: protocol-neutral shared traits and small utility types.
- `sequins-arrow-schema`: Arrow schema definitions and logical schema catalog.
- `sequins-flight`: Flight/query stream wire metadata and encoding helpers.

Intended public contracts:

- `sequins-types`: passive domain models and value types without schema or
  protocol umbrella exports.
- `sequins-traits`: protocol-neutral shared traits and small utility types.
- `sequins-arrow-schema`: canonical OTEL-shaped Arrow table schemas and signal
  schema catalog.
- `sequins-flight`: Flight/query stream metadata, builders, and IPC helpers for
  Arrow/DataFusion query results.

OpenTelemetry remains the domain model for ingest and table shape. These
contracts should avoid accidental umbrella dependencies, but they should not
weaken OTLP ingest or OTEL-shaped schema alignment.

## Current Weaknesses

### `sequins-types` Is Both Domain Crate and Compatibility Facade

`sequins-types` re-exports `sequins-arrow-schema` and `sequins-traits`.

Why it matters:

- Downstream code can depend on the broadest crate and accidentally pull schema,
  trait, Arrow, and protocol concerns through the model layer.
- The crate manifest contains dependencies that appear unused after these
  responsibilities moved elsewhere.

Direction:

- Keep passive domain models in `sequins-types`.
- Migrate callers to import schemas from `sequins-arrow-schema` and traits from
  `sequins-traits` or the future query protocol crate.
- Keep temporary compatibility exports only behind an explicit module or feature.

Priority: P1.

### Time Value Types Depend on Wall Clock

`TimeRange` and `TimeWindow` constructors call `Timestamp::now()` internally in
some paths.

Why it matters:

- Pure value operations become nondeterministic.
- Tests and replay logic must know which APIs secretly read the system clock.

Direction:

- Make wall-clock constructors explicit, for example `last_minutes_from(now,
  minutes)`.
- Keep convenience helpers with names that clearly signal system time use.

Priority: P1.

### Metric Identity Has Competing Definitions

`MetricId::from_name_and_service` and `MetricId::from_fields` coexist. Health
metric generation still uses legacy identity with placeholder resource/scope
values.

Why it matters:

- Generated and ingested metrics may not share stable identity semantics.
- Resource/scope identity becomes implicit and lossy.

Direction:

- Introduce a `MetricIdentity` value object.
- Require generated and ingested metric paths to use the same identity inputs.

Priority: P1.

### Behavior Modules Live in the Model Crate

Health metric generation and metric grouping detection live under
`sequins-types`.

Why it matters:

- Domain policy and generated telemetry behavior evolve differently from passive
  model structs.
- UI/query/storage consumers may want those policies without depending on all
  models through one crate.

Direction:

- Keep structs, enums, and IDs in `sequins-types`.
- Move generation/grouping policy to storage, view, or a dedicated model-policy
  crate once call sites are clear.

Priority: P2.

### `sequins-traits` Mixes Protocol-Neutral and Protocol-Specific Contracts

`sequins-traits` exposes `arrow_flight::FlightData` through `SeqlStream` and OTLP
protobuf request/response types through `OtlpIngest`.

Why it matters:

- A supposedly light contract crate pulls Flight retrieval and OTLP ingest
  surfaces even for consumers that only need management or time utilities.
- The issue is the umbrella location, not the OTLP ingest contract itself.

Direction:

- Move query transport traits and stream types to `sequins-flight` or a dedicated
  query protocol crate.
- Move OTLP ingest traits behind an explicit OTEL ingest contract or feature.
- Leave `sequins-traits` with protocol-neutral types such as `Duration`,
  `NowTime`, and management contracts.

Priority: P1.

### `sequins-traits::Error` Is Too Generic

The top-level error has only generic string-style behavior.

Why it matters:

- API consumers cannot distinguish validation, unavailable, retryable, or
  internal failures.

Direction:

- Add stable variants where shared traits require shared error semantics.
- Prefer domain-specific errors for richer subsystem APIs.

Priority: P2.

### Vortex Leaks Into Arrow Schema

`sequins-arrow-schema` depends on Vortex and contains Vortex extension dtypes and
encoding hints.

Why it matters:

- A crate named and documented as Arrow schema is also a Vortex policy crate.
- Non-Vortex schema consumers inherit storage-format concepts.

Direction:

- Gate Vortex-specific dtypes/hints behind a feature or move them to a
  `sequins-vortex-schema` crate.
- Keep logical schema and Arrow field construction separate from Vortex encoding
  strategy.

Priority: P1.

### Column Naming Policy Is Inconsistent

Custom attribute names use an `attr_` prefix, while built-in promoted attributes
use names like `http_request_method` and `service_name_attr`.

Why it matters:

- Attribute promotion, companion indexing, parser field recognition, and view
  projection must each learn naming exceptions.

Direction:

- Define one `ColumnNamePolicy`.
- Apply it to built-in and custom promoted columns.
- Test that schema, codec, index builders, and query field resolution agree.

Priority: P1.

### `SignalType` Mixes Logical Identity and Hot-Tier Indexing

`sequins-arrow-schema::SignalType` owns schema dispatch and hot-tier array index
constants.

Why it matters:

- Logical schema identity knows about hot-tier storage layout.
- Hot-tier implementation choices become schema API surface.

Direction:

- Keep logical signal identity and schema dispatch in schema/catalog code.
- Move hot-tier array indexing into hot-tier internals.

Priority: P2.

### `sequins-flight` Mixes Wire Metadata and Frame Models

Flight metadata repeats schema frame concepts and depends on AST schema types and
traits. Its public API wildcard-exports all helpers.

Why it matters:

- Wire format, client reducer frames, and query schema metadata are not clearly
  owned.
- All helper functions become stable public surface.

Direction:

- Make one canonical query stream metadata model.
- Explicitly re-export stable wire types and builders.
- Keep lower-level helpers internal or under named modules.

Priority: P1.

### Flight Encoding Silently Swallows Errors

`batch_to_ipc` ignores Arrow IPC writer construction/write/finish failures and
can return empty bytes.

Why it matters:

- Encoding failure can look like a valid empty payload.
- Downstream reducers and views cannot distinguish corruption from no data.

Direction:

- Return `Result<Bytes, ArrowError>` from IPC helpers.
- Make FlightData builder APIs return `Result`.
- Propagate encoding failures through query execution errors.

Priority: P1.

## Boundary Decisions

- Do not join `sequins-types` and `sequins-traits`.
- Move query transport out of `sequins-traits`.
- Keep Arrow schemas separate from domain models.
- Gate or split Vortex schema policy.
- Treat `sequins-flight` as the natural owner for query stream wire contracts.

## Acceptance Checks

- `sequins-types` builds without Arrow schema or trait dependencies except
  compatibility features.
- `sequins-traits` builds without Arrow Flight and OTLP proto unless explicit
  features require them.
- One warning code representation is used on both internal and wire metadata.
- Flight IPC encoding failures are observable in tests.
