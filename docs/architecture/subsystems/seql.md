# SeQL Subsystem

Crates covered: `seql-ast`, `seql-parser`, `seql-substrait`, and
`sequins-query`.

## Intended Purpose

The SeQL subsystem should have clear layers:

- `seql-ast`: pure syntax and semantic query model.
- `seql-parser`: text-to-AST parsing only.
- `seql-substrait`: AST-to-Substrait/DataFusion planning.
- `sequins-query`: either a deliberate compatibility facade or removed in favor
  of direct dependencies on the purpose crates.

Intended public contracts:

- Parser callers should use `seql-parser` for text parsing and parse errors.
- AST callers should use `seql-ast` for syntax and semantic query structures.
- Compiler callers should use `seql-substrait` for supported plan compilation
  entrypoints.
- Retrieval/client callers should use `sequins-flight` or client/view crates
  for Flight frames and result reduction, not language crates.

## Current Weaknesses

### Duplicated Signal Metadata

Signal names, singular aliases, table names, time columns, join keys, and
correlation edges are duplicated across AST, parser, compiler, and backend code.

Evidence:

- `seql-ast/src/ast.rs` defines `Signal` and `Signal::join_key`.
- `seql-ast/src/correlation.rs` owns separate correlation rules.
- `seql-parser/src/stages.rs` has hardcoded plural/singular signal parsing and
  error text.
- `seql-substrait/src/compiler.rs` has separate signal-to-name, signal-to-table,
  join-key, and time-column mappings.

Why it matters:

- Adding a signal requires coordinated edits in multiple crates.
- Query names and physical table names can drift.
- Unsupported or unknown names can silently route to the wrong signal.

Direction:

- Move signal metadata into the shared catalog described in the roadmap.
- Parser, AST helpers, compiler, and backend should all delegate to that catalog.

Priority: P1.

### Parser Pulls Runtime Types for Clock Access

`seql-parser` depends on `sequins-types` mostly to access `NowTime` /
`SystemNowTime` for relative time parsing.

Why it matters:

- A text parser inherits a broad domain/runtime dependency.
- `today` and `yesterday` parsing is nondeterministic unless callers can inject a
  clock.

Direction:

- Add parser options with an explicit `now_ns` or `Timestamp`.
- Keep the default parser convenience API, but make the deterministic API the
  lower-level primitive.
- Remove the `sequins-types` dependency from parser after migration.

Priority: P1.

### Parser Performs Product Policy Desugaring

`parse_id_lookup` turns `signal(id)` into a query with a 24-hour window and
`Limit(1)`.

Why it matters:

- Syntax parsing owns product policy.
- The default time window is hidden from callers and hard to evolve.

Direction:

- Parse ID lookup as an explicit AST form or normalize it in a separate semantic
  pass.
- Make the default window a named policy owned by query semantics, not the
  grammar.

Priority: P2.

### `seql-ast` Owns Result/UI Schema Concepts

`seql-ast/src/schema.rs` contains `ResponseShape`, `ColumnDef`, `DataType`, and
`infer_shape`.

Why it matters:

- A pure AST crate carries response and rendering hints.
- Query result metadata becomes mixed with syntax types.

Direction:

- Move response metadata into a query protocol or result metadata module.
- Keep `seql-ast` focused on query syntax and semantic query structure.

Priority: P2.

### `seql-substrait` Mixes Compiler and Client Reducer

`seql-substrait` exports compiler APIs and a Flight reducer. Its dependency
surface includes DataFusion/Substrait, Flight, async, CBOR, base64, and row
materialization helpers.

Why it matters:

- Compiler consumers inherit client-side stream reduction and serialization
  dependencies.
- Transport behavior is coupled to planning behavior.

Direction:

- Keep `seql-substrait` focused on AST-to-plan compilation.
- Move reducer and Arrow-to-JSON conversion to `sequins-flight`,
  `sequins-view`, or a small response adapter crate.

Priority: P1.

### Compiler Public API Leaks DataFusion Internals

`seql-substrait` publicly re-exports stage translators such as `apply_filter`,
`apply_project`, `ast_expr_to_df_expr`, and `schema_context`.

Why it matters:

- Internal planning steps become semver surface.
- Callers can depend on partial compiler internals that may need to change
  together.

Direction:

- Publicly expose `compile`, `compile_ast`, and deliberately supported helper
  APIs only.
- Move stage translators to `pub(crate)` or behind test/support features.

Priority: P1.

### AST Semantics Exceed Compiler Support

Examples observed:

- `UniqueStage.field` is ignored and whole-row distinct is applied.
- `Patterns` appears as a no-op in compiler stage handling.
- `Heatmap` and `Sample` are parseable but rejected later.
- Some aggregate modes are represented more richly in AST than in plan output.

Why it matters:

- Callers cannot know whether a parsed AST is executable.
- Unsupported semantics fail late or degrade silently.

Direction:

- Add a semantic validation/capability pass before planning.
- Return structured unsupported-stage errors before plan generation.

Priority: P1.

### `sequins-query` Is a Halfway Facade

`sequins-query/src/lib.rs` re-exports `seql-*` crates, but the crate still
contains local parser, schema, AST, correlation, compiler, frame, and reducer
files.

Why it matters:

- Contributors can edit files that are no longer the active implementation.
- The crate depends on parser, DataFusion, Arrow, Flight, protobuf, async, and
  metadata dependencies even when consumers only need one layer.

Direction:

- Choose one of two end states:
  - keep `sequins-query` as a thin compatibility facade and delete stale local
    implementations; or
  - make it the real owner again and remove the split `seql-*` crates.
- Prefer the facade path because the split crates model clearer ownership.

Priority: P1.

## Boundary Decisions

- Keep parser separate from AST.
- Keep compiler separate from execution backend.
- Split reducer/client frame handling out of compiler.
- Treat `sequins-query` as temporary compatibility unless a concrete product
  reason requires a unified query crate.

## Acceptance Checks

- Adding a new signal requires editing the catalog and tests, not parser,
  compiler, backend, server, and view by hand.
- `seql-parser` has a deterministic parse API that does not depend on
  wall-clock access.
- Unsupported AST features fail in validation before Substrait generation.
- `sequins-query` has no stale duplicate implementation files.
