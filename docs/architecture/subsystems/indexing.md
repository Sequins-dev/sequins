# Indexing and Profile Support

Crates covered: `sequins-pprof`, `sequins-companion-index`,
`sequins-series-index`, and `sequins-vortex-indexed-layout`.

## Intended Purpose

Indexing crates should make pruning and lookup cheaper without changing query
semantics. Profile parsing should isolate pprof dependency cost while exposing
reusable parsed and resolved profile representations.

Intended public contracts:

- `sequins-companion-index`: companion index builders, encoded bundle format,
  and field-aware lookup helpers.
- `sequins-vortex-indexed-layout`: Vortex registration/strategy integration
  that delegates encoded companion-index details to the companion index crate.
- `sequins-series-index`: in-memory series lookup and typed series IDs, with
  persistence hidden behind a store/feature boundary.
- `sequins-pprof`: pprof parse, symbol resolution, and sample materialization
  as separate entrypoints.

## Current Weaknesses

### Vortex Indexed Layout Can Incorrectly Prune Data

The indexed-layout reader extracts all string equality predicates and checks
them against bloom filters. Fields not present in bloom filters return false,
which can incorrectly skip files for predicates such as `service_name = ...` or
other low-cardinality fields.

Why it matters:

- This is a correctness bug. Query results can be incomplete.

Direction:

- Persist/read companion index capability metadata.
- Only apply bloom pruning to fields known to have bloom indexes.
- Delegate other predicates to Vortex or Tantivy pruning where supported.
- Add regression tests with predicates on indexed and non-indexed fields.

Priority: P0.

### Companion Index Format Ownership Is Split

`sequins-companion-index` defines `CompanionIndexBytes`, while
`sequins-vortex-indexed-layout` separately bundles, serializes, deserializes, and
opens Tantivy files.

Why it matters:

- Format changes require synchronized edits across crates.
- Reader behavior depends on private assumptions about writer output.

Direction:

- Let `sequins-companion-index` own the serialized bundle format.
- Expose APIs like `CompanionIndexBundle::encode`, `decode`, and
  field-aware search helpers.
- Keep Vortex layout focused on integrating companion indexes with Vortex
  pruning.

Priority: P1.

### Tantivy Reader Reconstructs Indexes in Tempdirs

Query-time Tantivy access writes all files to a tempdir and opens a Tantivy index
from disk.

Why it matters:

- Pruning can become filesystem-heavy and repeated across splits.
- The Vortex reader owns low-level companion index reconstruction.

Direction:

- Cache decoded companion indexes per segment.
- Prefer a RAM-directory reconstruction API owned by `sequins-companion-index`.

Priority: P1.

### Span Tantivy Indexes Are Not Used for Span Equality Pruning

Span index writing emits Tantivy files, but reader-side Tantivy lookup only
searches a `body` field.

Why it matters:

- Storage and write complexity may not translate into query pruning benefits.

Direction:

- Either teach the reader to query field-specific Tantivy equality/range
  predicates, or stop emitting unused span Tantivy segments until supported.

Priority: P1.

### Companion Index Generic Strategy Lives in Span Module

Log indexing imports a strategy enum from `span_index`.

Why it matters:

- A generic companion-index concept is hidden in a product-specific module.

Direction:

- Move strategy and metadata definitions to neutral modules such as `strategy`
  and `metadata`.
- Keep signal-specific builders focused on extracting fields.

Priority: P1.

### Log and Span Builders Duplicate Field Extraction Rules

Log and span index builders duplicate Arrow field extraction and `attr_` naming
logic.

Why it matters:

- Schema convention changes require edits in multiple builders.

Direction:

- Add a shared `BatchFieldAccess` or `PromotedColumnSet` helper tied to the
  shared column naming policy.

Priority: P2.

### Series Index Mixes Core Index and Object-Store Persistence

`sequins-series-index` is otherwise light, but owns async object-store
`persist/load` methods.

Why it matters:

- In-memory users inherit object-store and async dependencies.

Direction:

- Split persistence into `SeriesIndexStore` or feature-gate object-store support.
- Keep the core index as an in-memory, dependency-light structure.

Priority: P1.

### Series Index Load Treats All Get Errors as Missing

Any object-store `get` error returns a new empty index.

Why it matters:

- Permission, transient network, and backend failures can silently reset the
  index.

Direction:

- Distinguish not-found from other object-store errors.
- Preserve failure for non-not-found errors.

Priority: P1.

### Pprof Parser Has Too Many Layers in One API

`sequins-pprof` parses pprof, resolves symbols, and materializes Sequins
`ProfileSample` values with placeholder storage-normalization IDs.

Why it matters:

- Callers cannot reuse parsed/resolved pprof structures without accepting
  storage-specific sentinel values.

Direction:

- Split into `parse_profile`, `resolve_symbols`, and `to_profile_samples`.
- Keep pprof dependency isolation, but expose lower-level parsed/resolved data.

Priority: P1.

## Boundary Decisions

- Keep companion-index separate from Vortex layout, but move byte-format and
  Tantivy bundle operations into companion-index.
- Keep series-index separate from cold-tier, with optional persistence.
- Keep pprof separate as a dependency isolation crate, but split parser stages.

## Acceptance Checks

- Indexed-layout tests prove non-indexed predicates do not prune files.
- Companion index writer and reader use the same encoded bundle API.
- Series index load distinguishes not-found from other object-store failures.
- Pprof parsing can be used without immediately producing storage-bound sample
  IDs.
