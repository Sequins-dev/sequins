# Sequins Development Skills

This directory contains custom Claude Code skills specifically designed for Sequins development. These skills help maintain code quality, architectural consistency, and development velocity.

## Skill Categories

### 📚 Documentation Maintenance

**sequins-docs-sync** - Keep `plans/` directory synchronized with code
- **When to use:** Before commits, after implementing features
- **What it does:** Compares planning docs against actual code, reports drift
- **Invocation:** Manual or checkpoint-based (before commits)

**sequins-doc-comments** - Ensure all public APIs have proper doc comments
- **When to use:** Before commits, during code review
- **What it does:** Scans for missing doc comments, suggests templates
- **Invocation:** Manual or checkpoint-based

### 🏗️ Architecture Enforcement

**sequins-crate-boundaries** - Enforce clean crate architecture
- **When to use:** Before commits, when editing Cargo.toml
- **What it does:** Verifies crate dependencies follow architectural rules
- **Invocation:** Checkpoint-based, proactive on Cargo.toml edits

**sequins-zero-cost-abstractions** - Check for trait objects vs generics
- **When to use:** After implementing features, during code review
- **What it does:** Finds `dyn Trait` usage, suggests generic alternatives
- **Invocation:** Checkpoint-based (after features)

**sequins-storage-patterns** - Ensure consistent hot→cold tier query pattern
- **When to use:** After implementing query methods
- **What it does:** Verifies all queries follow: hot→cold→merge→limit pattern
- **Invocation:** Checkpoint-based, manual

### 🛠️ Development Assistance

**sequins-trait-coordinator** - Coordinate trait/type changes across codebase
- **When to use:** When adding/modifying trait methods, changing type signatures
- **What it does:** Ensures all implementations, usages, tests, and docs are updated
- **Invocation:** Manual when making structural changes

**sequins-lldb-debugger** - Debug segfaults, deadlocks, and hangs with LLDB
- **When to use:** When app crashes or becomes unresponsive
- **What it does:** Provides systematic debugging workflows for common failure modes
- **Invocation:** Manual when issues occur

### 🧭 Pattern Guidance (Framework Skills)

**sequins-architecture-guide** - Deep dive on three-trait architecture
- **When to use:** When editing core traits, implementing new features
- **What it does:** Provides comprehensive guidance on OtlpIngest/QueryApi/ManagementApi patterns
- **Invocation:** Proactive when editing `sequins-core/src/traits/`, manual

**sequins-storage-guide** - Storage implementation patterns
- **When to use:** When editing storage layer, optimizing queries
- **What it does:** Detailed patterns for hot tier (Papaya) and cold tier (Parquet+DataFusion)
- **Invocation:** Proactive when editing `sequins-storage/`, manual

**sequins-lock-free-guide** - Correct Papaya HashMap usage
- **When to use:** When working with hot tier, debugging hangs
- **What it does:** Explains lock-free concurrency patterns, guard lifecycle, common mistakes
- **Invocation:** Proactive when working with hot tier code, manual

## Invocation Methods

### Manual Invocation
Call any skill explicitly:
```bash
# In Claude Code
Use the sequins-docs-sync skill to check documentation
```

### Proactive Invocation (File-Based Triggers)
Some skills auto-load when editing specific files:
- Editing `sequins-core/src/traits/` → `sequins-architecture-guide`
- Editing `sequins-storage/` → `sequins-storage-guide`
- Working with hot tier → `sequins-lock-free-guide`
- Editing `Cargo.toml` → `sequins-crate-boundaries`

### Checkpoint-Based Invocation
Some skills run at specific checkpoints:
- Before commits: `sequins-docs-sync`, `sequins-doc-comments`, `sequins-crate-boundaries`
- After features: `sequins-zero-cost-abstractions`, `sequins-storage-patterns`

## Skill Relationships

Skills often work together:

```
sequins-trait-coordinator
    ├─> sequins-architecture-guide  (provides context)
    ├─> sequins-zero-cost-abstractions  (verifies generics used)
    ├─> sequins-doc-comments  (ensures docs updated)
    └─> sequins-docs-sync  (updates planning docs)

sequins-storage-guide
    ├─> sequins-lock-free-guide  (Papaya patterns)
    ├─> sequins-storage-patterns  (query pattern linting)
    └─> sequins-zero-cost-abstractions  (trait usage)

sequins-docs-sync
    └─> All other skills (keeps plans in sync with code)
```

## Usage Tips

### For New Features
1. Start with `sequins-architecture-guide` to understand patterns
2. Implement feature using guidance skills as needed
3. Use `sequins-trait-coordinator` if adding trait methods
4. Run `sequins-storage-patterns` for query methods
5. Run `sequins-zero-cost-abstractions` to check generics
6. Before commit:
   - `sequins-doc-comments`
   - `sequins-crate-boundaries`
   - `sequins-docs-sync`

### For Bug Fixes
1. If crash/hang: `sequins-lldb-debugger`
2. If hot tier issue: `sequins-lock-free-guide`
3. If architecture issue: `sequins-architecture-guide`
4. Before commit: checkpoint skills

### For Refactoring
1. `sequins-trait-coordinator` for structural changes
2. `sequins-zero-cost-abstractions` to maintain patterns
3. `sequins-crate-boundaries` to verify dependencies
4. `sequins-docs-sync` to update documentation

## Maintenance

These skills should be updated when:
- Architecture patterns change (update guides)
- New enforcement rules added (update checkers)
- Planning doc structure changes (update docs-sync)
- New common mistakes identified (add to guides)

## Quick Reference

| Task | Skill to Use |
|------|-------------|
| Adding trait method | `sequins-trait-coordinator` |
| Implementing query | `sequins-storage-guide` + `sequins-storage-patterns` |
| Using Papaya | `sequins-lock-free-guide` |
| App crashes | `sequins-lldb-debugger` |
| Check architecture | `sequins-crate-boundaries` + `sequins-zero-cost-abstractions` |
| Before commit | All checkpoint skills |
| Update docs | `sequins-docs-sync` + `sequins-doc-comments` |
| Understand patterns | `sequins-architecture-guide` |

## Contributing

When adding new skills:
1. Follow naming pattern: `sequins-<purpose>`
2. Include clear "When to use" section
3. Add to appropriate category above
4. Update this README
5. Consider invocation triggers (manual/proactive/checkpoint)
