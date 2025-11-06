# Sequins Documentation Sync Enforcer

**Purpose:** Keep the `plans/` directory documentation synchronized with actual code implementation.

**When to use:**
- Before committing code changes
- After implementing a feature that touches core architecture
- On-demand when documentation drift is suspected
- Monthly as part of maintenance

**Invocation:** `sequins-docs-sync` or manually when needed

---

## What This Skill Does

The `plans/` directory is the **source of truth** for Sequins architecture. This skill ensures that as code evolves, the planning documents stay accurate and valuable.

## Verification Checklist

### 1. **Architecture Documentation** (`plans/architecture.md`)

Check the Three-Trait Architecture section:
- [ ] `OtlpIngest`, `QueryApi`, `ManagementApi` traits match actual trait definitions in `crates/sequins-core/src/traits/`
- [ ] Method signatures documented match actual implementations
- [ ] Descriptions of what each trait does are accurate
- [ ] Diagrams showing trait implementations (Storage, QueryClient) are current

### 2. **Workspace Structure** (`plans/workspace-and-crates.md`)

Verify crate structure:
- [ ] All crates listed exist in `crates/` directory
- [ ] Crate dependencies match actual `Cargo.toml` files
- [ ] Trait implementation assignments (which crate implements what) are correct
- [ ] Generic patterns described match actual code (e.g., `OtlpServer<I: OtlpIngest>`)
- [ ] Directory structure diagrams reflect reality

### 3. **Data Models** (`plans/data-models.md`)

Check data type definitions:
- [ ] Rust structs in docs match actual definitions in `crates/sequins-core/src/models/`
- [ ] Field types and names are accurate
- [ ] ID types (ServiceId, TraceId, etc.) match actual newtypes
- [ ] Time types (Timestamp, Duration, TimeWindow) match implementations
- [ ] Relationships between types are correctly documented

### 4. **Database Schema** (`plans/database.md`)

Verify Parquet schemas:
- [ ] Arrow schema definitions match actual code in `crates/sequins-storage/src/`
- [ ] Column names and types are accurate
- [ ] Index strategies (bloom filters, RocksDB) match implementation
- [ ] Query patterns documented exist in code
- [ ] Compression settings are correct

### 5. **Storage Layer** (`plans/storage-layer.md` or similar)

Check storage implementation:
- [ ] Hot tier (Papaya) usage matches documentation
- [ ] Cold tier (Parquet + DataFusion) matches documentation
- [ ] Data lifecycle (ingestion → hot → cold → retention) is accurate
- [ ] Configuration options match actual config structs
- [ ] Performance characteristics are still valid

### 6. **OTLP Ingestion** (`plans/otlp-ingestion.md`)

Verify OTLP endpoint documentation:
- [ ] Supported protocols (gRPC, HTTP, HTTP+JSON) match server implementations
- [ ] Port numbers (4317, 4318) are correct
- [ ] Protobuf → internal model transformations are documented accurately
- [ ] Error handling patterns match code

### 7. **UI Design** (`plans/ui-design.md`)

Check UI component documentation:
- [ ] Component hierarchy matches actual GPUI components in `crates/sequins-app/src/ui/`
- [ ] View types and their responsibilities are accurate
- [ ] State management patterns match implementation
- [ ] Event handling patterns are correct

### 8. **Implementation Roadmap** (`plans/implementation-roadmap.md`)

Update progress tracking:
- [ ] Mark completed phases
- [ ] Update current phase status
- [ ] Note any deviations from original plan
- [ ] Add new tasks discovered during implementation
- [ ] Update estimates if needed

## How to Report Drift

When you find documentation that doesn't match code, report in this format:

```
📄 File: plans/architecture.md
🔍 Section: "QueryApi trait methods"
❌ Issue: Documentation shows `query_traces(query: TraceQuery)` but actual
         signature is `query_traces(&self, query: TraceQuery) -> Result<Vec<Trace>>`
✅ Fix: Update documentation to include `&self` and return type
```

## Fixing Documentation

For each drift found:

1. **Read the current code** - Understand what actually exists
2. **Update the plan document** - Make it match reality
3. **Preserve intent** - If the plan describes something not yet implemented, note it as "Planned" or "Future"
4. **Update diagrams** - If ASCII diagrams or architecture drawings are affected, update them
5. **Cross-reference** - Check if changes affect other plan documents and update those too

## Special Cases

### When Code is Wrong (not docs)

If documentation is correct and code has deviated from the plan:
- Flag it for discussion
- Determine if code should change or plan should update
- Document the decision
- Update accordingly

### When Both Need Updates

If both code and docs are outdated:
- Determine the correct design (may need user input)
- Update both code and docs to match
- Ensure consistency across all related areas

## Automation Hints

This skill can be partially automated:
- Extract trait definitions from code and compare to docs
- Parse Cargo.toml files and verify against workspace docs
- Use `cargo tree` to verify dependency graphs
- Compare struct definitions to documentation

## Success Criteria

Documentation is in sync when:
- ✅ Every trait method in code is documented accurately
- ✅ Crate structure matches workspace documentation
- ✅ Data models are identical between code and docs
- ✅ No "TODO" or "TBD" items that are actually implemented
- ✅ Implementation roadmap reflects current progress
- ✅ Architecture diagrams match actual component relationships

## Output Format

Provide a summary report:

```markdown
# Documentation Sync Report

## ✅ In Sync
- plans/technology-decisions.md (all accurate)
- plans/deployment.md (matches current implementation)

## ⚠️  Minor Drift
- plans/architecture.md
  - Line 45: Missing new `get_profile_by_id()` method in QueryApi
  - Fix: Add method signature to trait documentation

## ❌ Major Drift
- plans/data-models.md
  - Profile struct has 3 new fields not documented
  - Timestamp type changed from i64 to newtype wrapper
  - Fix: Update entire "Time Types" section

## 📊 Summary
- Total documents checked: 8
- Accurate: 5
- Minor drift: 2
- Major drift: 1
- Action items: 3 updates needed
```

## After Fixing

Once you've updated documentation:
- Run `cargo test --doc` to verify any code examples in docs still work
- Check for broken cross-references between documents
- Update the INDEX.md if document structure changed
- Commit docs with clear message: "docs: sync plans/ with implementation"

---

**Remember:** The `plans/` directory is living documentation. It's normal for drift to occur during rapid development. The goal is to catch it regularly and keep it useful as architectural reference.
