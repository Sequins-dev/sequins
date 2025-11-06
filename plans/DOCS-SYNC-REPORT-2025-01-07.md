# Documentation Synchronization Report
**Date:** 2025-01-07
**Status:** Documentation Audit Complete
**Action Required:** Update plan documents to reflect current implementation

---

## Executive Summary

Comprehensive audit of `plans/` documentation against actual codebase implementation reveals significant architectural evolution since original planning. The checkpoint document `CHECKPOINT-unified-datafusion-queries.md` captures recent decisions, but main planning documents need updates to reflect:

1. **Trait architecture evolution** - Traits moved from `sequins-storage` to `sequins-core`
2. **Storage implementation changes** - `TieredStorage` → `Storage` with hot/cold tier separation
3. **New infrastructure** - DataFusion integration, internal storage traits
4. **Undocumented crate** - `sequins-web` exists but not in any plan document
5. **Implementation progress** - Phase 1 substantially complete but roadmap not updated

**Overall Assessment:** 📊 **Moderate drift** requiring documentation updates, but architecture remains sound.

---

## ✅ In Sync

### 1. Core Data Models (`plans/data-models.md`) ✅ **ACCURATE**

**Status:** All documented models match implementation

**Evidence:**
- ✅ 56 tests passing in `sequins-core`
- ✅ Time types: `Timestamp`, `Duration`, `TimeWindow` implemented
- ✅ ID types: `TraceId`, `SpanId`, `LogId`, `MetricId`, `ProfileId` implemented
- ✅ Trace models: `Trace`, `Span`, `AttributeValue` implemented
- ✅ Log models: `LogEntry`, `LogSeverity` implemented
- ✅ Metric models: `Metric`, `MetricDataPoint`, `HistogramDataPoint` implemented
- ✅ Profile models: `Profile`, `ProfileType` implemented

**Files verified:**
- `crates/sequins-core/src/models/time/` - Timestamp, Duration, TimeWindow
- `crates/sequins-core/src/models/ids.rs` - All ID types
- `crates/sequins-core/src/models/traces/` - Trace, Span, AttributeValue
- `crates/sequins-core/src/models/logs/` - LogEntry, LogSeverity
- `crates/sequins-core/src/models/metrics/` - Metric types
- `crates/sequins-core/src/models/profiles/` - Profile types

**Action:** ✅ None required

---

### 2. Technology Decisions (`plans/technology-decisions.md`) ✅ **ACCURATE**

**Status:** All technology choices remain valid

**Evidence:**
- ✅ DataFusion + Parquet chosen and integrated
- ✅ Papaya HashMap for hot tier (validated in checkpoint)
- ✅ object_store for cold tier storage
- ✅ OpenTelemetry protobuf integration

**Action:** ✅ None required

---

### 3. Business Model & Deployment (`plans/deployment.md`) ✅ **ACCURATE**

**Status:** Free local / paid enterprise model documented correctly

**Evidence:**
- ✅ Architecture supports both deployment modes
- ✅ Localhost-only OTLP for free tier concept remains valid
- ✅ Enterprise daemon architecture documented correctly

**Action:** ✅ None required

---

## ⚠️ Minor Drift

### 1. Architecture Documentation (`plans/architecture.md`)

**Issue:** Missing documentation of internal storage traits and actual trait method signatures

**Current State:**
- ❌ Documents conceptual three-trait architecture but no method signatures
- ❌ Doesn't mention `StorageRead`, `StorageWrite`, `TierMetadata` traits
- ❌ Doesn't explain difference between external API traits vs internal storage traits

**Reality:**
```rust
// External API traits (in sequins-core/src/traits/)
pub trait OtlpIngest: Send + Sync {
    async fn ingest_spans(&self, spans: Vec<Span>) -> Result<()>;
    async fn ingest_logs(&self, logs: Vec<LogEntry>) -> Result<()>;
    async fn ingest_metrics(&self, metrics: Vec<Metric>) -> Result<()>;
    async fn ingest_profiles(&self, profiles: Vec<Profile>) -> Result<()>;
}

pub trait QueryApi: Send + Sync {
    async fn get_services(&self) -> Result<Vec<Service>>;
    async fn query_traces(&self, query: TraceQuery) -> Result<Vec<QueryTrace>>;
    async fn get_spans(&self, trace_id: TraceId) -> Result<Vec<Span>>;
    async fn get_span(&self, trace_id: TraceId, span_id: SpanId) -> Result<Option<Span>>;
    async fn query_logs(&self, query: LogQuery) -> Result<Vec<LogEntry>>;
    async fn get_log(&self, log_id: LogId) -> Result<Option<LogEntry>>;
    async fn query_metrics(&self, query: MetricQuery) -> Result<Vec<Metric>>;
    async fn get_metric(&self, metric_id: MetricId) -> Result<Option<Metric>>;
    async fn get_profiles(&self, query: ProfileQuery) -> Result<Vec<Profile>>;
    async fn get_profile(&self, profile_id: ProfileId) -> Result<Option<Profile>>;
}

pub trait ManagementApi: Send + Sync {
    async fn run_retention_cleanup(&self) -> Result<usize>;
    async fn update_retention_policy(&self, policy: RetentionPolicy) -> Result<()>;
    async fn get_retention_policy(&self) -> Result<RetentionPolicy>;
    async fn run_maintenance(&self) -> Result<MaintenanceStats>;
    async fn get_storage_stats(&self) -> Result<StorageStats>;
}

// Internal storage traits (NEW - for hot/cold tier abstraction)
pub trait StorageRead: Send + Sync {
    async fn query_traces(&self, query: &TraceQuery) -> Result<Vec<QueryTrace>>;
    async fn query_logs(&self, query: &LogQuery) -> Result<Vec<LogEntry>>;
    async fn query_metrics(&self, query: &MetricQuery) -> Result<Vec<Metric>>;
    async fn query_profiles(&self, query: &ProfileQuery) -> Result<Vec<Profile>>;
    async fn get_spans(&self, trace_id: TraceId) -> Result<Vec<Span>>;
    async fn get_span(&self, trace_id: TraceId, span_id: SpanId) -> Result<Option<Span>>;
    async fn get_log(&self, log_id: LogId) -> Result<Option<LogEntry>>;
    async fn get_metric(&self, metric_id: MetricId) -> Result<Option<Metric>>;
    async fn get_profile(&self, profile_id: ProfileId) -> Result<Option<Profile>>;
}

pub trait StorageWrite: Send + Sync {
    async fn write_spans(&self, spans: Vec<Span>) -> Result<()>;
    async fn write_logs(&self, logs: Vec<LogEntry>) -> Result<()>;
    async fn write_metrics(&self, metrics: Vec<Metric>) -> Result<()>;
    async fn write_profiles(&self, profiles: Vec<Profile>) -> Result<()>;
}

pub trait TierMetadata {
    fn tier_id(&self) -> &str;
    fn priority(&self) -> u8;
    fn covers_time_range(&self, start: Timestamp, end: Timestamp) -> bool;
}
```

**Fix:** Add new section to `architecture.md` documenting all trait signatures and explaining two-tier trait architecture

**Files:** `plans/architecture.md` (lines ~120-170)

---

### 2. Naming Inconsistency

**Issue:** Documentation uses "TieredStorage" but code uses "Storage"

**Plans say:** `TieredStorage` (appears ~50 times in workspace-and-crates.md)
**Reality:** `pub struct Storage` in `crates/sequins-storage/src/storage.rs`

**Reason:** Simplified naming - "tiered" is implementation detail, "Storage" is clearer

**Fix:** Global find-replace "TieredStorage" → "Storage" in plan docs, add note explaining evolution

**Files:**
- `plans/workspace-and-crates.md`
- `plans/architecture.md`

---

## ❌ Major Drift

### 1. Workspace Structure (`plans/workspace-and-crates.md`)

**Issue:** File structure and crate contents differ significantly from documentation

**Documentation says:**
```
crates/sequins-storage/
├── src/
│   ├── ingest_trait.rs      # OtlpIngest trait
│   ├── query_trait.rs       # QueryApi trait
│   ├── management_trait.rs  # ManagementApi trait
│   ├── tiered.rs            # TieredStorage implementation
│   └── db/                  # Database operations
```

**Reality:**
```
crates/sequins-storage/
├── src/
│   ├── storage.rs              # Storage struct (not tiered.rs)
│   ├── hot_tier.rs             # HotTier (Papaya HashMap)
│   ├── cold_tier.rs            # ColdTier (Parquet + DataFusion)
│   ├── hot_tier_provider.rs   # DataFusion TableProvider for hot tier
│   ├── hot_tier_exec.rs        # DataFusion ExecutionPlan for hot tier
│   ├── config.rs               # Configuration types
│   └── error.rs                # Error types

Traits are in sequins-core/src/traits/:
├── ingest.rs       # OtlpIngest trait
├── query.rs        # QueryApi trait
├── management.rs   # ManagementApi trait
└── storage.rs      # StorageRead, StorageWrite, TierMetadata (NEW)
```

**Reason:**
1. Architectural evolution for DataFusion integration (see checkpoint doc)
2. Traits moved to `sequins-core` for better dependency management
3. Hot/cold tier separation for performance optimization

**Fix:** Completely rewrite file structure section in `workspace-and-crates.md` to match reality

**Files:**
- `plans/workspace-and-crates.md` (lines ~20-105, ~140-160)

---

### 2. Undocumented Crate: `sequins-web`

**Issue:** `sequins-web` crate exists but appears in NO plan documents

**Reality:**
```toml
# crates/sequins-web/Cargo.toml
[package]
name = "sequins-web"

[dependencies]
leptos = { version = "0.7", features = ["csr"] }
leptos_meta = "0.7"
leptos_router = "0.7"
wasm-bindgen = "0.2"

[lib]
crate-type = ["cdylib", "rlib"]
```

**Purpose:** WASM-based web UI using Leptos framework
**Target:** `wasm32-unknown-unknown`
**Deployment:** Client-side rendered web interface alternative to GPUI desktop app

**Why it exists:** Provides web-based alternative to native GPUI app for remote access scenarios

**Fix:** Add new section to `workspace-and-crates.md` documenting `sequins-web` crate

**Files:**
- `plans/workspace-and-crates.md` (add new section after sequins-app)
- `plans/deployment.md` (add web UI deployment mode)

---

### 3. Implementation Roadmap (`plans/implementation-roadmap.md`)

**Issue:** Phase 1 substantially complete but not marked as such

**Current roadmap says:**
```markdown
### Phase 1: Foundation (Week 1)
- [x] Set up Cargo.toml with all dependencies
- [x] Implement Arrow schemas for telemetry data (7 tests ✅)
- [x] Implement time types, ID types, trace models, log models, etc.
- [x] All core models complete: 65 tests passing ✅
- [ ] Create simple GPUI window
- [ ] Implement TieredStorage
- [ ] Configure Parquet bloom filters
- [ ] Write tests for storage layer
```

**Reality:** (from test output and code inspection)
- ✅ All core models implemented: 56 tests passing in sequins-core
- ✅ Storage layer implemented: 40 tests passing in sequins-storage
- ✅ HotTier (Papaya) + ColdTier (Parquet) working
- ✅ DataFusion integration complete (with MemTable prototype)
- ✅ Basic OtlpIngest, QueryApi, ManagementApi traits implemented
- ❌ GPUI window not started (sequins-app crate empty)
- ⚠️ Bloom filters configured in code but not documented as complete

**Fix:** Update Phase 1 checklist with actual completion status, add note about DataFusion integration work

**Files:**
- `plans/implementation-roadmap.md` (lines ~11-40)

---

### 4. Architecture Evolution Not Reflected in Main Docs

**Issue:** Checkpoint document captures DataFusion integration decisions but main architecture doc doesn't

**Checkpoint says:**
- Custom DataFusion `TableProvider` for hot tier
- Lazy conversion from HashMap → RecordBatch
- Unified SQL queries across hot + cold tiers
- Filter pushdown optimization

**Architecture.md says:**
- Basic three-layer architecture
- No mention of DataFusion integration pattern
- No explanation of how queries work across tiers

**Fix:** Add "Query Execution Architecture" section to `architecture.md` explaining DataFusion integration

**Files:**
- `plans/architecture.md` (add new section after "Data Lifecycle Stages")

---

## Test Status Summary

```
✅ sequins-core:    56 tests passing
✅ sequins-storage: 40 tests passing
✅ Total:           96 tests passing, 0 failures

Crates with 0 tests (expected - not implemented yet):
- sequins-app
- sequins-daemon
- sequins-server
- sequins-client
- sequins-web
```

---

## Action Items

### High Priority 🔴

1. **Update `plans/workspace-and-crates.md`**
   - Fix file structure for sequins-storage
   - Document actual trait locations (sequins-core, not sequins-storage)
   - Rename TieredStorage → Storage throughout
   - Add sequins-web crate documentation

2. **Update `plans/architecture.md`**
   - Add trait method signatures section
   - Document internal storage traits (StorageRead, StorageWrite, TierMetadata)
   - Add "Query Execution Architecture" section explaining DataFusion integration
   - Update diagrams to show hot/cold tier separation

3. **Update `plans/implementation-roadmap.md`**
   - Mark Phase 1 items as complete
   - Add Phase 1.5 for DataFusion integration work (already done)
   - Update milestone tracking

### Medium Priority 🟡

4. **Update `plans/deployment.md`**
   - Add web UI deployment mode
   - Document sequins-web as alternative interface

5. **Create `plans/datafusion-integration.md`** (NEW)
   - Document TableProvider pattern
   - Explain hot tier lazy conversion
   - Reference checkpoint document for historical context

### Low Priority 🟢

6. **Update `plans/INDEX.md`**
   - Add link to new datafusion-integration.md
   - Update descriptions to reflect current state

7. **Add implementation notes**
   - Document decision to move traits to sequins-core
   - Document Storage naming simplification
   - Link checkpoint documents from main plans

---

## Recommendations

### 1. Adopt Checkpoint Pattern

**Keep using checkpoint documents** like `CHECKPOINT-unified-datafusion-queries.md` for:
- Detailed research and decision-making
- Architecture exploration and prototyping
- Historical record of why decisions were made

**Then update main plan docs** when:
- Decisions are finalized
- Implementation is complete
- Architecture has stabilized

This two-tier documentation approach works well:
- Checkpoints = working documents (detailed, temporal)
- Plans = reference docs (concise, current state)

### 2. Documentation Maintenance Cadence

**After each phase:**
1. Review checkpoint documents created during phase
2. Extract key decisions and outcomes
3. Update relevant plan documents
4. Mark phase complete in roadmap

**Monthly:**
1. Run documentation sync check (like this report)
2. Identify drift
3. Schedule documentation updates

### 3. Cross-Reference More

Current docs rarely link to each other. Recommendations:
- Add "See also" sections with specific line references
- Link trait definitions to their implementation docs
- Cross-reference related decisions across documents

---

## Files Requiring Updates

```
HIGH PRIORITY:
✏️  plans/workspace-and-crates.md        - Major rewrite needed (file structure, traits location)
✏️  plans/architecture.md                - Add trait signatures, query architecture section
✏️  plans/implementation-roadmap.md      - Mark Phase 1 complete, update milestones

MEDIUM PRIORITY:
✏️  plans/deployment.md                  - Add web UI mode
📄 plans/datafusion-integration.md      - NEW: Document TableProvider pattern

LOW PRIORITY:
✏️  plans/INDEX.md                       - Update links and descriptions
📝 plans/architecture.md                 - Add implementation notes section
```

---

## Conclusion

The codebase has evolved thoughtfully with good architectural decisions (captured in checkpoint docs). The main planning documents lag behind but are not fundamentally wrong - they just need updating to reflect:

1. Trait location changes (sequins-core vs sequins-storage)
2. Storage implementation details (HotTier/ColdTier separation, DataFusion integration)
3. New crate (sequins-web)
4. Phase 1 completion

**Overall health:** ✅ **Good** - Architecture is sound, code quality is high (96 passing tests), documentation just needs catching up.

**Recommended next action:** Start with updating `workspace-and-crates.md` (highest drift) and `implementation-roadmap.md` (easiest win).

---

**Report generated:** 2025-01-07
**Next sync recommended:** 2025-02-07 (monthly cadence)
