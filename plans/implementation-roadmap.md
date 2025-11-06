# Implementation Roadmap

[← Back to Index](INDEX.md)

**Related Documentation:** [workspace-and-crates.md](workspace-and-crates.md) | [module-breakdown.md](module-breakdown.md) | [architecture.md](architecture.md) | [database.md](database.md) | [otlp-ingestion.md](otlp-ingestion.md) | [ui-design.md](ui-design.md) | [state-management.md](state-management.md) | [retention.md](retention.md) | [deployment.md](deployment.md) | [technology-decisions.md](technology-decisions.md)

---

## Implementation Phases

### Phase 1: Foundation ✅ **COMPLETE**
**Goal:** Basic app structure and data storage
**Status:** All core infrastructure implemented, tested, and working
**Completed:** 2025-01-07

**Tasks:**
- [x] Set up Cargo.toml with all dependencies (datafusion, arrow, parquet, papaya, object_store, rocksdb optional, opentelemetry)
- [x] Implement Arrow schemas for telemetry data (spans, logs, metrics, profiles) (7 tests ✅)
- [x] Implement time types: Timestamp, Duration, TimeWindow
- [x] Implement ID types: TraceId, SpanId with OpenTelemetry integration
- [x] Implement trace models: Trace, Span, AttributeValue
- [x] Implement log models: LogEntry, LogSeverity, LogId
- [x] Implement metric models: Metric, MetricDataPoint, HistogramDataPoint, MetricId
- [x] Implement profile models: Profile, ProfileType, ProfileId
- [x] Remove serde dependency (using protobuf for serialization instead)
- [x] Implement proper error handling (TimestampError, TimeWindowError)
- [x] **All core models and Arrow schemas complete: 56 tests passing in sequins-core** ✅
- [x] **Implement Storage with hot/cold tiers (40 tests passing in sequins-storage)** ✅
- [x] **Configure Parquet bloom filters in code** ✅
- [x] **Implement OtlpIngest, QueryApi, ManagementApi traits** ✅
- [x] **Create HotTier (Papaya HashMap) with eviction** ✅
- [x] **Create ColdTier (Parquet + DataFusion + object_store)** ✅
- [x] **Integrate DataFusion for unified hot+cold queries** ✅
- [x] **Add internal storage traits (StorageRead, StorageWrite, TierMetadata)** ✅
- [ ] Create simple GPUI window with "Hello World" (deferred to Phase 3)

**Deliverables:**
- ✅ Arrow schemas for all telemetry types
- ✅ All data models defined and tested (56 core tests)
- ✅ Storage implementation with hot/cold tiers (40 storage tests)
- ✅ Three main traits (OtlpIngest, QueryApi, ManagementApi) defined
- ✅ Internal storage abstraction traits
- ✅ DataFusion integration for queries
- ⏸️ GPUI window (moved to Phase 3)

**Architectural Decisions Made:**
- Traits moved from `sequins-storage` to `sequins-core` for better dependency management
- Named `Storage` instead of `TieredStorage` (simpler, "tiered" is implementation detail)
- Added internal `StorageRead`/`StorageWrite`/`TierMetadata` traits for future distributed queries
- Integrated DataFusion custom `TableProvider` for unified hot/cold queries (see CHECKPOINT-unified-datafusion-queries.md)

**Dependencies:** None
**Next Phase:** Phase 2 (OTLP Ingestion)

### Phase 2: OTLP Ingestion (Week 2)
**Goal:** Receive and store telemetry data

**Tasks:**
- [ ] Implement OTLP/gRPC endpoint for traces
- [ ] Implement OTLP/HTTP endpoint (protobuf + JSON)
- [ ] Implement ingestion pipeline
- [ ] Parse OpenTelemetry protobuf format
- [ ] Store traces in hot tier (Papaya lock-free HashMap)
- [ ] Implement background flush to Parquet (every 5-15 minutes)
- [ ] Write Parquet files with bloom filters and compression
- [ ] Update RocksDB index on flush (if enabled)
- [ ] Test with real OTLP client
- [ ] Add logs and metrics support

**Deliverables:**
- Working OTLP endpoints (gRPC and HTTP)
- Ingestion pipeline writing to hot tier
- Background flush to Parquet working
- Integration tests with OTLP clients

**Dependencies:** Phase 1

### Phase 3: Basic UI Layout (Week 3)
**Goal:** Create main UI structure

**Tasks:**
- [ ] Implement main window layout (sidebar + main pane)
- [ ] Create service navigator sidebar (flat list with name filter)
- [ ] Implement tab navigation
- [ ] Create filter bar with time range picker
- [ ] Add settings panel
- [ ] Wire up reactive state management

**Deliverables:**
- Complete window layout
- Service navigator with filterable list
- Tab navigation working
- Filter bar functional
- State management in place

**Dependencies:** Phase 1

### Phase 4: Logs View (Week 4)
**Goal:** Functional log viewer

**Tasks:**
- [ ] Implement log list with virtualization
- [ ] Add timestamp formatting
- [ ] Create expandable log detail view
- [ ] Implement log search (full-text)
- [ ] Add severity filtering
- [ ] Add trace ID linking

**Deliverables:**
- Full-featured logs view
- Virtualized scrolling
- Search and filtering
- Trace navigation

**Dependencies:** Phase 2, Phase 3

### Phase 5: Traces View (Week 5)
**Goal:** Functional trace viewer

**Tasks:**
- [ ] Implement trace list
- [ ] Create trace timeline visualization
- [ ] Implement span details panel
- [ ] Add trace search
- [ ] Add span event display
- [ ] Add span link navigation

**Deliverables:**
- Complete traces view
- Waterfall timeline
- Span details
- Navigation between traces

**Dependencies:** Phase 2, Phase 3

### Phase 6: Metrics View (Week 6)
**Goal:** Functional metrics dashboards

**Tasks:**
- [ ] Implement histogram visualization
- [ ] Create time series charts
- [ ] Add latency charts (p50, p95, p99)
- [ ] Add status code distribution
- [ ] Add CPU and memory charts
- [ ] Support custom metrics

**Deliverables:**
- Complete metrics view
- Multiple chart types
- Standard and custom metrics

**Dependencies:** Phase 2, Phase 3

### Phase 7: Profiles View (Week 7)
**Goal:** Functional profile viewer

**Tasks:**
- [ ] Parse pprof format
- [ ] Implement flame graph visualization
- [ ] Add frame details panel
- [ ] Add profile search
- [ ] Link profiles to traces

**Deliverables:**
- Complete profiles view
- Flame graph visualization
- Profile navigation

**Dependencies:** Phase 2, Phase 3

### Phase 8: Polish & Performance (Week 8)
**Goal:** Production-ready app

**Tasks:**
- [ ] Implement data retention (file-based deletion of old Parquet files)
- [ ] Add settings persistence
- [ ] Optimize Parquet queries (predicate pushdown, partition pruning)
- [ ] Implement Parquet compaction for small files
- [ ] Add loading states
- [ ] Improve error handling
- [ ] Add keyboard shortcuts
- [ ] Theme support (light/dark)

**Deliverables:**
- Retention manager working (file-based cleanup)
- Settings persisted
- Performance optimized
- Parquet compaction working
- Error handling complete

**Dependencies:** All previous phases

### Phase 9: Testing & Documentation (Week 9)
**Goal:** Ship v1.0

**Tasks:**
- [ ] Write comprehensive tests
- [ ] Create user documentation
- [ ] Add example configurations
- [ ] Create demo video
- [ ] Set up GitHub Actions CI
- [ ] Create release build

**Deliverables:**
- Test coverage >80%
- Complete user documentation
- Release v1.0

**Dependencies:** All previous phases

## Timeline

```
Week 1: Foundation
  - Arrow schemas + Parquet setup
  - TieredStorage implementation
  - Basic UI skeleton

Week 2: OTLP Ingestion
  - gRPC/HTTP endpoints
  - Data parsing
  - Hot tier (Papaya HashMap) + background Parquet flush

Week 3: UI Layout
  - Window structure
  - Service navigator sidebar
  - Tab navigation

Week 4: Logs View
  - Log display
  - Search

Week 5: Traces View
  - Timeline
  - Span details

Week 6: Metrics View
  - Charts
  - Dashboards

Week 7: Profiles View
  - Flame graphs
  - Frame details

Week 8: Polish
  - Performance
  - UX improvements

Week 9: Release
  - Testing
  - Documentation
```

## Milestones

### M1: Data Layer Complete ✅ **ACHIEVED** (2025-01-07)
- ✅ Storage operational (hot + cold tiers)
- ✅ DataFusion integration for unified queries
- ✅ All core traits defined and tested
- ⏸️ OTLP ingestion (in progress, see Phase 2)

**Validation:**
- ✅ 56 tests passing in sequins-core (models, IDs, time types)
- ✅ 40 tests passing in sequins-storage (hot tier, cold tier, trait implementations)
- ✅ HotTier eviction working
- ✅ ColdTier Parquet writes working
- ✅ DataFusion SessionContext registered with object store
- ⏸️ OTLP protocol handlers (Phase 2)

### M2: Basic UI Complete (End of Week 4)
- Window layout functional
- Logs view working
- Can visualize basic telemetry

**Validation:**
- Open app, see logs
- Search and filter logs
- Navigate via trace IDs

### M3: Core Views Complete (End of Week 7)
- All four views functional (Logs, Metrics, Traces, Profiles)
- Service navigator working
- Basic visualization working

**Validation:**
- View all telemetry types
- Navigate between views and services
- Filter by service and time

### M4: v1.0 Release (End of Week 9)
- Performance optimized
- Documentation complete
- Tests passing
- Release build ready

**Validation:**
- Handle 1000 spans/sec
- UI responsive at 60fps
- Test coverage >80%
- All features documented
- Release published

## Risk Mitigation

### Technical Risks

**Risk:** GPUI learning curve
- **Mitigation:** Start with simple components, reference Zed source code
- **Contingency:** Budget extra time for UI phases

**Risk:** OTLP protocol complexity
- **Mitigation:** Use opentelemetry-proto crate, test with real clients early
- **Contingency:** Implement HTTP+JSON first (simpler), add gRPC later

**Risk:** Parquet query performance
- **Mitigation:** Implement Parquet bloom filters, use optional RocksDB index for high query volumes, test with large datasets, profile queries
- **Contingency:** Tune row group sizes, implement compaction, optimize DataFusion queries

**Risk:** Flame graph complexity
- **Mitigation:** Research existing implementations (e.g., speedscope)
- **Contingency:** Defer profiles view to v1.1 if needed

### Schedule Risks

**Risk:** Underestimated complexity
- **Mitigation:** Conservative estimates, buffer time in Phase 8
- **Contingency:** Reduce scope, defer profiles to v1.1

**Risk:** Blocked dependencies
- **Mitigation:** Work on independent tasks in parallel
- **Contingency:** Reorder phases if possible

## Success Criteria

### v1.0 Release Checklist
- [ ] All OTLP protocols working (gRPC, HTTP, HTTP+JSON)
- [ ] All four views functional (Logs, Metrics, Traces, Profiles)
- [ ] Service navigator with filtering
- [ ] Time-based data retention working
- [ ] Settings UI complete
- [ ] Cross-platform builds (macOS, Linux, Windows)
- [ ] Comprehensive test coverage (>80%)
- [ ] User documentation complete
- [ ] Performance targets met:
  - [ ] UI renders at 60fps
  - [ ] Handles 1000 spans/sec ingestion
  - [ ] Search returns results in <100ms
  - [ ] Memory usage <500MB for 1M spans

---

**Last Updated:** 2025-01-07 (Phase 1 marked complete, M1 milestone achieved)
