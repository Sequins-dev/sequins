# Sequins Planning Documentation

**Navigation hub for all architecture, design, and implementation planning documents.**

## About This Documentation

This directory contains the comprehensive planning documentation for Sequins, a local-first OpenTelemetry visualization tool. These documents serve as the **source of truth** for all development work.

Before implementing features, consult the relevant documentation. When plans change, update the corresponding documents to keep them accurate.

---

## Quick Links by Category

### Getting Started
- **[Main README](../README.md)** - Project overview, quick start, and introduction
- **[Implementation Roadmap](implementation-roadmap.md)** - 10-phase development plan with timeline

### System Design & Architecture
- **[Architecture](architecture.md)** - Three-layer architecture, component communication, system diagrams
- **[Workspace & Crates](workspace-and-crates.md)** - Cargo workspace structure, crate responsibilities, three-trait architecture
- **[Deployment](deployment.md)** - Local vs enterprise deployment scenarios, monetization strategy

### Data & Storage
- **[Data Models](data-models.md)** - Service, Trace, Span, Log, Metric, and Profile models
- **[Database](database.md)** - Storage architecture, two-tier system (hot/cold), DataFusion query patterns, Parquet optimization
- **[Parquet Schema](parquet-schema.md)** - Arrow schemas, type mappings, compression, indexing, row group sizing
- **[Retention](retention.md)** - RetentionManager architecture, file-based deletion, configurable policies
- **[Object Store Integration](object-store-integration.md)** - Universal storage abstraction, Parquet built-in indexes, optional RocksDB

### Implementation Details
- **[OTLP Ingestion](otlp-ingestion.md)** - gRPC and HTTP endpoints, ingestion pipeline, protocol handling
- **[UI Design](ui-design.md)** - Application layout, component hierarchy, view specifications
- **[State Management](state-management.md)** - Application state structure, reactive patterns, GPUI integration

### Development Guide
- **[Module Breakdown](module-breakdown.md)** - Module organization, file structure, responsibilities
- **[Technology Decisions](technology-decisions.md)** - Technology choices, rationale, trade-offs
- **[Configuration](configuration.md)** - KDL configuration system, daemon settings, deployment examples

### Scaling & Operations
- **[Scaling Strategy](scaling-strategy.md)** - Distributed multi-node architecture, Kubernetes deployment, horizontal scalability

### Future Planning
- **[Future Considerations](future-considerations.md)** - Open questions, short/medium/long-term features

---

## Document Descriptions

### [Architecture](architecture.md)
Complete system architecture including the three-layer design (UI, Business Logic, Data), component communication patterns, and architectural diagrams. Understand how all pieces fit together.

**Related:** workspace-and-crates.md, deployment.md, otlp-ingestion.md

---

### [Workspace & Crates](workspace-and-crates.md)
Detailed breakdown of the Cargo workspace structure and all crates (sequins-core, sequins-storage, sequins-server, sequins-client, sequins-app, sequins-daemon). Covers the three-trait architecture (OtlpIngest, QueryApi, ManagementApi) with extensive code examples.

**Related:** architecture.md, deployment.md, data-models.md, retention.md, module-breakdown.md

---

### [Deployment](deployment.md)
Deployment scenarios for local development (FREE) vs enterprise cloud deployment (PAID). Includes architecture flows, configuration examples, and monetization strategy.

**Related:** workspace-and-crates.md, architecture.md

---

### [Data Models](data-models.md)
Complete definitions of all data models: Service, Trace, Span, LogEntry, Metric, and Profile. Includes type definitions, relationships, and OpenTelemetry type usage (TraceId, SpanId).

**Related:** database.md, otlp-ingestion.md, ui-design.md

---

### [Database](database.md)
Storage architecture overview covering the two-tier system (hot in-memory tier + cold Parquet tier), DataFusion query patterns, Parquet optimization strategies, and retention cleanup. Includes query examples and performance characteristics.

**Related:** data-models.md, parquet-schema.md, retention.md, object-store-integration.md, workspace-and-crates.md

---

### [Parquet Schema](parquet-schema.md)
Complete Arrow schema definitions for all telemetry types (traces, spans, logs, metrics, profiles). Covers type mappings (OTLP → Arrow), compression strategies (Zstd, bloom filters, dictionary encoding), row group sizing, partitioning strategies, and conversion helpers between Rust types and RecordBatches.

**Related:** data-models.md, database.md, object-store-integration.md, technology-decisions.md

---

### [Retention](retention.md)
RetentionManager architecture with per-data-type retention policies, file-based deletion of old Parquet files, graceful shutdown via tokio channels, and complete implementation details. Covers automatic cleanup at hour-bucket granularity and manual triggers.

**Related:** database.md, object-store-integration.md, workspace-and-crates.md

---

### [Object Store Integration](object-store-integration.md)
Comprehensive guide to using the `object_store` crate for universal blob storage. Explains why we use `LocalFileSystem` for local development and S3/MinIO for cloud, with config-driven backend selection, performance characteristics, testing strategies, and complete implementation examples.

**Related:** scaling-strategy.md, database.md, workspace-and-crates.md

---

### [OTLP Ingestion](otlp-ingestion.md)
OTLP endpoint implementation for gRPC (port 4317) and HTTP (port 4318), supporting protobuf and JSON. Covers ingestion pipeline, data enrichment, and async storage.

**Related:** data-models.md, database.md, workspace-and-crates.md

---

### [UI Design](ui-design.md)
Complete UI specification including application window layout, component tree/hierarchy, and detailed view specifications for Logs, Metrics, Traces, and Profiles views. Includes ASCII mockups.

**Related:** state-management.md, data-models.md, architecture.md

---

### [State Management](state-management.md)
Application state structure, reactive update patterns, and GPUI integration. Covers how state changes propagate through the UI and async data fetching patterns.

**Related:** ui-design.md, workspace-and-crates.md

---

### [Module Breakdown](module-breakdown.md)
Module organization across all crates, file structure, and responsibilities. Helps navigate the codebase and understand where to implement features.

**Related:** workspace-and-crates.md, implementation-roadmap.md

---

### [Implementation Roadmap](implementation-roadmap.md)
10-phase implementation plan from foundation to v1.0 release. Includes timeline, milestones, dependencies between phases, and testing strategy for each phase.

**Related:** All other plan docs as relevant per phase

---

### [Technology Decisions](technology-decisions.md)
Technology choices and rationale: Why GPUI, DataFusion + Parquet, two-tier storage, optional RocksDB indexes, Tokio, Tonic, and Axum. Covers trade-offs, alternatives considered, and performance implications.

**Related:** architecture.md, workspace-and-crates.md, database.md, parquet-schema.md

---

### [Configuration](configuration.md)
Complete configuration system using KDL (Cuddle Document Language). Covers daemon configuration, storage backends, retention policies, OTLP endpoints, authentication, TLS, and deployment examples. Includes Rust implementation with knuffel parser.

**Related:** deployment.md, object-store-integration.md, retention.md, workspace-and-crates.md

---

### [Scaling Strategy](scaling-strategy.md)
Distributed multi-node scaling architecture for enterprise deployment. Covers horizontal scalability, Kubernetes StatefulSets, tiered storage (local SSD + S3/MinIO), gossip protocol membership, scatter-gather queries, and operational concerns. Includes replication strategies, shard assignment, and performance targets for high-volume OTLP ingestion.

**Related:** deployment.md, workspace-and-crates.md, database.md, retention.md

---

### [Future Considerations](future-considerations.md)
Open questions and future features organized by timeframe (short/medium/long-term). Includes potential enhancements like cloud sync, plugins, remote mode, and advanced querying.

**Related:** implementation-roadmap.md, scaling-strategy.md

---

## How to Use This Documentation

1. **New to the project?** Start with the [Main README](../README.md), then read [Architecture](architecture.md)
2. **Implementing a feature?** Check [Implementation Roadmap](implementation-roadmap.md), then the relevant technical docs
3. **Understanding the codebase?** See [Workspace & Crates](workspace-and-crates.md) and [Module Breakdown](module-breakdown.md)
4. **Working on data/storage?** See [Data Models](data-models.md), [Database](database.md), [Parquet Schema](parquet-schema.md), [Object Store Integration](object-store-integration.md), and [Retention](retention.md)
5. **Working on UI?** See [UI Design](ui-design.md) and [State Management](state-management.md)
6. **Working on ingestion?** See [OTLP Ingestion](otlp-ingestion.md) and [Workspace & Crates](workspace-and-crates.md)
7. **Planning deployment?** See [Deployment](deployment.md) and [Configuration](configuration.md) for local vs enterprise modes
8. **Need horizontal scaling?** See [Scaling Strategy](scaling-strategy.md) for distributed multi-node architecture

## Keeping Documentation Updated

These documents are living documentation. When plans change:

1. Identify which document(s) need updating
2. Update the content to reflect new decisions
3. Update cross-links if document structure changes
4. Add notes about why decisions changed (in comments or a changelog section)

---

**Last Updated:** 2025-11-05
