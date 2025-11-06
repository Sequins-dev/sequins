# Deployment Scenarios & Monetization

[← Back to Index](INDEX.md)

**Related Documentation:** [workspace-and-crates.md](workspace-and-crates.md) | [architecture.md](architecture.md) | [scaling-strategy.md](scaling-strategy.md)

---

## Deployment Scenarios

### Local Development Mode (FREE)
**Use Case:** Individual developers testing locally, no network required

```
┌──────────────────────────────────────────────────────────┐
│   Sequins App (GPUI)                                     │
│                                                          │
│  ┌─────────────────────┐    ┌──────────────────────┐   │
│  │   UI Components     │    │  OtlpServer (Embed)  │   │
│  │                     │    │  - gRPC: 4317        │   │
│  └──────────┬──────────┘    │  - HTTP: 4318        │   │
│             │                └──────────┬───────────┘   │
│             │  Direct Access            │               │
│             │  (no network overhead)    │ OTLP Ingest   │
│             │                           │               │
│  ┌──────────▼───────────────────────────▼───────────┐   │
│  │        sequins-storage (TieredStorage)           │   │
│  │        ~/sequins/ (Parquet + bloom filters)      │   │
│  │        Implements: OtlpIngest + QueryApi         │   │
│  └──────────────────────────▲───────────────────────┘   │
│                             │ OTLP                      │
└─────────────────────────────┼───────────────────────────┘
                              │
                       [Local Services]
                    Your Node/Python/Go apps
```

**Architecture:**
- **OtlpServer** embedded in app for receiving telemetry (ports 4317/4318)
  - **Binds to 127.0.0.1 (localhost only)** - not accessible over network
  - Only local services on same machine can connect
  - Prevents abuse (running free app as network server)
- **UI** uses TieredStorage directly via QueryApi trait (no network overhead)
- **TieredStorage** stores all data locally in Parquet files with bloom filter indexes
- No external dependencies, zero network calls for queries
- **100% FREE for local development**

**Components:**
- `OtlpServer::new()` - Embedded OTLP server, localhost binding only (127.0.0.1)
- `TieredStorage` - Implements both `OtlpIngest` and `QueryApi` traits
- UI accesses storage directly (in-process)

**Security/Monetization Enforcement:**
- OtlpServer defaults to `127.0.0.1` binding (localhost only)
- Prevents running free app as enterprise server
- Only accepts telemetry from local machine

### Enterprise Cloud Deployment (PAID)
**Use Case:** Team deployment, multiple developers, centralized telemetry

```
┌───────────────────────────┐       ┌───────────────────────────┐
│  Developer Workstation #1 │       │  Developer Workstation #2 │
│                           │       │                           │
│  ┌─────────────────────┐  │       │  ┌─────────────────────┐  │
│  │  Sequins App (GPUI) │  │       │  │  Sequins App (GPUI) │  │
│  │  ┌───────────────┐  │  │       │  │  ┌───────────────┐  │  │
│  │  │ UI Components │  │  │       │  │  │ UI Components │  │  │
│  │  ├───────────────┤  │  │       │  │  ├───────────────┤  │  │
│  │  │ QueryClient   │  │  │       │  │  │ QueryClient   │  │  │
│  │  │ ManagementCli │  │  │       │  │  │ ManagementCli │  │  │
│  │  └───────┬───────┘  │  │       │  │  └───────┬───────┘  │  │
│  └──────────┼──────────┘  │       │  └──────────┼──────────┘  │
└─────────────┼──────────────┘       └─────────────┼──────────────┘
              │ HTTPS :8080/:8081                  │
              └──────────┬─────────────────────────┘
                         │
    ┌────────────────────▼───────────────────────────┐
    │   Enterprise Cloud/Network                     │
    │                                                │
    │  ┌──────────────────────────────────────────┐ │
    │  │  sequins-daemon                          │ │
    │  │                                          │ │
    │  │  ┌────────────────────────────────────┐ │ │
    │  │  │ OtlpServer                         │ │ │
    │  │  │  - gRPC: 4317                      │ │ │
    │  │  │  - HTTP: 4318                      │ │ │
    │  │  └────────────┬───────────────────────┘ │ │
    │  │               │                         │ │
    │  │  ┌────────────▼───────────────────────┐ │ │
    │  │  │ QueryServer (Port 8080)            │ │ │
    │  │  │  - GET /traces, /logs, /metrics    │ │ │
    │  │  │  + Optional Authentication         │ │ │
    │  │  │  + CORS                            │ │ │
    │  │  └────────────┬───────────────────────┘ │ │
    │  │               │                         │ │
    │  │  ┌────────────▼───────────────────────┐ │ │
    │  │  │ ManagementServer (Port 8081)       │ │ │
    │  │  │  - GET /storage/stats              │ │ │
    │  │  │  - POST /retention/cleanup         │ │ │
    │  │  │  + Required Authentication         │ │ │
    │  │  └────────────┬───────────────────────┘ │ │
    │  │               │                         │ │
    │  │  ┌────────────▼───────────────────────┐ │ │
    │  │  │  sequins-storage (TieredStorage)   │ │ │
    │  │  │  S3 + Parquet + Optional RocksDB   │ │ │
    │  │  └─────────▲──────────────────────────┘ │ │
    │  └────────────┼─────────────────────────────┘ │
    │               │ OTLP                           │
    │      [Production Services]                     │
    │   Prod/Staging/Test Environments               │
    └────────────────────────────────────────────────┘
```

**Architecture:**
- **Three independent servers** running in daemon process:
  - `OtlpServer::new_enterprise()` - Receives OTLP telemetry (ports 4317/4318)
    - **Binds to 0.0.0.0** - accepts network connections from any interface
    - Production services send telemetry over network
  - `QueryServer` - Handles data queries (port 8080, optional auth)
  - `ManagementServer` - Admin operations (port 8081, required auth)
- **Multiple apps connect remotely** using QueryClient and ManagementClient
- **Apps use separate clients:**
  - `QueryClient` - Queries traces/logs/metrics from QueryServer
  - `ManagementClient` - Admin operations via ManagementServer
- **Centralized telemetry** from all production environments
- **Requires paid license** for Query and Management API access

**Benefits of Separated Servers:**
- Independent lifecycle management per server
- Different authentication requirements (Query: optional, Management: required)
- Can scale individual servers independently
- Future: Could run servers on different machines

**Network Access:**
- OtlpServer uses `new_enterprise()` constructor → binds to `0.0.0.0`
- Accepts telemetry from remote production services
- This is the key difference from free tier (localhost-only binding)

## Monetization Strategy

| Feature | Local (FREE) | Enterprise (PAID) |
|---------|-------------|-------------------|
| OTLP Endpoint (gRPC/HTTP) | ✅ Embedded | ✅ Daemon |
| **Network Binding** | **127.0.0.1 only** | **0.0.0.0 (all interfaces)** |
| Remote OTLP Access | ❌ Localhost only | ✅ Network accessible |
| Local Visualization | ✅ Full UI | ✅ Full UI |
| Local Database | ✅ Direct Access | ❌ Remote Only |
| Query API | ❌ Not Needed | ✅ Required |
| Multi-User Access | ❌ Single Dev | ✅ Team |
| Centralized Telemetry | ❌ | ✅ |
| Authentication | ❌ | ✅ |
| Cloud Deployment | ❌ | ✅ |

### Anti-Abuse Protection

**Localhost-Only Binding (Free Tier):**

The free desktop app enforces localhost-only OTLP binding to prevent abuse:

```rust
// Free app - hardcoded to localhost
let otlp_server = OtlpServer::new()
    .with_grpc("127.0.0.1:4317".parse()?)  // Localhost only
    .with_http("127.0.0.1:4318".parse()?)  // Localhost only
    .start(storage)
    .await?;
```

**Why this matters:**
- Prevents running free app as enterprise server (e.g., on Mac Mini)
- Only accepts telemetry from same machine
- Cannot be reconfigured to accept network traffic
- Forces proper enterprise licensing for production use

**Enterprise daemon - network accessible:**
```rust
// Enterprise daemon - accepts network connections
let otlp_server = OtlpServer::new()
    .with_grpc("0.0.0.0:4317".parse()?)  // All interfaces
    .with_http("0.0.0.0:4318".parse()?)  // All interfaces
    .start(storage)
    .await?;
```

### Value Proposition

**Free Tier Benefits:**
- Zero cost for individual developers
- Full-featured local observability
- No vendor lock-in - always keep OTLP endpoint
- Perfect for local development and debugging
- No external dependencies or network required
- Localhost-only binding ensures legitimate use

**Enterprise Tier Benefits:**
- Team-wide centralized telemetry
- Single source of truth for all environments
- Multi-user access with authentication
- Cloud or on-premise deployment
- Production-grade observability
- Cost-effective compared to SaaS alternatives
- Keep data on your infrastructure

### Deployment Flexibility

The **separated server architecture** provides flexibility while maintaining simplicity:

**Free Mode (Local Development):**
- `OtlpServer` embedded in app for OTLP ingestion (ports 4317/4318)
- UI uses `TieredStorage` directly (implements `QueryApi`)
- No network layer needed for queries
- Zero configuration, zero cost

**Paid Mode (Enterprise Cloud):**
- Three independent servers in daemon:
  - `OtlpServer` - OTLP ingestion from production services
  - `QueryServer` - Remote queries via QueryClient
  - `ManagementServer` - Admin operations via ManagementClient
- All three servers share same `TieredStorage` instance (unified config)
- Different authentication per server (Query: optional, Management: required)
- Centralized telemetry for entire team

**Architecture Benefits:**
- **Type Safety** - Each server wraps one trait (OtlpIngest, QueryApi, or ManagementApi)
- **Internal Flexibility** - Can manage lifecycle of each server independently
- **Unified Config** - Single storage config used by all servers (v1.0)
- **Future Flexibility** - Could run servers separately with individual configs

---

**Last Updated:** 2025-11-05
