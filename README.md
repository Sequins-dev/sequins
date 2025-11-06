# Sequins

**Local-first OpenTelemetry visualization tool for distributed systems**

Sequins is a desktop-native observability platform that provides developers with a rich interface for exploring traces, logs, metrics, and profiles. Unlike cloud-based platforms, Sequins runs entirely on your machine, offering zero-latency visualization and complete data privacy.

---

## Features

- **Embedded OTLP endpoint** - gRPC (port 4317), HTTP (port 4318), and HTTP+JSON support
- **Real-time service map** - Visualize service dependencies and relationships
- **Distributed trace timeline** - Explore request flows across services
- **Log search** - Full-text search with structured data expansion
- **Metrics dashboards** - Latency, throughput, and resource usage visualization
- **Flame graphs** - CPU and memory profiling visualization
- **Local-first storage** - All data stored locally in embedded database
- **Time-based retention** - Automatic cleanup of old data

---

## Target Users

Developers working on distributed systems who need local observability during development and debugging.

---

## Quick Start

### Prerequisites

- Rust 1.75+ (edition 2024 support)
- macOS, Linux, or Windows

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/sequins.git
cd sequins

# Build and run
cargo run --release
```

The app will start and listen for OTLP data on:
- **gRPC:** `localhost:4317`
- **HTTP:** `localhost:4318`

### Send Data

Configure your application to send OTLP data to Sequins:

```bash
# Example: Node.js with OpenTelemetry
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_SERVICE_NAME=my-app
```

See the [OTLP Ingestion](plans/otlp-ingestion.md) documentation for more details.

---

## Architecture

Sequins uses a three-layer architecture:

```
┌─────────────────────────────────────────┐
│         UI Layer (GPUI)                 │  ← Desktop app interface
├─────────────────────────────────────────┤
│      Business Logic Layer               │  ← OTLP parsing, queries
├─────────────────────────────────────────┤
│      Data Layer (Turso/libSQL)          │  ← Embedded database
└─────────────────────────────────────────┘
```

**Key Technologies:**
- **GPUI** - GPU-accelerated UI framework
- **Turso (libSQL)** - Embedded SQLite-compatible database
- **Tokio** - Async runtime
- **Tonic** - gRPC framework
- **Axum** - HTTP framework

See [Architecture](plans/architecture.md) for detailed design.

---

## Documentation

📋 **[Planning Documentation](plans/INDEX.md)** - Comprehensive architecture, design, and implementation docs

### Quick Links

- **Getting Started**
  - [Implementation Roadmap](plans/implementation-roadmap.md) - Development phases and timeline

- **System Design**
  - [Architecture](plans/architecture.md) - Three-layer design and communication patterns
  - [Workspace & Crates](plans/workspace-and-crates.md) - Cargo structure and trait architecture
  - [Deployment](plans/deployment.md) - Local vs enterprise deployment modes

- **Data & Storage**
  - [Data Models](plans/data-models.md) - Trace, Span, Log, Metric, Profile models
  - [Database](plans/database.md) - Schema, indexes, and queries
  - [Retention](plans/retention.md) - Automatic data cleanup

- **Implementation**
  - [OTLP Ingestion](plans/otlp-ingestion.md) - gRPC and HTTP endpoint implementation
  - [UI Design](plans/ui-design.md) - Component hierarchy and view specifications
  - [State Management](plans/state-management.md) - Reactive state patterns

- **Scaling**
  - [Scaling Strategy](plans/scaling-strategy.md) - Multi-node distributed architecture (enterprise)

---

## Project Status

🚧 **In Development** - Phase 1 (Foundation)

See [Implementation Roadmap](plans/implementation-roadmap.md) for current progress.

### v1.0 Goals

- All OTLP protocols working (gRPC, HTTP, HTTP+JSON)
- All four views functional (Logs, Metrics, Traces, Profiles)
- Service map showing dependencies
- Time-based data retention
- Cross-platform builds (macOS, Linux, Windows)

---

## Development

### Building from Source

```bash
# Build all crates
cargo build

# Run tests
cargo test

# Format code
cargo fmt

# Run linter
cargo clippy
```

### Project Structure

```
sequins/
├── Cargo.toml              # Workspace root
├── README.md               # This file
├── CLAUDE.md               # Development guidelines
├── plans/                  # Planning documentation
│   ├── INDEX.md            # Documentation navigation
│   ├── architecture.md
│   ├── database.md
│   └── ...
├── crates/
│   ├── sequins-core/       # Shared types and traits
│   ├── sequins-storage/    # Data layer
│   ├── sequins-server/     # Protocol adapters
│   ├── sequins-client/     # Remote client
│   ├── sequins-app/        # Desktop app (GPUI)
│   └── sequins-daemon/     # Enterprise daemon
└── tests/                  # Integration tests
```

See [Module Breakdown](plans/module-breakdown.md) for detailed organization.

---

## Deployment Modes

### Local Mode (FREE)

Run Sequins on your local machine for development:

```bash
cargo run --release
```

- Embedded OTLP server
- Local database storage
- Full UI access
- No network required

### Enterprise Mode (PAID)

Deploy Sequins daemon on server for team access:

```bash
# Start daemon
sequins-daemon --config /etc/sequins/config.toml

# Connect app
sequins-app --remote https://sequins.company.com
```

- Centralized telemetry
- Multiple users
- Authentication & authorization
- Horizontal scaling support

See [Deployment](plans/deployment.md) for configuration details.

---

## Contributing

Contributions are welcome! Please read our contributing guidelines before submitting PRs.

### Before Contributing

1. Read the [planning documentation](plans/INDEX.md)
2. Check the [implementation roadmap](plans/implementation-roadmap.md)
3. Follow the [coding guidelines](CLAUDE.md)
4. Ensure tests pass: `cargo test`
5. Format code: `cargo fmt`
6. Run linter: `cargo clippy`

---

## License

[License information TBD]

---

## Acknowledgments

Inspired by:
- **Jaeger** - Distributed tracing UI
- **Grafana Tempo** - Object storage architecture
- **Honeycomb** - Observability workflows
- **Speedscope** - Flame graph visualization

Built with:
- **GPUI** - GPU-accelerated UI (Zed team)
- **Turso** - Embedded database (Chiselstrike)
- **OpenTelemetry** - Observability standard (CNCF)

---

## Support

- **Documentation:** [plans/INDEX.md](plans/INDEX.md)
- **Issues:** [GitHub Issues](https://github.com/yourusername/sequins/issues)
- **Discussions:** [GitHub Discussions](https://github.com/yourusername/sequins/discussions)

---

**Last Updated:** 2025-11-05
