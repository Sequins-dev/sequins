# Configuration System

[← Back to Index](INDEX.md)

**Related Documentation:** [deployment.md](deployment.md) | [workspace-and-crates.md](workspace-and-crates.md) | [object-store-integration.md](object-store-integration.md) | [retention.md](retention.md)

---

## Overview

Sequins uses **YAML (YAML Ain't Markup Language)** for configuration files. YAML provides a clean, human-friendly syntax that's more readable than TOML for hierarchical configuration while maintaining strong type safety.

**Why YAML?**
- Human-friendly syntax with clear nesting
- Industry standard for DevOps configuration
- Excellent support for hierarchical data
- Built-in comments (`#`)
- Mature Rust ecosystem with serde integration
- Widely understood by DevOps teams

---

## Configuration File Location

### Daemon Mode
```
/etc/sequins/config.yaml           # System-wide (Linux)
/usr/local/etc/sequins/config.yaml # System-wide (macOS)
~/.config/sequins/config.yaml      # User-specific
./sequins.yaml                      # Current directory (development)
```

**Resolution Order:** Current directory → User config → System config → Defaults

### Local Mode
Local mode (free tier) uses sensible defaults and doesn't require configuration. All settings are embedded in the application.

---

## Server Architecture & Configuration

Sequins uses a **separated server architecture** with independent server types for each API:

- **OtlpServer** - Handles OTLP ingestion (ports 4317/4318)
- **QueryServer** - Handles query API (port 8080, paid feature)
- **ManagementServer** - Handles admin API (port 8081, paid feature)

**Configuration Modes:**

1. **Unified Config (Default)** - All servers share the same storage backend
   - Single `storage` section configures TieredStorage
   - All three servers use the same TieredStorage instance
   - Simplest deployment, recommended for most use cases

2. **Separated Config (Future)** - Each server can have independent configuration
   - Could run servers on different machines
   - Could use different storage backends per server
   - Advanced flexibility for specialized deployments

**Current Implementation:** Only unified config is supported in v1.0. The separated architecture provides internal flexibility and type safety while maintaining simple unified configuration.

---

## Configuration Schema

### Complete Example (Unified Mode)

```yaml
// Sequins Daemon Configuration
// This configuration is only used in daemon/enterprise mode

// Storage backend configuration
storage {
    // Backend type: "local", "s3", or "minio"
    backend "local"

    // Local filesystem path for development
    path "/var/lib/sequins/data"

    // Alternative S3 configuration
    // backend "s3"
    // bucket "sequins-prod-telemetry"
    // region "us-east-1"

    // Alternative MinIO configuration
    // backend "minio"
    // endpoint "https://minio.company.com"
    // bucket "sequins-telemetry"
}

// Data lifecycle configuration (unified hot-to-cold and retention)
data-lifecycle {
    // Hot tier (in-memory) - Performance optimization
    // Data stays in memory for fast queries (< 1ms latency)
    hot-tier {
        duration-minutes 15       // Keep data in memory for 15 min
        max-memory-mb 512         // Maximum hot tier memory usage
    }

    // How often to check for data to flush from hot → cold
    flush-check-interval seconds=60

    // Retention policies (per telemetry type) - Compliance/Cost management
    // Data moves to cold tier (Parquet) after hot tier duration
    // Data is deleted after retention period expires
    retention {
        traces hours=24           // Keep traces for 24 hours
        logs hours=24             // Keep logs for 24 hours
        metrics hours=168         // Keep metrics for 7 days
        profiles hours=24         // Keep profiles for 24 hours

        // How often to check for data to delete
        cleanup-interval seconds=300
    }
}

// OTLP ingestion endpoints
otlp {
    // Bind address: "127.0.0.1" for local-only (FREE), "0.0.0.0" for network (PAID)
    // Default: "0.0.0.0" in daemon mode (enterprise)
    // Note: Free desktop app hardcodes "127.0.0.1" to prevent abuse
    bind-address "0.0.0.0"

    // gRPC endpoint
    grpc {
        enabled true
        port 4317
        max-connections 1000
    }

    // HTTP endpoint
    http {
        enabled true
        port 4318
        max-body-size-mb 10
    }
}

// Query API (Enterprise/Paid feature)
query-api {
    enabled true
    port 8080

    // CORS settings for web clients
    cors {
        allowed-origins origin="http://localhost:3000" origin="https://app.company.com"
        allow-credentials true
    }

    // Authentication
    auth {
        type "bearer"  // "bearer", "basic", or "none"
        token-file "/etc/sequins/tokens.txt"
    }
}

// Management API (admin operations)
management-api {
    enabled true
    port 8081

    // Admin authentication (separate from query API)
    auth {
        type "bearer"
        token-file "/etc/sequins/admin-tokens.txt"
    }
}

// Note: Client Authentication
// RemoteClient uses optional bearer tokens for each API:
// - query_auth: Optional, added as "Authorization: Bearer <token>" header if configured
// - management_auth: Required for ManagementApi methods, returns clear error if not configured
// See workspace-and-crates.md for RemoteClient implementation details

// Parquet optimization settings
parquet {
    // Compression
    compression "zstd"
    compression-level 3

    // Row group sizing
    row-group-size 1000000  // 1M rows

    // Optional RocksDB index for faster trace lookups
    // Default: use Parquet built-in indexes (bloom filters, column stats)
    // rocksdb {
    //     enabled true
    //     path "/var/lib/sequins/indexes"
    // }

    // Bloom filters
    bloom-filters {
        trace-id true
        span-id true
        service-name true
    }
}

// Logging configuration
logging {
    level "info"  // "trace", "debug", "info", "warn", "error"

    // Log format
    format "json"  // "json" or "pretty"

    // Log to file
    file {
        enabled true
        path "/var/log/sequins/sequins.log"
        rotation-size-mb 100
        max-backups 10
    }
}

// Performance tuning
performance {
    // Worker thread count (defaults to CPU count)
    worker-threads 8

    // Query timeout
    query-timeout-seconds 30

    // Max concurrent queries
    max-concurrent-queries 100

    // Note: flush-interval has moved to data-lifecycle.flush-check-interval
}

// TLS/SSL configuration (optional)
tls {
    enabled false

    // Certificate files
    // cert-file "/etc/sequins/certs/server.crt"
    // key-file "/etc/sequins/certs/server.key"

    // Client certificate verification (mutual TLS)
    // verify-client true
    // ca-file "/etc/sequins/certs/ca.crt"
}

// Clustering configuration (future - multi-node scaling)
clustering {
    enabled false

    // Node identity
    // node-id "node-1"
    //
    // // Gossip protocol for membership
    // gossip {
    //     port 7946
    //     seeds node="node1.example.com:7946" node="node2.example.com:7946"
    // }
    //
    // // Shard assignment
    // sharding {
    //     strategy "consistent-hash"  // or "range"
    //     replication-factor 3
    // }
}
```

### Future: Separated Config Mode

In future versions, the configuration could support running each server independently with separate configs:

```yaml
// otlp-server.yaml - Run only OTLP ingestion server
server {
    type "otlp"  // Only start OtlpServer
}

storage {
    backend "s3"
    bucket "ingest-tier"
    region "us-east-1"
}

otlp {
    grpc {
        enabled true
        port 4317
    }
    http {
        enabled true
        port 4318
    }
}
```

```yaml
// query-server.yaml - Run only query API server
server {
    type "query"  // Only start QueryServer
}

storage {
    backend "s3"
    bucket "query-tier"  // Could be different bucket
    region "us-east-1"
}

query-api {
    enabled true
    port 8080
    auth {
        type "bearer"
        token-file "/etc/sequins/api-tokens.txt"
    }
}
```

```yaml
// management-server.yaml - Run only management API server
server {
    type "management"  // Only start ManagementServer
}

storage {
    backend "local"
    path "/var/lib/sequins/admin"
}

management-api {
    enabled true
    port 8081
    auth {
        type "bearer"
        token-file "/etc/sequins/admin-tokens.txt"
    }
}
```

**Note:** This separated config mode is not implemented in v1.0. All servers currently share the same storage configuration. This example shows the potential flexibility enabled by the separated server architecture.

---

## Configuration Structs

### Rust Implementation

```rust
// sequins-server/src/config.rs
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Main configuration structure
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub storage: StorageConfig,
    pub retention: RetentionConfig,
    pub otlp: OtlpConfig,
    pub query_api: QueryApiConfig,
    pub management_api: ManagementApiConfig,
    pub parquet: ParquetConfig,
    pub logging: LoggingConfig,
    pub performance: PerformanceConfig,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub clustering: Option<ClusteringConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub hot_tier: HotTierConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StorageBackend {
    Local { path: PathBuf },
    S3 { bucket: String, region: String },
    MinIO { endpoint: String, bucket: String },
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HotTierConfig {
    pub duration_minutes: u32,
    pub max_memory_mb: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RetentionConfig {
    pub traces_hours: u32,
    pub logs_hours: u32,
    pub metrics_hours: u32,
    pub profiles_hours: u32,
    pub cleanup_interval_seconds: u64,
}

/// Unified data lifecycle configuration (ergonomic wrapper)
///
/// Groups hot-to-cold transition and retention settings together
/// for easier understanding of data flow. Internally, these remain
/// separate concerns with independent background tasks.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DataLifecycleConfig {
    /// Hot tier settings (performance optimization)
    pub hot_tier: HotTierConfig,

    /// How often to check for data to flush from hot to cold (seconds)
    /// Default: 60 seconds
    pub flush_check_interval_seconds: u64,

    /// Retention settings (compliance/cost management)
    pub retention: RetentionConfig,
}

impl DataLifecycleConfig {
    /// Typical development configuration (short retention, frequent flush)
    pub fn development() -> Self {
        Self {
            hot_tier: HotTierConfig {
                duration_minutes: 5,
                max_memory_mb: 256,
            },
            flush_check_interval_seconds: 30,
            retention: RetentionConfig {
                traces_hours: 24,
                logs_hours: 24,
                metrics_hours: 24,
                profiles_hours: 24,
                cleanup_interval_seconds: 300,
            },
        }
    }

    /// Typical production configuration (longer retention, standard flush)
    pub fn production() -> Self {
        Self {
            hot_tier: HotTierConfig {
                duration_minutes: 15,
                max_memory_mb: 512,
            },
            flush_check_interval_seconds: 60,
            retention: RetentionConfig {
                traces_hours: 168,  // 7 days
                logs_hours: 168,
                metrics_hours: 720,  // 30 days
                profiles_hours: 72,  // 3 days
                cleanup_interval_seconds: 300,
            },
        }
    }
}

/**
 * GUIDANCE: Choosing Data Lifecycle Values
 *
 * ## Hot Tier Duration (hot-tier.duration-minutes)
 *
 * **Purpose:** Keep recently ingested data in memory for fast queries (< 1ms)
 *
 * **Typical values:**
 * - Development: 5 minutes (faster iteration, lower memory)
 * - Production: 15 minutes (optimal for most observability queries)
 * - High-traffic: 10 minutes (reduce memory pressure)
 *
 * **Rule of thumb:** Most queries are for data < 15 minutes old (90-95% hit rate)
 *
 * ## Flush Check Interval (flush-check-interval-seconds)
 *
 * **Purpose:** How often to check for data to move from hot → cold
 *
 * **Typical values:**
 * - Development: 30 seconds (faster testing)
 * - Production: 60 seconds (good balance)
 * - High-throughput: 30 seconds (prevent memory buildup)
 *
 * **Rule of thumb:** Should be << hot tier duration (e.g., 1/15th of hot tier duration)
 *
 * ## Retention Duration (retention.{type}_hours)
 *
 * **Purpose:** How long to keep data before deletion (compliance/cost)
 *
 * **Typical values by use case:**
 *
 * ### Development/Testing
 * - All types: 1-24 hours (fast cleanup, low storage)
 *
 * ### Production (Cost-Conscious)
 * - Traces: 24-48 hours (recent debugging)
 * - Logs: 48-72 hours (troubleshooting)
 * - Metrics: 168 hours / 7 days (trend analysis)
 * - Profiles: 24 hours (point-in-time analysis)
 *
 * ### Production (Standard)
 * - Traces: 168 hours / 7 days (week-over-week comparison)
 * - Logs: 168 hours / 7 days
 * - Metrics: 720 hours / 30 days (monthly trends)
 * - Profiles: 72 hours / 3 days (recent performance)
 *
 * ### Production (Compliance/Long-term)
 * - Traces: 720-2160 hours / 30-90 days (audit trail)
 * - Logs: 720-2160 hours / 30-90 days (regulatory compliance)
 * - Metrics: 2160-8760 hours / 90-365 days (long-term trends)
 * - Profiles: 168 hours / 7 days (performance baselines)
 *
 * ## Cleanup Interval (retention.cleanup-interval-seconds)
 *
 * **Purpose:** How often to check for data to delete
 *
 * **Typical values:**
 * - Development: 60 seconds (fast cleanup for testing)
 * - Production: 300 seconds / 5 minutes (standard)
 * - Low-priority: 600-900 seconds (reduce overhead)
 *
 * **Rule of thumb:** Can be slower than flush interval (deletion is less time-sensitive)
 *
 * ## Relationship Between Hot and Retention
 *
 * **Important:** Hot tier duration and retention are INDEPENDENT
 *
 * Data flow:
 * 1. Ingestion → Hot tier (immediate)
 * 2. Hot → Cold after `hot-tier.duration-minutes` (e.g., 15 min)
 * 3. Cold → Deleted after `retention.{type}_hours` (e.g., 168 hours)
 *
 * Example lifecycle for a trace with 15min hot / 168h retention:
 * - Minutes 0-15: Hot tier (< 1ms queries)
 * - Minutes 15 - Hour 168: Cold tier (15-35ms queries)
 * - Hour 168+: Deleted
 *
 * **Time in cold tier = retention - hot tier duration**
 * - With 15min hot, 168h retention: ~167h 45min in cold tier
 * - With 5min hot, 24h retention: ~23h 55min in cold tier
 *
 * ## Memory Considerations
 *
 * **Hot tier memory usage** depends on:
 * - `hot-tier.max-memory-mb`: Hard limit
 * - `hot-tier.duration-minutes`: How much data accumulates
 * - Ingestion rate: spans/logs/metrics per second
 *
 * **Estimate memory needed:**
 * - 1000 spans/sec × 15 min × ~1KB/span = ~900MB
 * - 1000 spans/sec × 5 min × ~1KB/span = ~300MB
 *
 * **If hitting memory limits:**
 * - Reduce `hot-tier.duration-minutes` (e.g., 15 → 10 min)
 * - Increase `hot-tier.max-memory-mb` (more RAM)
 * - Reduce `flush-check-interval-seconds` (flush more often)
 */

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OtlpConfig {
    pub grpc: OtlpEndpointConfig,
    pub http: OtlpEndpointConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OtlpEndpointConfig {
    pub enabled: bool,
    pub port: u16,
    #[serde(default)]
    pub max_connections: Option<usize>,
    #[serde(default)]
    pub max_body_size_mb: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct QueryApiConfig {
    pub enabled: bool,
    pub port: u16,
    #[serde(default)]
    pub cors: Option<CorsConfig>,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ManagementApiConfig {
    pub enabled: bool,
    pub port: u16,
    #[serde(default)]
    pub auth: Option<AuthConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CorsConfig {
    pub allowed_origins: Vec<String>,
    pub allow_credentials: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    pub auth_type: AuthType,
    pub token_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthType {
    Bearer,
    Basic,
    None,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ParquetConfig {
    pub compression: String,
    pub compression_level: i32,
    pub row_group_size: usize,
    pub rocksdb: Option<RocksDbConfig>,
    pub bloom_filters: BloomFilterConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RocksDbConfig {
    pub enabled: bool,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BloomFilterConfig {
    pub trace_id: bool,
    pub span_id: bool,
    pub service_name: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: LogFormat,
    pub file: Option<LogFileConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Json,
    Pretty,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogFileConfig {
    pub enabled: bool,
    pub path: PathBuf,
    pub rotation_size_mb: usize,
    pub max_backups: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PerformanceConfig {
    pub worker_threads: Option<usize>,
    pub query_timeout_seconds: u64,
    pub flush_interval_seconds: u64,
    pub max_concurrent_queries: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TlsConfig {
    pub enabled: bool,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub verify_client: bool,
    pub ca_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClusteringConfig {
    pub enabled: bool,
    pub node_id: Option<String>,
    pub gossip: Option<GossipConfig>,
    pub sharding: Option<ShardingConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GossipConfig {
    pub port: u16,
    pub seeds: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShardingConfig {
    pub strategy: ShardingStrategy,
    pub replication_factor: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ShardingStrategy {
    ConsistentHash,
    Range,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            storage: StorageConfig {
                backend: StorageBackend::Local {
                    path: PathBuf::from("~/.sequins/data"),
                },
                hot_tier: HotTierConfig {
                    duration_minutes: 15,
                    max_memory_mb: 512,
                },
            },
            retention: RetentionConfig {
                traces_hours: 24,
                logs_hours: 24,
                metrics_hours: 24,
                profiles_hours: 24,
                cleanup_interval_seconds: 300,
            },
            otlp: OtlpConfig {
                grpc: OtlpEndpointConfig {
                    enabled: true,
                    port: 4317,
                    max_connections: Some(1000),
                    max_body_size_mb: None,
                },
                http: OtlpEndpointConfig {
                    enabled: true,
                    port: 4318,
                    max_connections: None,
                    max_body_size_mb: Some(10),
                },
            },
            query_api: QueryApiConfig {
                enabled: false,
                port: 8080,
                cors: None,
                auth: None,
            },
            management_api: ManagementApiConfig {
                enabled: false,
                port: 8081,
                auth: None,
            },
            parquet: ParquetConfig {
                compression: "zstd".to_string(),
                compression_level: 3,
                row_group_size: 1_000_000,
                rocksdb: None,
                bloom_filters: BloomFilterConfig {
                    trace_id: true,
                    span_id: true,
                    service_name: true,
                },
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                format: LogFormat::Pretty,
                file: None,
            },
            performance: PerformanceConfig {
                worker_threads: None,
                query_timeout_seconds: 30,
                flush_interval_seconds: 60,
                max_concurrent_queries: 100,
            },
            tls: None,
            clustering: None,
        }
    }
}
```

---

## Configuration Loading

### Loading Strategy

```rust
// sequins-server/src/config.rs
use anyhow::{Context, Result};
use yaml::KdlDocument;
use std::fs;

impl Config {
    /// Load configuration from YAML file with fallback to defaults
    pub fn load() -> Result<Self> {
        // Try loading in priority order
        let config_paths = [
            PathBuf::from("./sequins.yaml"),
            dirs::config_dir()
                .map(|p| p.join("sequins/config.yaml"))
                .unwrap_or_default(),
            PathBuf::from("/etc/sequins/config.yaml"),
            PathBuf::from("/usr/local/etc/sequins/config.yaml"),
        ];

        for path in &config_paths {
            if path.exists() {
                return Self::load_from_file(path);
            }
        }

        // No config file found, use defaults
        tracing::info!("No config file found, using defaults");
        Ok(Self::default())
    }

    /// Load configuration from specific YAML file
    pub fn load_from_file(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        let doc: KdlDocument = content.parse()
            .with_context(|| format!("Failed to parse YAML config: {}", path.display()))?;

        Self::from_yaml(&doc)
            .with_context(|| format!("Failed to deserialize config: {}", path.display()))
    }

    /// Parse YAML document into Config struct
    fn from_yaml(doc: &KdlDocument) -> Result<Self> {
        // Use knuffel or kaydle crate for YAML -> Rust struct deserialization
        // This is a placeholder - actual implementation depends on chosen YAML parser

        // Option 1: knuffel (declarative, derive macros)
        knuffel::decode(doc).context("YAML deserialization failed")

        // Option 2: kaydle (serde-based)
        // kaydle::from_yaml_doc(doc).context("YAML deserialization failed")
    }

    /// Validate configuration after loading
    pub fn validate(&self) -> Result<()> {
        // Ensure ports don't conflict
        let mut ports = vec![
            self.otlp.grpc.port,
            self.otlp.http.port,
            self.query_api.port,
            self.management_api.port,
        ];
        ports.sort();
        ports.dedup();

        if ports.len() < 4 {
            anyhow::bail!("Port conflict detected in configuration");
        }

        // Validate storage backend
        match &self.storage.backend {
            StorageBackend::Local { path } => {
                if !path.exists() {
                    fs::create_dir_all(path)
                        .with_context(|| format!("Failed to create storage directory: {}", path.display()))?;
                }
            }
            StorageBackend::S3 { bucket, region } => {
                if bucket.is_empty() || region.is_empty() {
                    anyhow::bail!("S3 bucket and region must be specified");
                }
            }
            StorageBackend::MinIO { endpoint, bucket } => {
                if endpoint.is_empty() || bucket.is_empty() {
                    anyhow::bail!("MinIO endpoint and bucket must be specified");
                }
            }
        }

        // Validate retention periods
        if self.retention.traces_hours == 0 {
            anyhow::bail!("Traces retention must be > 0 hours");
        }

        Ok(())
    }
}
```

---

## Environment Variable Overrides

Configuration values can be overridden via environment variables using the `SEQUINS_` prefix:

```bash
# Storage backend
export SEQUINS_STORAGE_BACKEND=s3
export SEQUINS_STORAGE_BUCKET=my-bucket
export SEQUINS_STORAGE_REGION=us-west-2

# Retention
export SEQUINS_RETENTION_TRACES_HOURS=48
export SEQUINS_RETENTION_LOGS_HOURS=72

# Query API
export SEQUINS_QUERY_API_ENABLED=true
export SEQUINS_QUERY_API_PORT=9090

# Logging
export SEQUINS_LOGGING_LEVEL=debug
```

**Precedence:** Environment variables > Config file > Defaults

---

## Deployment Examples

### Development (Local Storage)

```yaml
// sequins.yaml
storage {
    backend "local"
    path "./dev-data"
}

data-lifecycle {
    hot-tier {
        duration-minutes 5
        max-memory-mb 128
    }

    flush-check-interval seconds=30

    retention {
        traces hours=1
        logs hours=1
        metrics hours=1
        profiles hours=1
        cleanup-interval seconds=60
    }
}

otlp {
    grpc {
        enabled true
        port 4317
    }
    http {
        enabled true
        port 4318
    }
}

query-api {
    enabled false
}

logging {
    level "debug"
    format "pretty"
}
```

### Production (S3 + Authentication)

```yaml
// /etc/sequins/config.yaml
storage {
    backend "s3"
    bucket "prod-telemetry"
    region "us-east-1"
}

data-lifecycle {
    hot-tier {
        duration-minutes 15
        max-memory-mb 2048
    }

    flush-check-interval seconds=60

    retention {
        traces hours=168      // 7 days
        logs hours=168
        metrics hours=720     // 30 days
        profiles hours=72     // 3 days
        cleanup-interval seconds=300
    }
}

otlp {
    grpc {
        enabled true
        port 4317
        max-connections 5000
    }
    http {
        enabled true
        port 4318
        max-body-size-mb 50
    }
}

query-api {
    enabled true
    port 8080

    cors {
        allowed-origins origin="https://sequins.company.com"
        allow-credentials true
    }

    auth {
        type "bearer"
        token-file "/etc/sequins/api-tokens.txt"
    }
}

management-api {
    enabled true
    port 8081

    auth {
        type "bearer"
        token-file "/etc/sequins/admin-tokens.txt"
    }
}

tls {
    enabled true
    cert-file "/etc/sequins/certs/server.crt"
    key-file "/etc/sequins/certs/server.key"
}

logging {
    level "info"
    format "json"

    file {
        enabled true
        path "/var/log/sequins/sequins.log"
        rotation-size-mb 100
        max-backups 30
    }
}

performance {
    worker-threads 16
    query-timeout-seconds 60
    max-concurrent-queries 500
}
```

---

## YAML Parsing Libraries

### Recommended: `serde_yaml`

```toml
[dependencies]
serde = { version = "1", features = ["derive"] }
serde_yaml = "0.9"
```

**Pros:**
- Uses standard serde traits (familiar API)
- Excellent compatibility with existing serde-based code
- Mature and well-maintained
- Industry standard approach

**Example:**
```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "kebab-case")]
struct Config {
    storage: StorageConfig,
    retention: RetentionConfig,
}

let config: Config = serde_yaml::from_str(&content)?;
```

---

## Testing

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_config() {
        let yaml = r#"
storage:
  backend: local
  path: /tmp/test
        "#;

        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(config.storage.backend, StorageBackend::Local { .. }));
    }

    #[test]
    fn test_parse_full_config() {
        let yaml = include_str!("../../../examples/config.yaml");
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        config.validate().unwrap();
    }

    #[test]
    fn test_invalid_config_fails() {
        let yaml = r#"
storage:
  backend: s3
  bucket: ""  # Empty bucket should fail validation
  region: us-east-1
        "#;

        let config: Config = serde_yaml::from_str(yaml).unwrap();
        assert!(config.validate().is_err());
    }
}
```

---

## Configuration Format Notes

YAML configuration files use standard YAML syntax:
- Comments start with `#`
- Nested structures use indentation (2 or 4 spaces)
- Lists use `-` prefix
- Key-value pairs use `: ` separator
- Strings can be quoted or unquoted (quotes required for special characters)

For reference examples, see the test cases in `crates/sequins-storage/src/config.rs`

---

## Related Documentation

- **[Deployment](deployment.md)** - How configuration differs between local and daemon modes
- **[Object Store Integration](object-store-integration.md)** - Storage backend configuration details
- **[Retention](retention.md)** - Retention policy settings
- **[Workspace & Crates](workspace-and-crates.md)** - Where configuration is used in the codebase

---

**Last Updated:** 2025-11-05
