# Distributed Multi-Node Scaling Strategy for Sequins

**Date:** 2025-01-05
**Status:** Architectural Design / Research Phase
**Target:** Enterprise deployment with horizontal scalability

---

## Executive Summary

This document outlines the architecture for scaling Sequins from a single-node embedded application to a distributed, multi-node cluster capable of handling high-volume OTLP ingestion and queries across multiple nodes. The design prioritizes **operational simplicity** for on-premises/self-hosted deployments while providing the scalability needed for enterprise observability workloads.

**Key Recommendation:** A **hybrid approach** with:
- **Shared object storage (S3/MinIO)** for persistence + local SSD caching
- **Homogeneous nodes** (no read/write specialization)
- **Replication factor of 1** with S3 write-through (simplest, most durable)
- **Time-based sharding** with consistent hashing for trace distribution
- **Kubernetes StatefulSet** with headless service for discovery
- **Gossip protocol** for membership (no external coordination service)
- **Scatter-gather pattern** for distributed queries

This design draws heavily from **Grafana Tempo's architecture** (object storage + local cache) while incorporating **ClickHouse's sharding patterns** for query distribution and **Loki's simplicity-first philosophy**.

---

## Table of Contents

1. [Replication Strategy](#1-replication-strategy)
2. [Storage Abstraction: Universal object_store](#2-storage-abstraction-universal-object_store)
3. [Node Specialization](#3-node-specialization)
4. [Data Distribution & Sharing](#4-data-distribution--sharing)
5. [Query Distribution](#5-query-distribution)
6. [Node Discovery & Membership](#6-node-discovery--membership)
7. [Storage Architecture](#7-storage-architecture)
8. [OTLP-Specific Optimizations](#8-otlp-specific-optimizations)
9. [Operational Concerns](#9-operational-concerns)
10. [Architecture Diagrams](#10-architecture-diagrams)
11. [Rust Implementation Sketch](#11-rust-implementation-sketch)
12. [Kubernetes Deployment](#12-kubernetes-deployment)
13. [Trade-Off Analysis](#13-trade-off-analysis)
14. [Implementation Roadmap](#14-implementation-roadmap)
15. [Success Metrics](#15-success-metrics)

---

## 1. Replication Strategy

### Decision: Replication Factor of 1 with S3 Write-Through

**Rationale:**

With S3 write-through, local replication is unnecessary. S3 becomes the source of truth, and local nodes are just performance caches.

**Specific Design:**

```
Replication Factor: 1 (no local replication)
Durability: S3 write-through (99.999999999% durability)
Failure Recovery: Pull from S3 on node restart or rebalancing
Hot Data: Last 1-2 hours on local SSD
Cold Data: Everything in S3
```

**Key Benefits:**

1. **Simplicity:** No replication coordination needed
2. **Cost-effective:** 1x storage cost (vs 2x for RF=2)
3. **S3 durability:** 11 nines durability guarantee
4. **Flexible recovery:** Node failure? Pull from S3. Rebalancing? Pull from S3.
5. **Operational ease:** No complex replication state to manage

**Recovery Scenarios:**

**Node goes down and restarts:**
```
1. Node starts up
2. Queries S3 for last 1-2 hour partitions (recent hot data)
3. Downloads and caches locally
4. Ready to serve queries (~30 seconds recovery)
```

**Cluster scaling (up or down):**
```
1. New shard assignments calculated
2. Nodes try to shift data peer-to-peer (fast path)
3. If source node unavailable, pull from S3 (fallback)
4. No data loss possible - S3 has everything
```

**Why not RF=2 or RF=3?**
- S3 already provides 11 nines durability
- Local replication adds complexity (coordination, consistency, state)
- Local replication doubles storage cost (RF=2) or triples it (RF=3)
- For observability data, S3 recovery is fast enough (< 1 minute)
- Peer-to-peer data shifting optimizes the common case

**Why this works for observability:**
- **Ephemeral data:** Short retention (days/weeks), not years
- **Acceptable brief unavailability:** 30-60s recovery on node failure is fine
- **Write-heavy workload:** 10:1+ write:read ratio
- **Query patterns:** Most queries hit recent data (already cached locally)

---

## 2. Storage Abstraction: Universal `object_store`

### Decision: Use `object_store` Crate for All Blob Storage

**Rationale:**

Instead of maintaining separate code paths for local file I/O and cloud storage, we use the `object_store` crate universally for all Parquet batch storage. This provides a single, clean API that works identically whether data is stored locally or in the cloud.

**Architecture:**

```rust
use object_store::{ObjectStore, local::LocalFileSystem, aws::AmazonS3Builder};

// Config-driven backend selection
let store: Arc<dyn ObjectStore> = match config.storage_backend {
    StorageBackend::Local { path } => {
        Arc::new(LocalFileSystem::new_with_prefix(path)?.with_automatic_cleanup())
    },
    StorageBackend::S3 { bucket, region } => {
        Arc::new(AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region(region)
            .build()?)
    },
    StorageBackend::MinIO { endpoint, bucket } => {
        Arc::new(AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .build()?)
    },
};
```

**Key Benefits:**

1. **Single Code Path:** Same storage code works for local and cloud deployments
2. **Production-Proven:** Used by InfluxDB IOx, DataFusion, crates.io
3. **Negligible Overhead:** LocalFileSystem adds ~95μs per operation (irrelevant for multi-MB batches)
4. **Config-Driven:** Switch backends via configuration file, not code changes
5. **Testing Simplicity:** Use `InMemoryStore` for unit tests, same code validates all backends

**Storage Tiers:**

```
Tier 1: HOT (In-Memory)
  └─> papaya::HashMap<TraceId, Trace> (lock-free)
      • No object_store - direct memory
      • Last 5-15 minutes
      • Async-friendly, no thread blocking

Tier 2: COLD (Parquet via object_store)
  └─> object_store::LocalFileSystem → /var/lib/sequins/traces/  (local)
  └─> object_store::AmazonS3 → s3://bucket/traces/              (cloud)
      • Same API for both!
      • Parquet + Zstd compression, bloom filters
      • Optional RocksDB indexes for high query volume
      • DataFusion SQL queries
```

**Example Usage:**

```rust
// Same code works for local and cloud!
pub async fn flush_batch_to_storage(
    store: &Arc<dyn ObjectStore>,
    batch: &ParquetBatch,
) -> Result<()> {
    let key = format!("traces/{}/{}.parquet.zst", batch.time_bucket, batch.id);
    store.put(&key.into(), batch.data.clone().into()).await?;
    Ok(())
}

pub async fn read_batch_from_storage(
    store: &Arc<dyn ObjectStore>,
    key: &str,
) -> Result<Vec<u8>> {
    let result = store.get(&key.into()).await?;
    Ok(result.bytes().await?.to_vec())
}
```

**Why Not Alternatives?**

- **Direct `std::fs` + separate S3 code:** Requires conditional logic, duplicate code paths, harder to test
- **`vfs` crate:** Not production-ready, poor semantic match for object storage, missing features
- **`s3s` / `rust-s3-server`:** Experimental or not embeddable, no production usage
- **Running MinIO per-node:** 15% CPU + 500MB-1GB memory overhead, operational complexity

**Performance Impact:**

- LocalFileSystem overhead: ~95 microseconds per operation
- Parquet batch size: ~10MB compressed
- Batch write frequency: Every 5 minutes
- **Verdict:** 95μs overhead is **completely negligible** for multi-millisecond I/O operations

**See Also:** [object-store-integration.md](object-store-integration.md) for detailed integration patterns.

---

## 3. Node Specialization

### Decision: Homogeneous Nodes (No Specialization)

**Rationale:**

The complexity cost of node specialization outweighs benefits for on-prem/self-hosted deployments.

**Homogeneous Architecture:**
```
All nodes can:
  - Ingest OTLP data (receive traces/logs/metrics)
  - Query local data
  - Coordinate scatter-gather queries
  - Cache hot data from object storage
  - Run retention cleanup
```

**Benefits:**
1. **Operational simplicity:** Single node type to deploy, monitor, scale
2. **Elastic scaling:** Add/remove nodes uniformly
3. **Failure resilience:** Any node can handle any request
4. **Load balancing:** Kubernetes Service distributes load automatically
5. **Resource efficiency:** No idle specialized nodes

**Why not read/write split (Elasticsearch-style)?**
- Adds operational complexity: Two node types, separate scaling decisions
- Requires query routing layer
- Benefits mainly at massive scale (1000+ nodes)
- Sequins's target: 3-20 nodes (reasonable for on-prem)

**Implementation:**
```yaml
# Single StatefulSet, all pods identical
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: sequins-node
spec:
  replicas: 5  # Scale up/down uniformly
  template:
    spec:
      containers:
      - name: sequins-daemon
        image: sequins-daemon:v1
        # All capabilities enabled on every node
```

---

## 4. Data Distribution & Sharing

### Decision: Hybrid Tiered Storage + Consistent Hashing

**Architecture:**

```
┌─────────────────────────────────────────────────────────────┐
│                        Data Flow                            │
└─────────────────────────────────────────────────────────────┘

OTLP Data → [Consistent Hash by TraceID] → Node Assignment
             ↓
        ┌────────────────────────────────────┐
        │  Node 1    Node 2    Node 3        │
        │  [Local]   [Local]   [Local]       │  ← Hot data
        │  [SSD]     [SSD]     [SSD]         │    (last 1-2h)
        └────────────┬───────────────────────┘
                     │ Write-through
                     ↓
              ┌──────────────┐
              │  Object Store│                    ← Cold storage
              │  (S3/MinIO)  │                      (all data)
              └──────────────┘
                     ↑
                     │ Read-through (cache miss)
        ┌────────────┴───────────────────────┐
        │  Node 4    Node 5    Node 6        │
        │  [Cache]   [Cache]   [Cache]       │  ← LRU cache
        └────────────────────────────────────┘
```

### Sharding Strategy

**Primary: Time-based Partitioning**
- Data naturally partitioned by time windows (1-hour chunks)
- Queries are always time-bounded (last 1h, last 24h)
- Retention cleanup is simple (delete old time partitions)

**Secondary: Trace ID Hashing**
- Within time window, distribute traces by consistent hash(trace_id)
- Ensures all spans for a trace land on same node
- Enables efficient trace reconstruction

**Implementation:**
```rust
// Shard assignment for incoming trace
fn assign_shard(trace_id: TraceId, timestamp: i64, num_shards: u32) -> u32 {
    let time_bucket = timestamp / (3600 * 1_000_000_000); // 1-hour buckets
    let hash = siphasher::hash(trace_id.as_bytes());
    (hash % num_shards) as u32
}
```

### Data Rebalancing

**On node failure:**
- Other nodes pull affected shards from S3
- Hot data (last 1-2 hours) downloaded immediately
- Older data fetched on-demand via cache misses

**On scaling up/down:**
- Calculate new shard assignments
- Nodes attempt peer-to-peer data transfer (fast path)
- Fallback to S3 if source node unavailable (always works)
- No data loss possible - S3 is source of truth

### Cluster Coordination: Gossip Protocol for Membership

**Use gossip (SWIM/Memberlist) for:**
- Cluster membership (which nodes are alive)
- Failure detection (< 1 second)
- Simple, eventually-consistent
- No external dependencies

**Optional: Raft for shard assignment coordination**
- Track which node owns which shards
- Coordinate shard reassignment on scaling
- Not required for MVP - can use simpler consistent hash-based assignment

```rust
// Membership using Rust memberlist crate
use memberlist::{Memberlist, NodeAddress};

// Each node runs gossip protocol
let memberlist = Memberlist::new(node_config).await?;

// Discover peers via K8s headless service DNS
let peers = resolve_peers("sequins-node.sequins.svc.cluster.local").await?;
for peer in peers {
    memberlist.join(peer).await?;
}

// Get live members for query routing
let members = memberlist.members();
```

---

## 5. Query Distribution

### Decision: Scatter-Gather with Time-Based Pruning

**Query Flow:**

```
┌──────────────┐
│ Query Request│
│ (last 1h)    │
└──────┬───────┘
       │
       ↓
┌──────────────────┐
│  Query Coordinator│  ← Any node can be coordinator
│  (Scatter Phase)  │
└──────┬───────────┘
       │
       ├──────────────────┬──────────────────┬──────────────────┐
       ↓                  ↓                  ↓                  ↓
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Node 1    │    │   Node 2    │    │   Node 3    │    │   Node 4    │
│ Query local │    │ Query local │    │ Query local │    │ Query local │
│   shards    │    │   shards    │    │   shards    │    │   shards    │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │                  │
       └──────────────────┴──────────────────┴──────────────────┘
                          │
                          ↓
                 ┌─────────────────┐
                 │ Gather & Merge  │
                 │ (Time-ordered)  │
                 └────────┬────────┘
                          ↓
                   ┌──────────────┐
                   │   Response   │
                   └──────────────┘
```

### Time-Based Pruning

```rust
// Coordinator knows which nodes have which time ranges
async fn query_traces(query: TraceQuery) -> Result<Vec<Trace>> {
    // 1. Determine which time buckets are needed
    let time_buckets = calculate_time_buckets(query.start_time, query.end_time);

    // 2. Determine which nodes own those buckets
    let target_nodes = shard_map.nodes_for_time_range(time_buckets);

    // 3. Scatter query only to relevant nodes (not all nodes)
    let futures = target_nodes.iter().map(|node| {
        node.query_local_traces(query.clone())
    });

    // 4. Gather results in parallel
    let results = futures::future::join_all(futures).await;

    // 5. Merge by timestamp (traces are already sorted per-node)
    let merged = merge_time_ordered_results(results);

    Ok(merged)
}

// Efficient merge of already-sorted results
fn merge_time_ordered_results(results: Vec<Vec<Trace>>) -> Vec<Trace> {
    // K-way merge using min-heap
    use std::collections::BinaryHeap;

    let mut heap = BinaryHeap::new();
    let mut iters: Vec<_> = results.into_iter()
        .map(|v| v.into_iter().peekable())
        .collect();

    // Initialize heap with first element from each result set
    for (idx, iter) in iters.iter_mut().enumerate() {
        if let Some(trace) = iter.peek() {
            heap.push((trace.start_time, idx));
        }
    }

    let mut merged = Vec::new();
    while let Some((_, idx)) = heap.pop() {
        if let Some(trace) = iters[idx].next() {
            merged.push(trace);
            if let Some(next_trace) = iters[idx].peek() {
                heap.push((next_trace.start_time, idx));
            }
        }
    }

    merged
}
```

### Query Optimization Techniques

1. **Time-based shard pruning:** Only query nodes with relevant time ranges
2. **Result streaming:** Start returning results before all nodes finish
3. **Caching:** Cache recent queries (last 5m, last 15m) at coordinator
4. **Parallel execution:** All node queries run concurrently
5. **Timeout handling:** Fast-fail slow nodes, return partial results

### Trace ID Lookup Optimization

For queries by specific trace ID (most common):
```rust
// Direct lookup - no scatter needed
async fn get_trace_by_id(trace_id: TraceId) -> Result<Trace> {
    // 1. Hash trace_id to find owning node
    let shard = hash(trace_id) % num_shards;
    let node = shard_map.node_for_shard(shard);

    // 2. Direct query to that node only
    node.get_trace(trace_id).await
}
```

---

## 6. Node Discovery & Membership

### Decision: Kubernetes-Native Discovery + Gossip Protocol

**Architecture:**

```
┌─────────────────────────────────────────────────────────────┐
│                     Kubernetes Discovery                     │
└─────────────────────────────────────────────────────────────┘
                              │
               ┌──────────────┴──────────────┐
               │  Headless Service DNS       │
               │  sequins-node.sequins.svc   │
               └──────────────┬──────────────┘
                              │
         ┌────────────────────┼────────────────────┐
         │                    │                    │
    ┌────▼────┐         ┌────▼────┐         ┌────▼────┐
    │ node-0  │◄───────►│ node-1  │◄───────►│ node-2  │
    │         │  Gossip │         │  Gossip │         │
    └─────────┘         └─────────┘         └─────────┘
         │                    │                    │
         └────────────────────┴────────────────────┘
                         │
                    ┌────▼─────┐
                    │ S3/MinIO │
                    └──────────┘
```

### Kubernetes StatefulSet Configuration

```yaml
apiVersion: v1
kind: Service
metadata:
  name: sequins-node
  namespace: sequins
spec:
  clusterIP: None  # Headless service
  selector:
    app: sequins-node
  ports:
  - name: otlp-grpc
    port: 4317
  - name: otlp-http
    port: 4318
  - name: query-api
    port: 8080
  - name: gossip
    port: 7946

---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: sequins-node
  namespace: sequins
spec:
  serviceName: sequins-node
  replicas: 5
  selector:
    matchLabels:
      app: sequins-node
  template:
    metadata:
      labels:
        app: sequins-node
    spec:
      containers:
      - name: sequins-daemon
        image: sequins-daemon:v1.0.0
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
        - name: CLUSTER_MEMBERS
          value: "sequins-node.sequins.svc.cluster.local"
        ports:
        - containerPort: 4317
          name: otlp-grpc
        - containerPort: 4318
          name: otlp-http
        - containerPort: 8080
          name: query-api
        - containerPort: 7946
          name: gossip
        volumeMounts:
        - name: cache
          mountPath: /var/cache/sequins
        - name: config
          mountPath: /etc/sequins
  volumeClaimTemplates:
  - metadata:
      name: cache
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: fast-ssd
      resources:
        requests:
          storage: 100Gi  # Local cache size
```

### Discovery Implementation (Rust)

```rust
// sequins-daemon/src/discovery.rs
use memberlist::{Memberlist, NodeMeta};
use trust_dns_resolver::TokioAsyncResolver;

pub struct ClusterDiscovery {
    memberlist: Memberlist,
    dns_resolver: TokioAsyncResolver,
}

impl ClusterDiscovery {
    pub async fn new(config: ClusterConfig) -> Result<Self> {
        // Create memberlist for gossip
        let memberlist = Memberlist::new(memberlist::Config {
            node_name: config.pod_name.clone(),
            bind_addr: config.gossip_addr,
            ..Default::default()
        }).await?;

        // Resolve peers via K8s headless service
        let dns_resolver = TokioAsyncResolver::tokio_from_system_conf()?;

        Ok(Self { memberlist, dns_resolver })
    }

    pub async fn join_cluster(&self, service_name: &str) -> Result<()> {
        // DNS lookup for headless service returns all pod IPs
        let peers = self.dns_resolver
            .lookup_ip(service_name)
            .await?;

        // Join gossip cluster
        for peer in peers {
            if let Err(e) = self.memberlist.join((peer, 7946)).await {
                tracing::warn!("Failed to join peer {}: {}", peer, e);
            }
        }

        Ok(())
    }

    pub fn live_members(&self) -> Vec<NodeInfo> {
        self.memberlist.members()
            .iter()
            .filter(|m| m.state.is_alive())
            .map(|m| NodeInfo {
                name: m.name.clone(),
                addr: m.addr,
            })
            .collect()
    }
}
```

### Why this approach?

1. **Kubernetes-native:** Uses built-in DNS service discovery
2. **No external dependencies:** No need for etcd, Consul, or ZooKeeper
3. **Automatic:** Pods discover each other on startup
4. **Health-aware:** Gossip protocol detects failures quickly (< 1s)
5. **Scalable:** StatefulSet provides stable network identities

**Alternative Considered: etcd for coordination**
- Pros: Strong consistency, battle-tested
- Cons: Additional operational burden, etcd cluster to manage
- **Rejected:** Gossip + K8s DNS is simpler and sufficient

---

## 7. Storage Architecture

### Decision: Hybrid Tiered Storage (Hot Local, Cold S3)

**Detailed Architecture:**

```
┌─────────────────────────────────────────────────────────────┐
│                      Storage Tiers                          │
└─────────────────────────────────────────────────────────────┘

Tier 1: HOT (In-Memory)
┌─────────────────────────────────┐
│  Recent data (last 5-15 minutes)│  ← Write buffer
│  HashMap<TraceId, Trace>        │    Not yet flushed
│  ~100MB - 1GB RAM per node      │
└────────────┬────────────────────┘
             │ Flush every 5m
             ↓
Tier 2: WARM (Local SSD)
┌─────────────────────────────────┐
│  Recent data (last 1-2 hours)   │  ← libSQL database
│  Local libSQL database          │    Queryable immediately
│  50-100GB per node              │    Read-optimized
└────────────┬────────────────────┘
             │ Upload to S3 in background
             ↓
Tier 3: COLD (Object Storage)
┌─────────────────────────────────┐
│  All data (full retention)      │  ← S3/MinIO
│  Parquet files, compressed      │    Durability: 11 nines
│  Cost: $0.023/GB/month          │    Infinite scalability
└─────────────────────────────────┘
             ↑
             │ Read-through on cache miss
┌────────────┴────────────────────┐
│  LRU Cache (Local SSD)          │  ← Automatic caching
│  Most recently queried data     │    Transparent to queries
│  20-30GB per node               │
└─────────────────────────────────┘
```

### Implementation

```rust
// sequins-storage/src/tiered_storage.rs
pub struct TieredStorage {
    // Tier 1: In-memory write buffer
    write_buffer: Arc<RwLock<HashMap<TraceId, Trace>>>,

    // Tier 2: Local database
    local_storage: Arc<TieredStorage>,

    // Tier 3: Object storage
    object_store: Arc<dyn ObjectStore>, // S3/MinIO/filesystem

    // LRU cache for hot data
    cache: Arc<Mutex<LruCache<String, Vec<u8>>>>,
}

impl TieredStorage {
    pub async fn write_trace(&self, trace: Trace) -> Result<()> {
        // 1. Write to memory buffer (immediate)
        {
            let mut buffer = self.write_buffer.write().unwrap();
            buffer.insert(trace.trace_id, trace.clone());
        }

        // 2. Asynchronously flush to local DB (background)
        if buffer.len() > 1000 {
            tokio::spawn({
                let local_db = self.local_db.clone();
                let buffer = self.write_buffer.clone();
                async move {
                    let traces = {
                        let mut buf = buffer.write().unwrap();
                        std::mem::take(&mut *buf)
                    };
                    local_db.insert_traces(traces.values()).await?;
                    Ok::<_, Error>(())
                }
            });
        }

        Ok(())
    }

    pub async fn read_trace(&self, trace_id: TraceId) -> Result<Option<Trace>> {
        // 1. Check memory buffer first
        {
            let buffer = self.write_buffer.read().unwrap();
            if let Some(trace) = buffer.get(&trace_id) {
                return Ok(Some(trace.clone()));
            }
        }

        // 2. Check local DB (warm tier)
        if let Some(trace) = self.local_db.get_trace(trace_id).await? {
            return Ok(Some(trace));
        }

        // 3. Check object storage (cold tier)
        let object_key = format!("traces/{}/{}.parquet",
            time_bucket(trace_id),
            trace_id
        );

        // Check cache first
        if let Some(cached) = self.cache.lock().unwrap().get(&object_key) {
            return Ok(Some(deserialize_trace(cached)?));
        }

        // Fetch from S3 and cache
        if let Some(data) = self.object_store.get(&object_key).await? {
            self.cache.lock().unwrap().put(object_key.clone(), data.clone());
            return Ok(Some(deserialize_trace(&data)?));
        }

        Ok(None)
    }
}
```

### Storage Format: Parquet

Use Apache Parquet for S3 storage:
- Columnar format → excellent compression (5-10x)
- Built-in schema evolution
- Efficient for analytical queries
- Standard format with Rust support (parquet crate)

### Object Storage Structure

```
s3://sequins-data/
├── traces/
│   ├── 2025-01-15-14/  ← Hour buckets
│   │   ├── batch-001.parquet
│   │   ├── batch-002.parquet
│   │   └── batch-003.parquet
│   ├── 2025-01-15-15/
│   └── 2025-01-15-16/
├── logs/
│   └── 2025-01-15-14/
│       └── batch-001.parquet
├── metrics/
│   └── 2025-01-15-14/
│       └── batch-001.parquet
└── metadata/
    └── shard-assignments.json
```

### Why Hybrid (Not Alternatives)?

| Requirement | Shared S3 Only | Local Only | Hybrid |
|------------|----------------|------------|--------|
| Query latency (hot) | ❌ High (500ms) | ✅ Low (5ms) | ✅ Low (5ms) |
| Query latency (cold) | ❌ High (500ms) | N/A | ⚠️ Medium (100ms) |
| Storage cost | ✅ Low | ❌ High | ⚠️ Medium |
| Durability | ✅ 11 nines | ⚠️ RAID only | ✅ 11 nines |
| Node failure recovery | ✅ Fast | ❌ Slow | ✅ Fast |
| Scalability | ✅ Infinite | ❌ Limited | ✅ Infinite |
| Operational simplicity | ✅ Simple | ⚠️ Medium | ⚠️ Medium |

### Rust Libraries

- **object_store** crate: Unified API for S3, MinIO, Azure, GCS, local filesystem
- **parquet** crate: Write/read Parquet files
- **lru** crate: LRU cache implementation

---

## 8. OTLP-Specific Optimizations

### Time-Based Partitioning (Natural for Telemetry)

Observability data is inherently time-ordered:

```rust
// Partition structure
pub struct TimePartition {
    start_time: i64,      // Hour boundary (nanoseconds)
    end_time: i64,
    shard_id: u32,
    traces: Vec<TraceId>, // Traces in this partition
}

// Queries always bounded by time
pub struct TraceQuery {
    start_time: i64,      // Required
    end_time: i64,        // Required
    service: Option<String>,
    status: Option<TraceStatus>,
    limit: usize,
}
```

**Benefits:**
- Retention cleanup is trivial: Delete old partitions
- Queries never scan full dataset
- Natural alignment with human query patterns ("last hour", "last 24 hours")

### Trace ID Based Sharding

Keep all spans of a trace together:

```rust
fn route_trace(trace_id: TraceId, num_shards: u32) -> u32 {
    // Consistent hashing ensures same trace always goes to same shard
    let hash = siphasher::hash(&trace_id.to_bytes());
    (hash % num_shards) as u32
}
```

**Benefits:**
- Single-node trace reconstruction (no cross-node joins)
- Efficient span parent-child relationship building
- Reduced network traffic for trace queries

### Columnar Storage for Metrics

Metrics have different query patterns than traces:

```rust
// Store metrics in columnar format
pub struct MetricColumn {
    metric_name: String,
    timestamps: Vec<i64>,
    values: Vec<f64>,
    attributes: Vec<HashMap<String, String>>,
}
```

**Benefits:**
- Excellent compression (timestamps compress 10:1, values 2-5:1)
- Fast aggregation queries (sum, avg, p99)
- Efficient time-range scans

### Compression Strategies

Different data types need different compression:

| Data Type | Compression | Ratio | Rust Crate |
|-----------|-------------|-------|-----------|
| Trace spans | Snappy | 3-4x | snap |
| Logs | Zstd | 5-10x | zstd |
| Metrics | Parquet native | 10-20x | parquet |
| Profiles (pprof) | gzip | 3-5x | flate2 |

---

## 9. Operational Concerns

### Node Addition/Removal

**Adding a Node:**

```yaml
# Scale up StatefulSet
kubectl scale statefulset sequins-node --replicas=6

# New node will:
# 1. Join gossip cluster via DNS discovery
# 2. Receive shard assignments from coordinator
# 3. Start receiving new data immediately
# 4. Optionally: Rebalance existing shards (background)
```

**Implementation:**
```rust
async fn handle_new_node(new_node: NodeInfo, shard_map: &mut ShardMap) {
    // Add node to cluster
    shard_map.add_node(new_node.clone());

    // Optional: Rebalance shards
    if shard_map.imbalance_threshold_exceeded() {
        rebalance_shards(shard_map).await?;
    }

    // New node immediately starts receiving new data
    // Old data remains on existing nodes (no migration required with S3 backing)
}
```

**Removing a Node:**

```rust
async fn handle_node_removal(node_id: NodeId, shard_map: &mut ShardMap) {
    // Mark shards as unassigned
    let orphaned_shards = shard_map.remove_node(node_id);

    // Reassign to surviving nodes
    for shard in orphaned_shards {
        let new_owner = shard_map.least_loaded_node();
        shard_map.assign_shard(shard, new_owner);
    }

    // No data loss: All data backed by S3
    // New owners will fetch data from S3 on demand
}
```

### Data Rebalancing

**Strategy: Lazy Rebalancing**

Don't immediately move data when nodes change:

1. **Write path:** New data uses updated shard assignments
2. **Read path:** Queries check both old and new owners during transition
3. **Background:** Slowly migrate data from overloaded nodes

```rust
async fn background_rebalancer(shard_map: Arc<RwLock<ShardMap>>) {
    let mut interval = tokio::time::interval(Duration::from_secs(3600)); // Hourly

    loop {
        interval.tick().await;

        let imbalance = {
            let map = shard_map.read().unwrap();
            map.calculate_imbalance()
        };

        if imbalance > 0.2 { // 20% imbalance threshold
            // Move one shard from most-loaded to least-loaded node
            rebalance_one_shard(&shard_map).await?;
        }
    }
}
```

### Backup and Disaster Recovery

**Backup Strategy:**

Since all data is in S3/MinIO:
- **Backup = S3 replication** (cross-region if needed)
- No separate backup mechanism required
- S3 versioning for point-in-time recovery

**Disaster Recovery:**

```bash
# Complete cluster failure scenario

# 1. Deploy new Sequins cluster
kubectl apply -f sequins-statefulset.yaml

# 2. Point to existing S3 bucket (no data copy needed)
# Config: s3_bucket=s3://sequins-data

# 3. Nodes start up, rebuild metadata from S3
# - Scan S3 prefixes to discover time partitions
# - Rebuild shard assignment map
# - Ready to serve queries in ~5 minutes

# 4. Resume OTLP ingestion
# New data flows immediately, no backfill needed
```

**Recovery Time Objective (RTO):** < 10 minutes
**Recovery Point Objective (RPO):** < 5 minutes (in-memory buffer loss only)

### Monitoring the Observability System

**Key Metrics:**

```rust
// Expose Prometheus metrics from each node
pub struct NodeMetrics {
    // Ingestion
    otlp_requests_total: Counter,
    otlp_traces_ingested: Counter,
    otlp_latency_seconds: Histogram,

    // Storage
    local_db_size_bytes: Gauge,
    s3_uploads_total: Counter,
    s3_upload_errors: Counter,
    cache_hit_ratio: Gauge,

    // Queries
    queries_total: Counter,
    query_latency_seconds: Histogram,
    scatter_gather_nodes: Histogram,

    // Cluster
    cluster_size: Gauge,
    shards_assigned: Gauge,
    gossip_messages_total: Counter,
}
```

**Health Checks:**

```rust
// Kubernetes readiness probe
#[get("/health/ready")]
async fn readiness() -> &'static str {
    // Check:
    // - Gossip cluster joined
    // - Local DB responsive
    // - S3 accessible
    "ok"
}

// Kubernetes liveness probe
#[get("/health/alive")]
async fn liveness() -> &'static str {
    // Basic process health check
    "ok"
}
```

---

## 10. Architecture Diagrams

### High-Level System Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                           Kubernetes Cluster                                 │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                      Ingress / Load Balancer                          │  │
│  │              (OTLP: 4317/4318, Query: 8080)                           │  │
│  └────────────────────────────┬──────────────────────────────────────────┘  │
│                               │                                              │
│  ┌────────────────────────────▼──────────────────────────────────────────┐  │
│  │                    Kubernetes Service (ClusterIP)                     │  │
│  │                  Load balances across all nodes                       │  │
│  └────────┬───────────────────┬───────────────────┬──────────────────────┘  │
│           │                   │                   │                          │
│  ┌────────▼────────┐  ┌───────▼────────┐  ┌──────▼────────┐               │
│  │  sequins-node-0 │  │ sequins-node-1 │  │ sequins-node-2│  ...          │
│  │  ┌────────────┐ │  │  ┌───────────┐ │  │  ┌──────────┐│               │
│  │  │OTLP Server │ │  │  │OTLP Server│ │  │  │OTLP Servr││               │
│  │  ├────────────┤ │  │  ├───────────┤ │  │  ├──────────┤│               │
│  │  │Query API   │ │  │  │Query API  │ │  │  │Query API ││               │
│  │  ├────────────┤ │  │  ├───────────┤ │  │  ├──────────┤│               │
│  │  │TieredStore │ │  │  │TieredStore│ │  │  │TierdStore││               │
│  │  │ -Memory    │ │  │  │ -Memory   │ │  │  │ -Memory  ││               │
│  │  │ -Local DB  │ │  │  │ -Local DB │ │  │  │ -Local DB││               │
│  │  │ -S3 Cache  │ │  │  │ -S3 Cache │ │  │  │ -S3 Cache││               │
│  │  └─────┬──────┘ │  │  └─────┬─────┘ │  │  └─────┬────┘│               │
│  └────────┼────────┘  └────────┼────────┘  └────────┼─────┘               │
│           │                    │                     │                       │
│           │    ┌───────────────┴─────────────────────┘                      │
│           │    │          Gossip Protocol (SWIM)                            │
│           │    │     7946 ◄──────────────────────► 7946                     │
│           │    │                                                             │
│  ┌────────▼────▼────────────────────────────────────────────────────────┐  │
│  │                   PersistentVolumeClaim (Local SSD)                   │  │
│  │                        100GB per node                                 │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└──────────────────────────────────────────┬───────────────────────────────────┘
                                           │
                        ┌──────────────────▼────────────────────┐
                        │     Object Storage (S3/MinIO)         │
                        │                                        │
                        │  s3://sequins-data/                   │
                        │    ├── traces/                        │
                        │    ├── logs/                          │
                        │    ├── metrics/                       │
                        │    └── metadata/                      │
                        │                                        │
                        │  Durability: 11 nines                 │
                        │  Cost: $0.023/GB/month                │
                        └────────────────────────────────────────┘
```

### Write Path (OTLP Ingestion)

```
┌──────────────┐
│ Application  │
│ (OTLP Client)│
└──────┬───────┘
       │ OTLP/gRPC or OTLP/HTTP
       ↓
┌──────────────────────────────┐
│ K8s Load Balancer            │  ← Round-robin across nodes
└──────┬───────────────────────┘
       │
       ↓
┌──────────────────────────────┐
│ sequins-node-2               │  ← Randomly selected node
│ ┌──────────────────────────┐ │
│ │ 1. Parse OTLP protobuf   │ │
│ │ 2. Extract trace_id      │ │
│ │ 3. Hash to shard         │ │
│ │    shard = hash % N      │ │
│ └──────────┬───────────────┘ │
└────────────┼─────────────────┘
             │
    ┌────────┴────────┐
    │ Shard routing   │
    ├─────────────────┤
    │ Shard 5 → node-1│
    │ Shard 7 → node-2│ ← This trace goes to shard 7
    └────────┬────────┘
             │
             ↓
┌──────────────────────────────┐
│ sequins-node-2 (Owner)       │
│ ┌──────────────────────────┐ │
│ │ 4. Write to memory buffer│ │  ← Immediate (μs latency)
│ │    HashMap<TraceId, Tr>  │ │
│ └──────────┬───────────────┘ │
│            │                  │
│ ┌──────────▼───────────────┐ │
│ │ 5. Async flush to local  │ │  ← Background (5s-5m delay)
│ │    libSQL database       │ │
│ └──────────┬───────────────┘ │
│            │                  │
│ ┌──────────▼───────────────┐ │
│ │ 6. Upload to S3 (batch)  │ │  ← Background (5m delay)
│ │    Parquet format        │ │    Write-through to S3
│ │    (source of truth)     │ │    (no replication needed)
│ └──────────────────────────┘ │
└────────────┼─────────────────┘
             │
             ↓
      ┌──────────────┐
      │  S3 Bucket   │  ← Durable storage (11 nines)
      └──────────────┘    Source of truth

Write Latency Breakdown:
- OTLP endpoint → Memory: 100μs - 1ms
- Memory → Local DB: 5s - 5m (async)
- Local DB → S3: 5m - 15m (async, write-through)
- Total perceived latency: < 5ms
- Durability: Guaranteed by S3 (no local replication)
```

### Query Path (Distributed Query)

```
┌──────────────┐
│ Sequins App  │
│ (UI Client)  │
└──────┬───────┘
       │ HTTP: GET /api/traces?start=...&end=...
       ↓
┌──────────────────────────────┐
│ Any Node (Coordinator)       │  ← Query hits any node
│ ┌──────────────────────────┐ │
│ │ 1. Parse query params    │ │
│ │ 2. Determine time range  │ │
│ │    Buckets: 14:00-15:00  │ │
│ │ 3. Find owning nodes     │ │
│ │    → node-1, node-2, -3  │ │
│ └──────────┬───────────────┘ │
└────────────┼─────────────────┘
             │
    ┌────────┴────────┬────────────────┐
    │ Scatter Phase   │                │
    ↓                 ↓                ↓
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│ node-1      │  │ node-2      │  │ node-3      │
│ Query local │  │ Query local │  │ Query local │
│ - Memory    │  │ - Memory    │  │ - Memory    │
│ - Local DB  │  │ - Local DB  │  │ - Local DB  │
│ - S3 cache  │  │ - S3 cache  │  │ - S3 cache  │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       │ Partial Results (sorted by time)│
       └────────┬───────┴────────────────┘
                │ Gather Phase
                ↓
┌──────────────────────────────┐
│ Coordinator Node             │
│ ┌──────────────────────────┐ │
│ │ 4. K-way merge           │ │  ← Merge pre-sorted results
│ │    (by timestamp)        │ │
│ │ 5. Apply limit           │ │
│ │ 6. Return to client      │ │
│ └──────────────────────────┘ │
└────────────┬─────────────────┘
             │
             ↓
      ┌──────────────┐
      │  JSON Result │
      │  [traces]    │
      └──────────────┘

Query Latency Breakdown (p50/p95):
- Hot data (memory/local): 5ms / 20ms
- Warm data (S3 cache): 50ms / 100ms
- Cold data (S3 fetch): 200ms / 500ms
- Scatter-gather overhead: 10ms / 30ms
- Total query: 15-70ms / 50-530ms
```

---

## 11. Rust Implementation Sketch

### Crate Structure

```
sequins/
├── crates/
│   ├── sequins-core/           # Existing (no changes)
│   ├── sequins-storage/        # Enhanced with distributed support
│   │   ├── tiered.rs          # TieredStorage (existing)
│   │   ├── tiered.rs          # NEW: TieredStorage
│   │   ├── object_store.rs    # NEW: S3/MinIO integration
│   │   └── cache.rs           # NEW: LRU cache
│   ├── sequins-server/         # Enhanced with clustering
│   │   ├── cluster.rs         # NEW: Cluster coordination
│   │   ├── discovery.rs       # NEW: K8s + gossip discovery
│   │   ├── shard_map.rs       # NEW: Shard assignment
│   │   └── query_router.rs    # NEW: Scatter-gather
│   ├── sequins-daemon/         # Enhanced for clustering
│   │   └── config.rs          # NEW: Cluster config
│   └── sequins-operator/       # NEW: Kubernetes operator (optional)
│       └── crd.rs             # Custom Resource Definitions
```

### Key Trait Additions

```rust
// sequins-storage/src/traits/distributed.rs

use opentelemetry::trace::TraceId;

/// Trait for distributed storage coordination
#[async_trait]
pub trait DistributedStorage: OtlpIngest + QueryApi + ManagementApi {
    /// Get cluster membership info
    async fn cluster_members(&self) -> Result<Vec<NodeInfo>>;

    /// Get shard assignment for a trace
    fn trace_shard(&self, trace_id: TraceId) -> u32;

    /// Get node responsible for a shard
    async fn shard_owner(&self, shard_id: u32) -> Result<NodeInfo>;

    /// Query a specific node
    async fn query_node(&self, node: NodeInfo, query: TraceQuery) -> Result<Vec<Trace>>;
}

pub struct NodeInfo {
    pub id: String,
    pub addr: SocketAddr,
    pub shards: Vec<u32>,
    pub state: NodeState,
}

pub enum NodeState {
    Alive,
    Suspect,
    Dead,
}
```

### Distributed Query Implementation

```rust
// sequins-server/src/query_router.rs

use sequins_storage::{QueryApi, DistributedStorage};
use futures::stream::{self, StreamExt};

pub struct DistributedQueryRouter<S: DistributedStorage> {
    storage: Arc<S>,
    cluster: Arc<ClusterDiscovery>,
}

impl<S: DistributedStorage> DistributedQueryRouter<S> {
    pub async fn scatter_gather_traces(
        &self,
        query: TraceQuery,
    ) -> Result<Vec<Trace>> {
        // 1. Determine which nodes to query based on time range
        let time_buckets = self.calculate_time_buckets(
            query.start_time,
            query.end_time,
        );

        let target_nodes = self.cluster
            .nodes_for_time_buckets(&time_buckets)
            .await?;

        tracing::debug!("Scatter query to {} nodes", target_nodes.len());

        // 2. Scatter: Query all nodes in parallel
        let query_futures = target_nodes.iter().map(|node| {
            let query = query.clone();
            let storage = self.storage.clone();
            let node = node.clone();

            async move {
                // Set timeout for each node query
                tokio::time::timeout(
                    Duration::from_secs(5),
                    storage.query_node(node.clone(), query),
                )
                .await
                .map_err(|_| Error::Timeout)?
            }
        });

        let results = stream::iter(query_futures)
            .buffer_unordered(10) // Concurrent queries
            .collect::<Vec<_>>()
            .await;

        // 3. Gather: Collect successful results
        let mut all_traces = Vec::new();
        for result in results {
            match result {
                Ok(traces) => all_traces.extend(traces),
                Err(e) => {
                    tracing::warn!("Node query failed: {}", e);
                    // Continue with partial results
                }
            }
        }

        // 4. Merge: K-way merge by timestamp
        all_traces.sort_by_key(|t| t.start_time);

        // 5. Apply limit
        all_traces.truncate(query.limit);

        Ok(all_traces)
    }
}
```

### Object Storage Integration

```rust
// sequins-storage/src/object_store.rs

use object_store::{ObjectStore, aws::AmazonS3Builder};
use parquet::arrow::ArrowWriter;

pub struct S3Storage {
    store: Arc<dyn ObjectStore>,
    bucket: String,
}

impl S3Storage {
    pub fn new(endpoint: &str, bucket: &str) -> Result<Self> {
        let store = AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_endpoint(endpoint)
            .build()?;

        Ok(Self {
            store: Arc::new(store),
            bucket: bucket.to_string(),
        })
    }

    pub async fn write_traces_batch(
        &self,
        traces: &[Trace],
        time_bucket: i64,
    ) -> Result<()> {
        // Convert to Parquet
        let schema = trace_arrow_schema();
        let batch = traces_to_record_batch(traces)?;

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;

        // Compress
        let compressed = zstd::encode_all(&buffer[..], 3)?;

        // Upload to S3
        let key = format!("traces/{}/{}.parquet.zst",
            time_bucket,
            Uuid::new_v4()
        );

        self.store.put(&key.into(), compressed.into()).await?;

        Ok(())
    }

    pub async fn read_traces(
        &self,
        time_bucket: i64,
    ) -> Result<Vec<Trace>> {
        // List all files in time bucket
        let prefix = format!("traces/{}/", time_bucket);
        let files = self.store.list(Some(&prefix.into())).await?;

        let mut all_traces = Vec::new();

        for file in files {
            // Download and decompress
            let compressed = self.store.get(&file.location).await?.bytes().await?;
            let decompressed = zstd::decode_all(&compressed[..])?;

            // Parse Parquet
            let traces = parquet_to_traces(&decompressed)?;
            all_traces.extend(traces);
        }

        Ok(all_traces)
    }
}
```

---

## 12. Kubernetes Deployment

### Helm Chart Values

```yaml
# values.yaml

# Sequins Cluster Configuration
replicaCount: 5

image:
  repository: sequins/daemon
  tag: "v1.0.0"
  pullPolicy: IfNotPresent

# Storage configuration
storage:
  # Local cache size per node
  cacheSize: 100Gi
  storageClass: fast-ssd

  # Object storage (S3/MinIO)
  objectStore:
    type: s3  # or minio
    endpoint: https://s3.us-east-1.amazonaws.com
    bucket: sequins-data
    region: us-east-1
    # Access keys in secret

# Retention policy
retention:
  traces: 168h    # 7 days
  logs: 168h      # 7 days
  metrics: 720h   # 30 days
  profiles: 168h  # 7 days

# Clustering
cluster:
  gossipPort: 7946
  # No replication - RF=1 with S3 write-through

# OTLP endpoints
otlp:
  grpcPort: 4317
  httpPort: 4318

# Query API
queryApi:
  port: 8080
  maxConcurrentQueries: 100

# Resource limits
resources:
  requests:
    memory: "4Gi"
    cpu: "2"
  limits:
    memory: "8Gi"
    cpu: "4"

# Monitoring
monitoring:
  enabled: true
  prometheus:
    port: 9090
    path: /metrics
```

### Deployment Commands

```bash
# Install Sequins cluster
helm install sequins ./sequins-helm \
  --namespace sequins \
  --create-namespace \
  --set storage.objectStore.endpoint=https://minio.example.com \
  --set storage.objectStore.bucket=sequins-prod \
  --set-file storage.objectStore.accessKey=./s3-key.txt \
  --set-file storage.objectStore.secretKey=./s3-secret.txt

# Scale up
helm upgrade sequins ./sequins-helm \
  --set replicaCount=10 \
  --reuse-values

# Scale down
helm upgrade sequins ./sequins-helm \
  --set replicaCount=3 \
  --reuse-values

# Update configuration
helm upgrade sequins ./sequins-helm \
  --set retention.traces=336h \
  --reuse-values
```

---

## 13. Trade-Off Analysis

### Key Decisions Summary

| Decision | Choice | Alternative | Rationale |
|----------|--------|-------------|-----------|
| **Replication** | RF=1 + S3 | RF=2 or RF=3 | S3 durability (11 nines) eliminates need for local replication |
| **Node Type** | Homogeneous | Read/Write split | Operational simplicity for on-prem deployments |
| **Storage** | Hybrid (SSD + S3) | S3-only or SSD-only | Balance query latency, cost, and durability |
| **Sharding** | Time + TraceID hash | Random or Round-robin | Natural for time-series, efficient trace reconstruction |
| **Consensus** | Gossip (memberlist) | Raft | Simpler for membership, eventual consistency sufficient |
| **Discovery** | K8s DNS + Gossip | etcd/Consul | No external dependencies, K8s-native |
| **Query** | Scatter-gather | Centralized coordinator | Fault-tolerant, no single point of failure |
| **Data Format** | Parquet + Zstd | JSON or Protobuf | Excellent compression, columnar efficiency |

### Complexity vs Simplicity Trade-offs

**Added Complexity:**
- Shard assignment logic
- Scatter-gather query coordination
- S3 integration and caching
- Gossip protocol membership
- Data rebalancing on scaling events

**Simplicity Preserved:**
- No external coordination service (no etcd)
- Homogeneous nodes (single deployment type)
- Kubernetes-native (standard StatefulSet)
- Standard object storage (S3 API)
- No custom network protocols

**Verdict:** Reasonable complexity increase for significant scalability gains.

### Cost Analysis (Example: 5-node cluster)

**Storage Costs (per month):**
```
Local SSD Cache:
  5 nodes × 100GB × $0.17/GB = $85/month

S3 Storage (hot data):
  10TB × $0.023/GB = $230/month

S3 Requests:
  ~$50/month (PUT/GET)

Total: ~$365/month for 10TB retained
```

**Compared to alternatives:**
- Elasticsearch cluster: ~$2000/month (3 data nodes, replicated)
- ClickHouse cluster: ~$800/month (3 nodes, local storage only)
- **Sequins (hybrid):** ~$365/month

**Savings: 4-5x cheaper than traditional solutions**

---

## 14. Implementation Roadmap

### Phase 1: Single-Node Enhancement
**Goal:** Prepare existing codebase for distribution

- Implement tiered storage (memory → local DB → S3)
- Add object_store integration
- Add Parquet serialization
- Add time-based partitioning
- Add shard assignment logic (even if only 1 shard)

**Deliverable:** Single-node daemon with S3 backing

### Phase 2: Cluster Membership
**Goal:** Multiple nodes discover each other

- Integrate memberlist crate for gossip
- Add Kubernetes discovery via DNS
- Implement health checking
- Add cluster metadata tracking
- Add metrics for cluster size

**Deliverable:** Multi-node cluster with membership tracking

### Phase 3: Distributed Writes
**Goal:** Distribute OTLP ingestion across nodes

- Implement consistent hashing for trace routing
- Add remote write forwarding (if trace hits wrong node)
- Implement S3 write-through (batch uploads)
- Add write-ahead log for durability before S3 upload

**Deliverable:** Distributed write path with S3 durability

### Phase 4: Distributed Queries
**Goal:** Query across all nodes

- Implement scatter-gather query router
- Add time-based query pruning
- Add result merging (K-way merge)
- Add query caching
- Add timeout handling

**Deliverable:** Full distributed query support

### Phase 5: Operations & Reliability
**Goal:** Production-ready operations

- Add graceful node addition/removal
- Add shard rebalancing
- Add backup/restore procedures
- Add disaster recovery testing
- Add comprehensive monitoring

**Deliverable:** Production-ready distributed cluster

### Phase 6: Optimization
**Goal:** Performance tuning

- Query performance optimization
- Storage format tuning
- Network protocol optimization
- Cache efficiency improvements
- Load balancing refinement

**Deliverable:** Optimized for production workloads

### Phase 7: Kubernetes Operator (Optional)
**Goal:** Automated cluster management

- Custom Resource Definitions (CRDs)
- Automated scaling
- Automated rebalancing
- Self-healing capabilities

**Deliverable:** Fully automated Kubernetes operator

---

## 15. Success Metrics

### Performance Targets

| Metric | Target (p50) | Target (p95) | Measurement |
|--------|-------------|--------------|-------------|
| OTLP write latency | < 5ms | < 20ms | End-to-end ingestion |
| Hot query latency | < 50ms | < 200ms | Recent data (last 1h) |
| Cold query latency | < 200ms | < 1000ms | Old data (> 1h ago) |
| Scatter-gather overhead | < 10ms | < 50ms | Multi-node query coordination |
| Storage efficiency | > 5:1 | N/A | Compression ratio (Parquet + Zstd) |
| Cache hit rate | > 80% | N/A | Local SSD cache effectiveness |

### Scalability Targets

- **Ingestion throughput:** 100,000 spans/sec per node
- **Concurrent queries:** 1,000 queries/sec per node
- **Cluster size:** 3-50 nodes
- **Data retention:** 30 days @ 10TB (compressed)
- **Query fan-out:** 10 nodes in parallel

### Reliability Targets

- **Availability:** 99.9% (< 9 hours downtime/year)
- **Data durability:** 99.999999999% (S3-backed, RF=1)
- **Recovery time:** < 1 minute (node failure - pull from S3)
- **Data loss on node failure:** Only in-memory buffer (< 5 minutes of data)
- **Data loss on planned shutdown:** Zero (graceful flush to S3)

---

## Conclusion

This architectural design provides a **pragmatic, Kubernetes-native approach** to building a distributed telemetry storage system for Sequins. The key principles are:

1. **Simplicity first:** Use Kubernetes primitives, avoid external dependencies
2. **Leverage object storage:** S3/MinIO for durability and cost-efficiency
3. **RF=1 with S3 write-through:** Simplest approach, no local replication needed
4. **Hybrid tiering:** Local SSD cache for hot data, S3 for cold data
5. **Homogeneous nodes:** Single node type for operational simplicity
6. **Eventual consistency:** Favor availability over strong consistency
7. **Time-based sharding:** Natural for time-series observability data
8. **Scatter-gather queries:** Distribute work without centralized coordinator

The design draws heavily from proven systems (Tempo, Loki, ClickHouse) while adapting to Sequins's specific requirements: **on-prem deployment, self-hosted, and operational simplicity**.

**Expected result:** Horizontally scalable telemetry platform supporting 3-50 nodes, 100k+ spans/sec ingestion, and sub-second query latencies.

**Recommendation:** Proceed with Phase 1 (single-node enhancement with S3 backing) to validate the storage architecture before committing to full distributed implementation.

---

## Next Steps

### Immediate Actions

1. **Validate assumptions with prototype:**
   - Build small proof-of-concept with 3 nodes
   - Test object_store crate with MinIO
   - Test memberlist crate for gossip
   - Measure latencies and throughput

2. **Make architecture decisions:**
   - Confirm S3/MinIO choice for object storage
   - Decide on initial cluster size (3, 5, or 7 nodes)
   - Configure S3 write-through batch size and interval

3. **Set up development environment:**
   - Local Kubernetes cluster (kind or minikube)
   - Local MinIO deployment
   - OTLP test data generator

### Research & Exploration

1. **Deep dive into Rust libraries:**
   - object_store: S3 API compatibility
   - memberlist: Gossip protocol features
   - parquet: Serialization performance
   - lru: Cache implementation

2. **Study reference architectures:**
   - Grafana Tempo codebase (Go)
   - ClickHouse sharding patterns
   - Cassandra consistency models

3. **Prototype key components:**
   - Consistent hashing implementation
   - K-way merge algorithm
   - Parquet serialization benchmarks

### Questions to Answer

1. **MinIO vs S3:** Self-hosted MinIO or cloud S3?
2. **Initial cluster size:** Start with 3 or 5 nodes?
3. **Cache size:** How much local SSD per node?
4. **Rebalancing strategy:** Manual or automatic?
5. **Query timeout:** How long to wait for slow nodes?
