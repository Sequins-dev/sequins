//! Metric Series Index for high-cardinality time series
//!
//! The series index maps (metric_name, sorted_attributes) -> SeriesId
//! This enables:
//! - Deduplication of attribute combinations
//! - Compact storage (SeriesId instead of full attributes)
//! - Efficient label-based filtering
//! - FastLanes encoding on series_id column

pub mod error;

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use crate::error::{Error, Result};
use object_store::{path::Path as ObjectPath, ObjectStore, ObjectStoreExt};

/// Unique identifier for a metric series
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
pub struct SeriesId(pub u64);

impl SeriesId {
    /// Create a new SeriesId from a u64
    pub fn new(id: u64) -> Self {
        Self(id)
    }

    /// Get the raw u64 value
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Derive a **content-addressed**, node-stable id from a series' identity
    /// (metric name + its attributes).
    ///
    /// Uses FNV-1a/64 over a canonical `name\0(k\0v\0)*` encoding of the
    /// name and the attributes in sorted-key order (a `BTreeMap` is already
    /// sorted). Because the id is a pure function of content — the same pattern
    /// the rest of the engine uses for resource/scope/metric ids — the same
    /// series hashes identically on **every** node with no coordination, which
    /// is what lets metric cold files written by different nodes share one
    /// dataset without a per-node id counter clobbering across the cluster.
    pub fn from_content(metric_name: &str, attributes: &BTreeMap<String, String>) -> Self {
        const OFFSET: u64 = 0xcbf29ce484222325;
        const PRIME: u64 = 0x100000001b3;
        fn mix(h: &mut u64, bytes: &[u8]) {
            for &b in bytes {
                *h ^= b as u64;
                *h = h.wrapping_mul(PRIME);
            }
        }
        let mut h = OFFSET;
        mix(&mut h, metric_name.as_bytes());
        mix(&mut h, &[0]);
        for (k, v) in attributes {
            mix(&mut h, k.as_bytes());
            mix(&mut h, &[0]);
            mix(&mut h, v.as_bytes());
            mix(&mut h, &[0]);
        }
        Self(h)
    }
}

/// Series metadata: metric name and attributes
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeriesMetadata {
    pub metric_name: String,
    pub attributes: BTreeMap<String, String>,
}

/// Single entry in the serialized series index payload
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SeriesEntry {
    series_id: u64,
    metric_name: String,
    attributes: BTreeMap<String, String>,
}

/// Serialization envelope for the series index
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SeriesIndexPayload {
    entries: Vec<SeriesEntry>,
}

/// In-memory index mapping series to IDs and supporting label-based lookups
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesIndex {
    /// Forward lookup: (metric_name, sorted_attributes) -> SeriesId
    series_map: HashMap<(String, BTreeMap<String, String>), SeriesId>,

    /// Reverse lookup: SeriesId -> SeriesMetadata
    reverse_map: HashMap<SeriesId, SeriesMetadata>,

    /// Label index: (label_key, label_value) -> Set<SeriesId>
    label_index: HashMap<(String, String), HashSet<SeriesId>>,
}

impl SeriesIndex {
    /// Create a new empty series index
    pub fn new() -> Self {
        Self {
            series_map: HashMap::new(),
            reverse_map: HashMap::new(),
            label_index: HashMap::new(),
        }
    }

    /// Register a series and return its **content-addressed** id.
    ///
    /// The id is a pure function of `(metric_name, attributes)`
    /// ([`SeriesId::from_content`]), so registering the same series is
    /// idempotent and yields the same id on every node — no counter, no
    /// coordination. Registering only records the id→metadata and label-index
    /// entries the first time a given series is seen.
    pub fn register(
        &mut self,
        metric_name: &str,
        attributes: BTreeMap<String, String>,
    ) -> SeriesId {
        let series_id = SeriesId::from_content(metric_name, &attributes);
        let key = (metric_name.to_string(), attributes.clone());

        if self.reverse_map.contains_key(&series_id) {
            return series_id;
        }

        // Forward mapping
        self.series_map.insert(key, series_id);

        // Reverse mapping
        let metadata = SeriesMetadata {
            metric_name: metric_name.to_string(),
            attributes: attributes.clone(),
        };
        self.reverse_map.insert(series_id, metadata);

        // Update label index
        for (key, value) in &attributes {
            self.label_index
                .entry((key.clone(), value.clone()))
                .or_default()
                .insert(series_id);
        }

        series_id
    }

    /// Look up series metadata by ID
    pub fn lookup(&self, id: SeriesId) -> Option<&SeriesMetadata> {
        self.reverse_map.get(&id)
    }

    /// Distinct metric names present in the index.
    pub fn metric_names(&self) -> BTreeSet<String> {
        self.reverse_map
            .values()
            .map(|m| m.metric_name.clone())
            .collect()
    }

    /// Distinct label keys across series, optionally scoped to one metric.
    pub fn label_keys(&self, metric: Option<&str>) -> BTreeSet<String> {
        self.reverse_map
            .values()
            .filter(|m| metric.map_or(true, |name| m.metric_name == name))
            .flat_map(|m| m.attributes.keys().cloned())
            .collect()
    }

    /// Distinct values for a label key, optionally scoped to one metric.
    pub fn label_values(&self, key: &str, metric: Option<&str>) -> BTreeSet<String> {
        self.reverse_map
            .values()
            .filter(|m| metric.map_or(true, |name| m.metric_name == name))
            .filter_map(|m| m.attributes.get(key).cloned())
            .collect()
    }

    /// Find all series matching a metric name and label filters
    ///
    /// Returns the intersection of series matching all filters.
    pub fn resolve_matchers(
        &self,
        metric_name: &str,
        filters: &[(String, String)],
    ) -> HashSet<SeriesId> {
        if filters.is_empty() {
            // No filters: return all series for this metric
            return self
                .reverse_map
                .iter()
                .filter(|(_, metadata)| metadata.metric_name == metric_name)
                .map(|(series_id, _)| *series_id)
                .collect();
        }

        // Start with series matching the first filter
        let mut result = if let Some(series_set) = self.label_index.get(&filters[0]) {
            series_set.clone()
        } else {
            return HashSet::new();
        };

        // Intersect with remaining filters
        for filter in &filters[1..] {
            if let Some(series_set) = self.label_index.get(filter) {
                result.retain(|id| series_set.contains(id));
            } else {
                return HashSet::new();
            }
        }

        // Filter by metric name
        result.retain(|id| {
            self.lookup(*id)
                .map(|metadata| metadata.metric_name == metric_name)
                .unwrap_or(false)
        });

        result
    }

    /// Find all series with a specific label key-value pair
    pub fn resolve_label_matcher(&self, key: &str, value: &str) -> HashSet<SeriesId> {
        self.label_index
            .get(&(key.to_string(), value.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    /// Merge another series index into this one
    ///
    /// Used for incremental updates when loading new data.
    /// Series that already exist will keep their existing IDs.
    pub fn merge(&mut self, other: SeriesIndex) {
        for ((metric_name, attributes), _other_id) in other.series_map {
            // Register will reuse existing ID if present, or create new one
            let _series_id = self.register(&metric_name, attributes);
        }
    }

    /// Object-store directory holding the per-node index shards.
    fn shard_dir(base_path: &str) -> String {
        format!("{}/metrics/series_index", base_path.trim_end_matches('/'))
    }

    /// Persist this node's slice of the series index to its **own** shard
    /// (`{base}/metrics/series_index/{node_id}.json`).
    ///
    /// Cold storage is shared cluster-wide, but each node writes only its own
    /// shard, so concurrent nodes never clobber one another's metadata. Readers
    /// ([`Self::load`]) union all shards; because ids are content-addressed, the
    /// union is conflict-free (the same series has the same id in every shard).
    pub async fn persist(
        &self,
        store: Arc<dyn ObjectStore>,
        base_path: &str,
        node_id: &str,
    ) -> Result<()> {
        use object_store::PutPayload;

        if self.reverse_map.is_empty() {
            return Ok(());
        }

        // Collect series data in sorted order for deterministic output
        let mut entries: Vec<SeriesEntry> = self
            .reverse_map
            .iter()
            .map(|(id, metadata)| SeriesEntry {
                series_id: id.as_u64(),
                metric_name: metadata.metric_name.clone(),
                attributes: metadata.attributes.clone(),
            })
            .collect();
        entries.sort_by_key(|e| e.series_id);

        let payload = SeriesIndexPayload { entries };

        let json_bytes = serde_json::to_vec(&payload)
            .map_err(|e| Error::Storage(format!("Failed to serialize series index: {}", e)))?;

        let sanitized: String = node_id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let path = format!("{}/{}.json", Self::shard_dir(base_path), sanitized);
        let object_path = ObjectPath::from(path.as_str());

        store
            .put(&object_path, PutPayload::from(json_bytes))
            .await
            .map_err(|e| Error::Storage(format!("Failed to write series index: {}", e)))?;

        Ok(())
    }

    /// Load the series index by unioning every node's shard under
    /// `{base}/metrics/series_index/`. Missing/unreadable shards are skipped;
    /// an absent directory yields an empty index.
    pub async fn load(store: Arc<dyn ObjectStore>, base_path: &str) -> Result<Self> {
        use futures::StreamExt;

        let mut index = Self::new();
        let prefix = ObjectPath::from(Self::shard_dir(base_path).as_str());
        let mut list = store.list(Some(&prefix));
        while let Some(meta) = list.next().await {
            let Ok(meta) = meta else { continue };
            if !meta.location.as_ref().ends_with(".json") {
                continue;
            }
            let Ok(result) = store.get(&meta.location).await else {
                continue;
            };
            let Ok(bytes) = result.bytes().await else {
                continue;
            };
            let Ok(payload) = serde_json::from_slice::<SeriesIndexPayload>(&bytes) else {
                continue;
            };
            index.ingest_entries(payload.entries);
        }
        Ok(index)
    }

    /// Absorb serialized entries into this index (used when unioning shards).
    fn ingest_entries(&mut self, entries: Vec<SeriesEntry>) {
        for entry in entries {
            let series_id = SeriesId(entry.series_id);
            self.series_map.insert(
                (entry.metric_name.clone(), entry.attributes.clone()),
                series_id,
            );
            self.reverse_map.insert(
                series_id,
                SeriesMetadata {
                    metric_name: entry.metric_name,
                    attributes: entry.attributes.clone(),
                },
            );
            for (key, value) in &entry.attributes {
                self.label_index
                    .entry((key.clone(), value.clone()))
                    .or_default()
                    .insert(series_id);
            }
        }
    }

    /// Get the total number of series
    pub fn len(&self) -> usize {
        self.reverse_map.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.reverse_map.is_empty()
    }
}

impl Default for SeriesIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_attrs(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn test_label_enumeration() {
        let mut index = SeriesIndex::new();
        index.register(
            "http_requests",
            create_attrs(&[("method", "GET"), ("route", "/a")]),
        );
        index.register(
            "http_requests",
            create_attrs(&[("method", "POST"), ("route", "/b")]),
        );
        index.register("cpu_seconds", create_attrs(&[("core", "0")]));

        assert_eq!(
            index.metric_names(),
            ["cpu_seconds".to_string(), "http_requests".to_string()]
                .into_iter()
                .collect()
        );
        // All label keys, and scoped to one metric.
        assert!(index.label_keys(None).contains("route"));
        assert!(index.label_keys(None).contains("core"));
        assert_eq!(
            index.label_keys(Some("http_requests")),
            ["method".to_string(), "route".to_string()]
                .into_iter()
                .collect()
        );
        // Values for a key, scoped to a metric.
        assert_eq!(
            index.label_values("route", Some("http_requests")),
            ["/a".to_string(), "/b".to_string()].into_iter().collect()
        );
        assert!(index.label_values("core", Some("http_requests")).is_empty());
    }

    #[test]
    fn test_series_registration() {
        let mut index = SeriesIndex::new();

        let attrs1 = create_attrs(&[("method", "GET"), ("status", "200")]);
        let id1 = index.register("http_requests", attrs1.clone());

        // Same series should return same ID
        let id2 = index.register("http_requests", attrs1.clone());
        assert_eq!(id1, id2);

        // Different attributes should get different ID
        let attrs2 = create_attrs(&[("method", "POST"), ("status", "200")]);
        let id3 = index.register("http_requests", attrs2);
        assert_ne!(id1, id3);

        assert_eq!(index.len(), 2);
    }

    #[test]
    fn test_series_deduplication() {
        let mut index = SeriesIndex::new();

        let attrs = create_attrs(&[("env", "prod"), ("region", "us-west")]);

        let id1 = index.register("cpu_usage", attrs.clone());
        let id2 = index.register("cpu_usage", attrs.clone());
        let id3 = index.register("cpu_usage", attrs);

        assert_eq!(id1, id2);
        assert_eq!(id2, id3);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_reverse_lookup() {
        let mut index = SeriesIndex::new();

        let attrs = create_attrs(&[("host", "server1"), ("region", "us-east")]);
        let id = index.register("memory_usage", attrs.clone());

        let metadata = index.lookup(id).unwrap();
        assert_eq!(metadata.metric_name, "memory_usage");
        assert_eq!(metadata.attributes, attrs);
    }

    #[test]
    fn test_label_matcher_resolution() {
        let mut index = SeriesIndex::new();

        let _id1 = index.register(
            "requests",
            create_attrs(&[("method", "GET"), ("status", "200")]),
        );
        let id2 = index.register(
            "requests",
            create_attrs(&[("method", "POST"), ("status", "200")]),
        );
        let _id3 = index.register(
            "requests",
            create_attrs(&[("method", "GET"), ("status", "404")]),
        );

        // Find all series with method=POST
        let result = index.resolve_label_matcher("method", "POST");
        assert_eq!(result.len(), 1);
        assert!(result.contains(&id2));

        // Find all series with status=200
        let result = index.resolve_label_matcher("status", "200");
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_resolve_matchers() {
        let mut index = SeriesIndex::new();

        let id1 = index.register(
            "http_requests",
            create_attrs(&[("method", "GET"), ("status", "200")]),
        );
        let _id2 = index.register(
            "http_requests",
            create_attrs(&[("method", "POST"), ("status", "200")]),
        );
        let _id3 = index.register(
            "http_requests",
            create_attrs(&[("method", "GET"), ("status", "404")]),
        );

        // Find series matching multiple filters
        let filters = vec![
            ("method".to_string(), "GET".to_string()),
            ("status".to_string(), "200".to_string()),
        ];
        let result = index.resolve_matchers("http_requests", &filters);
        assert_eq!(result.len(), 1);
        assert!(result.contains(&id1));

        // No matches
        let filters = vec![("method".to_string(), "DELETE".to_string())];
        let result = index.resolve_matchers("http_requests", &filters);
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_resolve_matchers_no_filters() {
        let mut index = SeriesIndex::new();

        index.register("cpu_usage", create_attrs(&[("host", "server1")]));
        index.register("cpu_usage", create_attrs(&[("host", "server2")]));
        index.register("memory_usage", create_attrs(&[("host", "server1")]));

        // Empty filters should return all series for metric
        let result = index.resolve_matchers("cpu_usage", &[]);
        assert_eq!(result.len(), 2);

        let result = index.resolve_matchers("memory_usage", &[]);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_index_merge() {
        let mut index1 = SeriesIndex::new();
        let id1 = index1.register("requests", create_attrs(&[("method", "GET")]));

        let mut index2 = SeriesIndex::new();
        let _id2 = index2.register("requests", create_attrs(&[("method", "POST")]));
        let _id3 = index2.register("requests", create_attrs(&[("method", "GET")])); // Duplicate

        index1.merge(index2);

        // Should have 2 unique series
        assert_eq!(index1.len(), 2);

        // Original ID should be preserved
        let new_id = index1.register("requests", create_attrs(&[("method", "GET")]));
        assert_eq!(id1, new_id);
    }

    #[tokio::test]
    async fn test_persist_and_load() {
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let base_path = "/test";

        let mut original = SeriesIndex::new();
        original.register(
            "http_requests",
            create_attrs(&[("method", "GET"), ("status", "200")]),
        );
        original.register(
            "http_requests",
            create_attrs(&[("method", "POST"), ("status", "201")]),
        );
        original.register("cpu_usage", create_attrs(&[("host", "server1")]));

        // Persist this node's shard
        original
            .persist(store.clone(), base_path, "node-a")
            .await
            .unwrap();

        // Load (unions all node shards)
        let loaded = SeriesIndex::load(store, base_path).await.unwrap();

        assert_eq!(loaded.len(), 3);

        // Verify all series are present
        for (id, metadata) in &original.reverse_map {
            let loaded_metadata = loaded.lookup(*id).unwrap();
            assert_eq!(loaded_metadata.metric_name, metadata.metric_name);
            assert_eq!(loaded_metadata.attributes, metadata.attributes);
        }

        // Verify label index works
        let result = loaded.resolve_label_matcher("method", "GET");
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_load_nonexistent_file() {
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let loaded = SeriesIndex::load(store, "/nonexistent").await.unwrap();

        assert!(loaded.is_empty());
    }

    #[test]
    fn test_series_id_is_content_addressed_and_node_stable() {
        // The same (name, attrs) must hash identically regardless of insertion
        // order or which "node" computes it — this is what lets metric cold files
        // from different nodes share one dataset.
        let a = SeriesId::from_content("http_requests", &create_attrs(&[("method", "GET")]));
        let b = SeriesId::from_content("http_requests", &create_attrs(&[("method", "GET")]));
        assert_eq!(a, b);

        // register() must agree with from_content().
        let mut idx = SeriesIndex::new();
        let r = idx.register("http_requests", create_attrs(&[("method", "GET")]));
        assert_eq!(r, a);

        // Different attributes → different id.
        let c = SeriesId::from_content("http_requests", &create_attrs(&[("method", "POST")]));
        assert_ne!(a, c);
    }

    #[tokio::test]
    async fn test_load_unions_multiple_node_shards() {
        use object_store::memory::InMemory;

        let store = Arc::new(InMemory::new());
        let base_path = "/test";

        // Two nodes each register a distinct series and persist their own shard.
        let mut node_a = SeriesIndex::new();
        node_a.register("cpu", create_attrs(&[("host", "a")]));
        node_a
            .persist(store.clone(), base_path, "node-a")
            .await
            .unwrap();

        let mut node_b = SeriesIndex::new();
        node_b.register("cpu", create_attrs(&[("host", "b")]));
        node_b
            .persist(store.clone(), base_path, "node-b")
            .await
            .unwrap();

        // A reader on any node sees both nodes' series (no clobber).
        let loaded = SeriesIndex::load(store, base_path).await.unwrap();
        assert_eq!(loaded.len(), 2);
    }
}
