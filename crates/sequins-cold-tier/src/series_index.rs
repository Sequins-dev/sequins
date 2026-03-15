//! Metric Series Index for high-cardinality time series
//!
//! The series index maps (metric_name, sorted_attributes) -> SeriesId
//! This enables:
//! - Deduplication of attribute combinations
//! - Compact storage (SeriesId instead of full attributes)
//! - Efficient label-based filtering
//! - FastLanes encoding on series_id column

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::error::{Error, Result};
use object_store::{path::Path as ObjectPath, ObjectStore};

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
    next_id: u64,
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

    /// Next available series ID
    next_id: u64,
}

impl SeriesIndex {
    /// Create a new empty series index
    pub fn new() -> Self {
        Self {
            series_map: HashMap::new(),
            reverse_map: HashMap::new(),
            label_index: HashMap::new(),
            next_id: 1, // Start from 1 (0 could be reserved for "no series")
        }
    }

    /// Register a series and return its ID
    ///
    /// If the series already exists, returns the existing ID.
    /// Otherwise, creates a new series and returns a new ID.
    pub fn register(
        &mut self,
        metric_name: &str,
        attributes: BTreeMap<String, String>,
    ) -> SeriesId {
        let key = (metric_name.to_string(), attributes.clone());

        if let Some(&series_id) = self.series_map.get(&key) {
            return series_id;
        }

        // Create new series
        let series_id = SeriesId(self.next_id);
        self.next_id += 1;

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

    /// Persist the series index to storage as a JSON file
    pub async fn persist(&self, store: Arc<dyn ObjectStore>, base_path: &str) -> Result<()> {
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

        let payload = SeriesIndexPayload {
            next_id: self.next_id,
            entries,
        };

        let json_bytes = serde_json::to_vec(&payload)
            .map_err(|e| Error::Storage(format!("Failed to serialize series index: {}", e)))?;

        let path = format!(
            "{}/metrics/series_index.json",
            base_path.trim_end_matches('/')
        );
        let object_path = ObjectPath::from(path.as_str());

        store
            .put(&object_path, PutPayload::from(json_bytes))
            .await
            .map_err(|e| Error::Storage(format!("Failed to write series index: {}", e)))?;

        Ok(())
    }

    /// Load the series index from storage
    pub async fn load(store: Arc<dyn ObjectStore>, base_path: &str) -> Result<Self> {
        let path = format!(
            "{}/metrics/series_index.json",
            base_path.trim_end_matches('/')
        );
        let object_path = ObjectPath::from(path.as_str());

        // Check if file exists
        let bytes = match store.get(&object_path).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|e| Error::Storage(format!("Failed to read series index: {}", e)))?,
            Err(_) => return Ok(Self::new()),
        };

        let payload: SeriesIndexPayload = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Storage(format!("Failed to deserialize series index: {}", e)))?;

        let mut index = Self::new();
        index.next_id = payload.next_id;

        for entry in payload.entries {
            let series_id = SeriesId(entry.series_id);
            index.series_map.insert(
                (entry.metric_name.clone(), entry.attributes.clone()),
                series_id,
            );
            index.reverse_map.insert(
                series_id,
                SeriesMetadata {
                    metric_name: entry.metric_name,
                    attributes: entry.attributes.clone(),
                },
            );
            for (key, value) in &entry.attributes {
                index
                    .label_index
                    .entry((key.clone(), value.clone()))
                    .or_default()
                    .insert(series_id);
            }
        }

        Ok(index)
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

        // Persist
        original.persist(store.clone(), base_path).await.unwrap();

        // Load
        let loaded = SeriesIndex::load(store, base_path).await.unwrap();

        assert_eq!(loaded.len(), 3);
        assert_eq!(loaded.next_id, 4); // Should be ready for next ID

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
        assert_eq!(loaded.next_id, 1);
    }
}
