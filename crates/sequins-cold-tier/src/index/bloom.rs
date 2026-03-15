//! Bloom filter utilities for companion indexes
//!
//! Provides bloom filters for ultra-high-cardinality fields where traditional
//! inverted indexes would be too large. Bloom filters offer fast membership
//! testing with a configurable false positive rate (default 1%).

use bloomfilter::Bloom;
use std::collections::HashMap;

/// A collection of bloom filters, one per field
#[derive(Clone)]
pub struct BloomFilterSet {
    filters: HashMap<String, Bloom<String>>,
}

impl BloomFilterSet {
    /// Create a new empty bloom filter set
    pub fn new() -> Self {
        Self {
            filters: HashMap::new(),
        }
    }

    /// Build a bloom filter for a specific field
    ///
    /// # Arguments
    /// * `field_name` - Name of the field to filter
    /// * `values` - Iterator of values to insert into the filter
    /// * `fpr` - False positive rate (0.0 to 1.0), default 0.01 (1%)
    ///
    /// # Performance
    /// With 1% FPR and 10,000 items, bloom filter is ~14KB
    pub fn build<I>(&mut self, field_name: String, values: I, fpr: f64)
    where
        I: IntoIterator<Item = String>,
    {
        let values_vec: Vec<String> = values.into_iter().collect();
        let num_items = values_vec.len();

        // Create bloom filter with specified FPR
        // Formula: m = -n*ln(p)/(ln(2)^2) where m=bits, n=items, p=fpr
        let mut bloom = Bloom::new_for_fp_rate(num_items, fpr);

        // Insert all values
        for value in values_vec {
            bloom.set(&value);
        }

        self.filters.insert(field_name, bloom);
    }

    /// Check if a value might be in the filter for a given field
    ///
    /// Returns:
    /// - `true` - Value might be present (or false positive)
    /// - `false` - Value is definitely not present
    pub fn check(&self, field_name: &str, value: &str) -> bool {
        self.filters
            .get(field_name)
            .map(|bloom| bloom.check(&value.to_string()))
            .unwrap_or(false)
    }

    /// Get the number of filters in this set
    pub fn len(&self) -> usize {
        self.filters.len()
    }

    /// Check if the set is empty
    pub fn is_empty(&self) -> bool {
        self.filters.is_empty()
    }

    /// Get all field names that have bloom filters
    pub fn field_names(&self) -> Vec<String> {
        self.filters.keys().cloned().collect()
    }

    /// Serialize to bytes for persistence
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        // Serialize bitmap + metadata + sip hash keys for each field
        // Type: (num_bits, num_hashes, bitmap, sip_key0, sip_key1)
        type BloomEntry = (usize, usize, Vec<u8>, (u64, u64), (u64, u64));
        let bitmap_data: HashMap<String, BloomEntry> = self
            .filters
            .iter()
            .map(|(k, bloom)| {
                let bitmap = bloom.bitmap();
                let num_bits = bloom.number_of_bits() as usize;
                let num_hashes = bloom.number_of_hash_functions() as usize;
                let sip_keys = bloom.sip_keys();
                (
                    k.clone(),
                    (num_bits, num_hashes, bitmap, sip_keys[0], sip_keys[1]),
                )
            })
            .collect();

        serde_json::to_vec(&bitmap_data)
            .map_err(|e| format!("Failed to serialize bloom set: {}", e))
    }

    /// Deserialize from bytes
    pub fn deserialize(bytes: &[u8]) -> Result<Self, String> {
        type BloomEntry = (usize, usize, Vec<u8>, (u64, u64), (u64, u64));
        let bitmap_data: HashMap<String, BloomEntry> = serde_json::from_slice(bytes)
            .map_err(|e| format!("Failed to deserialize bloom set: {}", e))?;

        let filters = bitmap_data
            .into_iter()
            .map(|(k, (num_bits, num_hashes, bitmap, sip0, sip1))| {
                let sips = [sip0, sip1];
                let bloom = Bloom::from_existing(&bitmap, num_bits as u64, num_hashes as u32, sips);
                (k, bloom)
            })
            .collect();

        Ok(Self { filters })
    }

    /// Estimate the size in bytes of all bloom filters
    pub fn estimated_size_bytes(&self) -> usize {
        self.filters
            .values()
            .map(|bloom| (bloom.number_of_bits() / 8) as usize)
            .sum()
    }
}

impl Default for BloomFilterSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let mut bloom_set = BloomFilterSet::new();

        // Build a filter with some trace IDs
        let trace_ids = vec![
            "trace-123".to_string(),
            "trace-456".to_string(),
            "trace-789".to_string(),
        ];

        bloom_set.build("trace_id".to_string(), trace_ids, 0.01);

        // Check for values
        assert!(bloom_set.check("trace_id", "trace-123"));
        assert!(bloom_set.check("trace_id", "trace-456"));
        assert!(bloom_set.check("trace_id", "trace-789"));

        // Non-existent field should always return false
        assert!(!bloom_set.check("span_id", "trace-123"));
    }

    #[test]
    fn test_bloom_filter_serialization() {
        let mut bloom_set = BloomFilterSet::new();

        let values = vec![
            "value1".to_string(),
            "value2".to_string(),
            "value3".to_string(),
        ];

        bloom_set.build("field1".to_string(), values, 0.01);

        // Serialize
        let bytes = bloom_set.serialize().unwrap();

        // Deserialize
        let restored = BloomFilterSet::deserialize(&bytes).unwrap();

        // Verify it works the same
        assert!(restored.check("field1", "value1"));
        assert!(restored.check("field1", "value2"));
        assert!(!restored.check("field1", "value999"));
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut bloom_set = BloomFilterSet::new();

        // Insert 1000 items
        let values: Vec<String> = (0..1000).map(|i| format!("value{}", i)).collect();
        bloom_set.build("field".to_string(), values.clone(), 0.01);

        // All inserted values should be found
        for value in &values {
            assert!(bloom_set.check("field", value));
        }

        // Check false positive rate with 1000 non-existent items
        let mut false_positives = 0;
        for i in 10000..11000 {
            if bloom_set.check("field", &format!("value{}", i)) {
                false_positives += 1;
            }
        }

        // With 1% FPR, we expect around 10 false positives out of 1000 checks
        // Allow some variance (0.3% to 2% - lower bound allows for good performance)
        assert!(
            (3..=20).contains(&false_positives),
            "False positive rate out of expected range: {} / 1000",
            false_positives
        );
    }

    #[test]
    fn test_bloom_filter_size_estimation() {
        let mut bloom_set = BloomFilterSet::new();

        // 10,000 items with 1% FPR should be around 14KB
        let values: Vec<String> = (0..10000).map(|i| format!("value{}", i)).collect();
        bloom_set.build("field".to_string(), values, 0.01);

        let size = bloom_set.estimated_size_bytes();

        // Expected size is approximately 14KB, allow 10-20KB range
        assert!(
            (10_000..=20_000).contains(&size),
            "Bloom filter size {} not in expected range",
            size
        );
    }

    #[test]
    fn test_multiple_filters() {
        let mut bloom_set = BloomFilterSet::new();

        // Use 1000+ items to avoid false positives
        let trace_ids: Vec<String> = (0..1000).map(|i| format!("trace{}", i)).collect();
        let user_ids: Vec<String> = (0..1000).map(|i| format!("user{}", i)).collect();

        bloom_set.build("trace_id".to_string(), trace_ids, 0.01);
        bloom_set.build("user_id".to_string(), user_ids, 0.01);

        assert_eq!(bloom_set.len(), 2);
        assert!(bloom_set.check("trace_id", "trace0"));
        assert!(bloom_set.check("user_id", "user0"));
        assert!(!bloom_set.check("trace_id", "user0")); // Wrong field
        assert!(!bloom_set.check("user_id", "trace0")); // Wrong field
    }
}
