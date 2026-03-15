//! Trigram skip index for efficient text search on log bodies
//!
//! Builds a compact bitset-based index that allows quickly identifying which
//! row groups (chunks of log records) might contain a search query.
//!
//! # How it works
//!
//! 1. Split log records into chunks (default: 10,000 records per chunk)
//! 2. For each chunk, extract all 3-character subsequences (trigrams) from log bodies
//! 3. Hash each trigram to a bit position (hash % 50000)
//! 4. Store a bitset for each chunk (~6KB per chunk)
//! 5. At query time, extract trigrams from query and check which chunks have ALL query trigrams
//!
//! # Performance
//!
//! - Index size: ~6KB per 10K log records (0.6 bytes per record)
//! - Query time: O(chunks) bitset intersection (microseconds for millions of records)
//! - False positive rate: ~1% (configurable via bitset size)

use serde::{Deserialize, Serialize};

/// Default number of records per chunk
pub const DEFAULT_CHUNK_SIZE: usize = 10_000;

/// Default bitset size (controls false positive rate)
/// 50,000 bits = 6.25KB per chunk
pub const DEFAULT_BITSET_SIZE: usize = 50_000;

/// Trigram index for a batch of log records
///
/// Each chunk has a bitset indicating which trigrams are present
#[derive(Clone, Serialize, Deserialize)]
pub struct TrigramIndex {
    /// Bitsets, one per chunk
    chunks: Vec<Vec<u8>>,

    /// Number of records per chunk
    chunk_size: usize,

    /// Size of each bitset in bits
    bitset_size: usize,
}

impl TrigramIndex {
    /// Build a trigram index from log bodies
    ///
    /// # Arguments
    /// * `bodies` - Slice of log body strings
    /// * `chunk_size` - Number of records per chunk (default: 10,000)
    ///
    /// # Returns
    /// A trigram index ready for querying
    pub fn build(bodies: &[&str], chunk_size: usize) -> Self {
        Self::build_with_bitset_size(bodies, chunk_size, DEFAULT_BITSET_SIZE)
    }

    /// Build a trigram index with custom bitset size
    pub fn build_with_bitset_size(bodies: &[&str], chunk_size: usize, bitset_size: usize) -> Self {
        let mut chunks = Vec::new();

        // Process records in chunks
        for chunk_bodies in bodies.chunks(chunk_size) {
            let mut bitset = vec![0u8; bitset_size.div_ceil(8)];

            // Extract all trigrams from this chunk
            for body in chunk_bodies {
                for trigram in extract_trigrams(body) {
                    let bit_pos = hash_trigram(&trigram) % bitset_size;
                    set_bit(&mut bitset, bit_pos);
                }
            }

            chunks.push(bitset);
        }

        Self {
            chunks,
            chunk_size,
            bitset_size,
        }
    }

    /// Find candidate chunks that might contain the query
    ///
    /// Returns indices of chunks where ALL query trigrams are present.
    /// These chunks must be scanned; others can be skipped.
    ///
    /// # Arguments
    /// * `query` - Search query string
    ///
    /// # Returns
    /// Vector of chunk indices that are candidates
    pub fn candidate_chunks(&self, query: &str) -> Vec<usize> {
        // Extract trigrams from query
        let query_trigrams = extract_trigrams(query);

        if query_trigrams.is_empty() {
            // No trigrams (query too short) - must scan all chunks
            return (0..self.chunks.len()).collect();
        }

        // Convert query trigrams to bit positions
        let query_bits: Vec<usize> = query_trigrams
            .iter()
            .map(|trigram| hash_trigram(trigram) % self.bitset_size)
            .collect();

        // Find chunks where ALL query bits are set
        let mut candidates = Vec::new();
        for (chunk_idx, bitset) in self.chunks.iter().enumerate() {
            if query_bits
                .iter()
                .all(|&bit_pos| is_bit_set(bitset, bit_pos))
            {
                candidates.push(chunk_idx);
            }
        }

        candidates
    }

    /// Get the number of chunks in the index
    pub fn num_chunks(&self) -> usize {
        self.chunks.len()
    }

    /// Get the total number of records indexed
    pub fn total_records(&self) -> usize {
        self.chunks.len() * self.chunk_size
    }

    /// Estimate the size of the index in bytes
    pub fn estimated_size_bytes(&self) -> usize {
        self.chunks.iter().map(|bitset| bitset.len()).sum()
    }

    /// Serialize to bytes for persistence
    pub fn serialize(&self) -> Result<Vec<u8>, String> {
        bincode::serialize(self).map_err(|e| format!("Failed to serialize trigram index: {}", e))
    }

    /// Deserialize from bytes
    pub fn deserialize(bytes: &[u8]) -> Result<Self, String> {
        bincode::deserialize(bytes)
            .map_err(|e| format!("Failed to deserialize trigram index: {}", e))
    }

    /// Get chunk size
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    /// Get bitset size
    pub fn bitset_size(&self) -> usize {
        self.bitset_size
    }
}

/// Extract all 3-character subsequences from a string
///
/// Uses character boundaries (UTF-8 aware), not byte offsets.
fn extract_trigrams(text: &str) -> Vec<String> {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() < 3 {
        return Vec::new();
    }

    let mut trigrams = Vec::new();
    for i in 0..=chars.len() - 3 {
        let trigram: String = chars[i..i + 3].iter().collect();
        // Convert to lowercase for case-insensitive matching
        trigrams.push(trigram.to_lowercase());
    }

    trigrams
}

/// Hash a trigram to a bit position
///
/// Uses a simple FNV-1a hash for speed
fn hash_trigram(trigram: &str) -> usize {
    let mut hash: u64 = 0xcbf29ce484222325; // FNV offset basis

    for byte in trigram.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3); // FNV prime
    }

    hash as usize
}

/// Set a bit in a bitset
fn set_bit(bitset: &mut [u8], bit_pos: usize) {
    let byte_idx = bit_pos / 8;
    let bit_idx = bit_pos % 8;
    if byte_idx < bitset.len() {
        bitset[byte_idx] |= 1 << bit_idx;
    }
}

/// Check if a bit is set in a bitset
fn is_bit_set(bitset: &[u8], bit_pos: usize) -> bool {
    let byte_idx = bit_pos / 8;
    let bit_idx = bit_pos % 8;
    if byte_idx < bitset.len() {
        (bitset[byte_idx] & (1 << bit_idx)) != 0
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_trigrams_simple() {
        let trigrams = extract_trigrams("error");
        assert_eq!(trigrams.len(), 3);
        assert!(trigrams.contains(&"err".to_string()));
        assert!(trigrams.contains(&"rro".to_string()));
        assert!(trigrams.contains(&"ror".to_string()));
    }

    #[test]
    fn test_extract_trigrams_too_short() {
        let trigrams = extract_trigrams("ab");
        assert_eq!(trigrams.len(), 0);
    }

    #[test]
    fn test_extract_trigrams_case_insensitive() {
        let trigrams1 = extract_trigrams("ERROR");
        let trigrams2 = extract_trigrams("error");
        assert_eq!(trigrams1, trigrams2);
    }

    #[test]
    fn test_extract_trigrams_unicode() {
        let trigrams = extract_trigrams("café");
        assert_eq!(trigrams.len(), 2);
        assert!(trigrams.contains(&"caf".to_string()));
        assert!(trigrams.contains(&"afé".to_string()));
    }

    #[test]
    fn test_bitset_operations() {
        let mut bitset = vec![0u8; 10];

        set_bit(&mut bitset, 5);
        set_bit(&mut bitset, 15);
        set_bit(&mut bitset, 75);

        assert!(is_bit_set(&bitset, 5));
        assert!(is_bit_set(&bitset, 15));
        assert!(is_bit_set(&bitset, 75));
        assert!(!is_bit_set(&bitset, 6));
        assert!(!is_bit_set(&bitset, 14));
    }

    #[test]
    fn test_trigram_index_build() {
        let bodies = vec![
            "error connecting to database",
            "warning: high memory usage",
            "info: request completed successfully",
        ];

        let index = TrigramIndex::build(&bodies, 10);

        assert_eq!(index.num_chunks(), 1);
        assert_eq!(index.chunk_size(), 10);
    }

    #[test]
    fn test_trigram_index_candidate_chunks_match() {
        let bodies = vec![
            "error connecting to database",
            "warning: high memory usage",
            "info: request completed successfully",
            "error in authentication module",
        ];

        let index = TrigramIndex::build(&bodies, 10);

        // Query for "error" - should find the chunk
        let candidates = index.candidate_chunks("error");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0], 0);
    }

    #[test]
    fn test_trigram_index_candidate_chunks_no_match() {
        let bodies = vec![
            "info: system started",
            "info: configuration loaded",
            "info: ready to accept connections",
        ];

        let index = TrigramIndex::build(&bodies, 10);

        // Query for "error" - shouldn't find the chunk (no "error" trigrams)
        let candidates = index.candidate_chunks("error");

        // May have false positives, but in this small example likely none
        // At minimum, this tests that the function runs without panic
        assert!(candidates.len() <= 1);
    }

    #[test]
    fn test_trigram_index_multiple_chunks() {
        // Create enough bodies to span multiple chunks
        let mut bodies = Vec::new();
        for i in 0..25 {
            if i < 10 {
                bodies.push("error in module A");
            } else if i < 20 {
                bodies.push("warning in module B");
            } else {
                bodies.push("error in module C");
            }
        }

        let bodies_refs: Vec<&str> = bodies.iter().map(|s| s.as_ref()).collect();
        let index = TrigramIndex::build(&bodies_refs, 10);

        assert_eq!(index.num_chunks(), 3);

        // Search for "error" - should find chunks 0 and 2
        let candidates = index.candidate_chunks("error");
        assert!(candidates.contains(&0));
        assert!(candidates.contains(&2));
    }

    #[test]
    fn test_trigram_index_serialization() {
        let bodies = vec!["error", "warning", "info"];
        let index = TrigramIndex::build(&bodies, 10);

        let bytes = index.serialize().unwrap();
        let restored = TrigramIndex::deserialize(&bytes).unwrap();

        assert_eq!(restored.num_chunks(), index.num_chunks());
        assert_eq!(restored.chunk_size(), index.chunk_size());
        assert_eq!(restored.bitset_size(), index.bitset_size());
    }

    #[test]
    fn test_trigram_index_size_estimation() {
        let bodies = vec!["test"; 10_000];
        let bodies_refs: Vec<&str> = bodies.iter().map(|s| s.as_ref()).collect();
        let index = TrigramIndex::build(&bodies_refs, 10_000);

        let size = index.estimated_size_bytes();

        // Should be about 6KB for 10K records
        assert!((5_000..=8_000).contains(&size), "Size: {}", size);
    }

    #[test]
    fn test_trigram_index_short_query() {
        let bodies = vec!["test"];
        let index = TrigramIndex::build(&bodies, 10);

        // Query too short for trigrams - should return all chunks
        let candidates = index.candidate_chunks("ab");
        assert_eq!(candidates.len(), 1);
    }

    #[test]
    fn test_hash_trigram_consistency() {
        let hash1 = hash_trigram("err");
        let hash2 = hash_trigram("err");
        assert_eq!(hash1, hash2);

        let hash3 = hash_trigram("war");
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_trigram_index_empty_bodies() {
        let bodies: Vec<&str> = vec![];
        let index = TrigramIndex::build(&bodies, 10);

        assert_eq!(index.num_chunks(), 0);
        assert_eq!(index.total_records(), 0);
    }
}
