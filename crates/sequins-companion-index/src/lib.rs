//! sequins-companion-index — Bloom filter, trigram, and Tantivy indexes for Vortex files
//!
//! Reusable indexes embedded in cold-tier Vortex files for efficient pruning.
//! No Vortex dependency — pure Arrow + index library deps.

pub mod bloom;
pub mod log_index;
pub mod span_index;
pub mod trigram;

pub use bloom::BloomFilterSet;
pub use log_index::LogCompanionIndex;
pub use span_index::SpanCompanionIndex;
pub use trigram::TrigramIndex;

/// All serialized companion index data for a single Vortex file.
pub struct CompanionIndexBytes {
    /// Serialized `BloomFilterSet` bytes.
    pub bloom_bytes: Vec<u8>,
    /// Serialized `TrigramIndex` bytes.
    pub trigram_bytes: Vec<u8>,
    /// Tantivy index files: (filename, file_bytes).
    pub tantivy_files: Vec<(String, Vec<u8>)>,
}

/// Extract all committed Tantivy index files from an in-memory `Index`
/// as (filename, bytes) pairs — no filesystem I/O.
///
/// Reads `meta.json`, `.managed.json`, and all segment component files via
/// `Index::directory().atomic_read()` so the index never touches disk.
pub(crate) fn collect_tantivy_files_from_index(
    index: &tantivy::Index,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    use tantivy::Directory as _;
    let dir = index.directory();
    let mut files = Vec::new();

    // Always include the top-level meta files.
    for name in &["meta.json", ".managed.json"] {
        let path = std::path::Path::new(name);
        if let Ok(bytes) = dir.atomic_read(path) {
            files.push((name.to_string(), bytes));
        } // Err: file may not exist in a fresh index
    }

    // Enumerate segment files via the searchable segment metas.
    let segment_metas = index
        .searchable_segment_metas()
        .map_err(|e| format!("Failed to list segment metas: {}", e))?;

    for meta in &segment_metas {
        for path in meta.list_files() {
            let filename = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string();
            if filename.is_empty() {
                continue;
            }
            if let Ok(bytes) = dir.atomic_read(&path) {
                files.push((filename, bytes));
            } // Err: component may not exist for this segment type
        }
    }

    Ok(files)
}

/// Collect all files from a Tantivy index directory as (filename, bytes) pairs.
///
/// Legacy on-disk variant — kept for callers that still hold a directory path.
/// Prefer `collect_tantivy_files_from_index` when the `Index` is available.
#[allow(dead_code)]
pub(crate) fn collect_tantivy_files(
    dir: &std::path::Path,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let mut files = Vec::new();
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("Failed to read Tantivy dir: {}", e))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("Failed to read dir entry: {}", e))?;
        let path = entry.path();
        if path.is_file() {
            let filename = path
                .file_name()
                .and_then(|n| n.to_str())
                .ok_or_else(|| "Invalid Tantivy filename (non-UTF8)".to_string())?
                .to_string();
            let bytes = std::fs::read(&path)
                .map_err(|e| format!("Failed to read Tantivy file '{}': {}", filename, e))?;
            files.push((filename, bytes));
        }
    }
    Ok(files)
}
