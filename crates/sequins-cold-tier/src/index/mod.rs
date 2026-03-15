//! Companion indexes for cold tier files

pub mod bloom;
pub mod log_index;
pub mod span_index;
pub mod trigram;

// Re-export index types
pub use bloom::BloomFilterSet;
pub use log_index::LogCompanionIndex;
pub use span_index::SpanCompanionIndex;
pub use trigram::TrigramIndex;

/// Collect all files from a Tantivy index directory as (filename, bytes) pairs.
///
/// Reads every regular file in the directory (non-recursively) and returns
/// `(filename, file_bytes)` pairs for use with `CompanionIndexBytes::tantivy_files`.
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
