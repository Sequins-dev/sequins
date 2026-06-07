//! Shared storage helpers for CLI commands

use anyhow::{Context, Result};
use sequins_storage::config::ColdTierConfig;
use sequins_storage::{Storage, StorageConfig};
use std::sync::Arc;

/// Open a local `Storage` instance rooted at `target`.
///
/// `target` must be a filesystem path (not an `http://` / `https://` URL).
pub async fn open_local_storage(target: &str) -> Result<Arc<Storage>> {
    let config = StorageConfig {
        cold_tier: ColdTierConfig {
            uri: target.to_owned(),
            ..Default::default()
        },
        ..Default::default()
    };
    let storage = Storage::new(config)
        .await
        .context("Failed to open database")?;
    Ok(Arc::new(storage))
}
