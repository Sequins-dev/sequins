//! Durable cold-flush watermark + WAL checkpoint/compaction.
//!
//! The watermark `W` is the highest WAL sequence number such that every
//! ingested entry with seq ≤ `W` is durably persisted in the cold tier
//! (see [`sequins_hot_tier::HotTier::durable_watermark`]). Periodically (and on
//! shutdown) the node persists `W` under its object-store prefix and compacts
//! the WAL behind it, so the WAL retains only the un-flushed hot window. On
//! restart the node replays WAL entries after `W` to rebuild that window
//! without duplicating anything already in cold storage.

use super::Storage;
use crate::error::{Error, Result};
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use std::sync::Arc;

/// The per-node WAL base path derived from the **shared** cold-tier uri and this
/// node's id.
///
/// Cold storage is a single dataset shared by the whole cluster (no per-node
/// prefix), but the WAL is private to each node — it exists only to replay a
/// node's own un-flushed hot window after a crash — so it lives under a
/// `{cold_base}/{node_id}` sub-prefix. Both the constructor (WAL setup) and the
/// watermark path go through this one function so the two can never drift.
pub(super) fn wal_base_path(cold_uri: &str, node_id: &str) -> String {
    let base = sequins_cold_tier::store_base_path(cold_uri).trim_end_matches('/');
    if base.is_empty() {
        // Bucket-root cold uri (e.g. `s3://bucket`): the WAL prefix is just the
        // node id, with no leading slash.
        node_id.to_string()
    } else {
        format!("{base}/{node_id}")
    }
}

impl Storage {
    /// The WAL base path for this node — a per-node sub-prefix of the shared cold
    /// root, under which this node's WAL segments and durable watermark live.
    fn wal_base_path(&self) -> String {
        wal_base_path(&self.config.cold_tier.uri, &self.node_id)
    }

    /// Object-store path of this node's durable watermark file.
    fn watermark_path(&self) -> ObjPath {
        ObjPath::from(format!("{}/wal/watermark", self.wal_base_path()))
    }

    async fn object_store(&self) -> Arc<dyn ObjectStore> {
        self.cold_tier.read().await.store.clone()
    }

    /// Load the durable cold-flush watermark; `0` if none has been persisted.
    pub(crate) async fn load_watermark(&self) -> u64 {
        let store = self.object_store().await;
        match store.get(&self.watermark_path()).await {
            Ok(res) => match res.bytes().await {
                Ok(b) if b.len() >= 8 => {
                    let mut buf = [0u8; 8];
                    buf.copy_from_slice(&b[..8]);
                    u64::from_le_bytes(buf)
                }
                _ => 0,
            },
            // Absent (or unreadable) watermark → start from the beginning.
            Err(_) => 0,
        }
    }

    pub(crate) async fn persist_watermark(&self, w: u64) -> Result<()> {
        let store = self.object_store().await;
        store
            .put(
                &self.watermark_path(),
                PutPayload::from(w.to_le_bytes().to_vec()),
            )
            .await
            .map_err(|e| Error::Storage(format!("Failed to persist watermark: {e}")))?;
        Ok(())
    }

    /// Advance and persist the durable cold-flush watermark, then compact the
    /// WAL behind it. Returns the new watermark.
    ///
    /// Safe to call repeatedly; a no-op until some data has been flushed to
    /// cold storage.
    pub async fn checkpoint(&self) -> Result<u64> {
        // Force content-addressed metadata (resources/scopes/…) into cold first.
        // It's excluded from the watermark, so this guarantees the metadata for
        // every entry at or below the watermark is durable before we advance it.
        self.hot_tier.flush_content_addressed().await;

        // `durable_watermark()` returns u64::MAX when nothing is outstanding;
        // cap it at the last assigned WAL sequence so we never claim beyond it.
        let w = self.hot_tier.durable_watermark().min(self.wal.last_seq());
        if w == 0 {
            return Ok(0);
        }
        self.persist_watermark(w).await?;
        // Compaction is best-effort — the persisted watermark alone guarantees
        // replay correctness; compaction just bounds WAL growth.
        if let Err(e) = self.wal.compact_before(w + 1).await {
            tracing::warn!(watermark = w, error = %e, "WAL compaction failed");
        }
        tracing::debug!(watermark = w, "checkpoint complete");
        Ok(w)
    }
}
