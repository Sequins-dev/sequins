//! [`AppStateStore`] — durable, in-memory-cached store for conversations and
//! dashboards, persisted as JSON on the shared object store.
//!
//! State is held in `RwLock`ed maps for fast reads (and for the DataFusion table
//! providers to snapshot at scan time) with **write-through** to the object store,
//! mirroring how the retention policy and cold-flush watermark are persisted. The
//! object store natively supports overwrite/delete, so app state can mutate even
//! though the telemetry tiers are append-only.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use futures::StreamExt;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tokio::sync::RwLock;

use crate::error::Result;
use crate::types::{Conversation, ConversationItem, Dashboard};

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// Durable app-state store over an object-store prefix.
pub struct AppStateStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    conversations: RwLock<HashMap<String, Conversation>>,
    dashboards: RwLock<HashMap<String, Dashboard>>,
    /// `response_id` → `conversation_id`, for resolving `previous_response_id`.
    response_index: RwLock<HashMap<String, String>>,
    seq: AtomicU64,
}

impl AppStateStore {
    /// Build a store over `store` rooted at `prefix` (e.g. `<cold_base>/app_state`).
    /// Call [`load`](Self::load) once to hydrate from durable storage.
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            store,
            prefix: prefix.into().trim_end_matches('/').to_string(),
            conversations: RwLock::new(HashMap::new()),
            dashboards: RwLock::new(HashMap::new()),
            response_index: RwLock::new(HashMap::new()),
            seq: AtomicU64::new(0),
        }
    }

    fn conversation_path(&self, id: &str) -> ObjPath {
        ObjPath::from(format!("{}/conversations/{id}.json", self.prefix))
    }

    fn dashboard_path(&self, id: &str) -> ObjPath {
        ObjPath::from(format!("{}/dashboards/{id}.json", self.prefix))
    }

    fn next_id(&self, kind: &str) -> String {
        let n = self.seq.fetch_add(1, Ordering::Relaxed);
        format!("{kind}_{}_{n}", now_ns())
    }

    /// Hydrate the in-memory maps from durable storage. Malformed objects are
    /// skipped (logged), never fatal.
    pub async fn load(&self) -> Result<()> {
        let conv_prefix = ObjPath::from(format!("{}/conversations", self.prefix));
        for location in self.list_locations(&conv_prefix).await? {
            match self.get_json::<Conversation>(&location).await {
                Ok(conv) => {
                    let mut idx = self.response_index.write().await;
                    for item in &conv.items {
                        if let Some(rid) = &item.response_id {
                            idx.insert(rid.clone(), conv.id.clone());
                        }
                    }
                    drop(idx);
                    self.conversations
                        .write()
                        .await
                        .insert(conv.id.clone(), conv);
                }
                Err(e) => tracing::warn!(%location, error = %e, "skipping unreadable conversation"),
            }
        }

        let dash_prefix = ObjPath::from(format!("{}/dashboards", self.prefix));
        for location in self.list_locations(&dash_prefix).await? {
            match self.get_json::<Dashboard>(&location).await {
                Ok(dash) => {
                    self.dashboards.write().await.insert(dash.id.clone(), dash);
                }
                Err(e) => tracing::warn!(%location, error = %e, "skipping unreadable dashboard"),
            }
        }
        Ok(())
    }

    async fn list_locations(&self, prefix: &ObjPath) -> Result<Vec<ObjPath>> {
        let mut stream = self.store.list(Some(prefix));
        let mut out = Vec::new();
        while let Some(meta) = stream.next().await {
            out.push(meta?.location);
        }
        Ok(out)
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, location: &ObjPath) -> Result<T> {
        let bytes = self.store.get(location).await?.bytes().await?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    async fn put_json<T: serde::Serialize>(&self, path: &ObjPath, value: &T) -> Result<()> {
        let bytes = serde_json::to_vec(value)?;
        self.store.put(path, PutPayload::from(bytes)).await?;
        Ok(())
    }

    // ---- Conversations -----------------------------------------------------

    /// Append a Responses turn to a conversation (creating it if needed), then
    /// persist. `input_items` (user-supplied, before the turn) then `output_items`
    /// (produced by `response_id`) are appended in order. Returns the conversation id.
    pub async fn append_response(
        &self,
        conversation_id: Option<String>,
        title: Option<String>,
        input_items: Vec<ConversationItem>,
        output_items: Vec<ConversationItem>,
        response_id: String,
    ) -> Result<String> {
        let now = now_ns();
        let cid = conversation_id.unwrap_or_else(|| self.next_id("conv"));

        let snapshot = {
            let mut convs = self.conversations.write().await;
            let conv = convs.entry(cid.clone()).or_insert_with(|| Conversation {
                id: cid.clone(),
                title: title.clone(),
                created_at_ns: now,
                updated_at_ns: now,
                items: Vec::new(),
            });
            if conv.title.is_none() {
                conv.title = title;
            }
            let mut position = conv.items.len() as u32;
            for mut item in input_items.into_iter().chain(output_items) {
                item.position = position;
                position += 1;
                conv.items.push(item);
            }
            conv.updated_at_ns = now;
            conv.clone()
        };

        self.response_index
            .write()
            .await
            .insert(response_id, cid.clone());
        let path = self.conversation_path(&cid);
        self.put_json(&path, &snapshot).await?;
        Ok(cid)
    }

    /// Resolve a `previous_response_id` to its conversation id.
    pub async fn conversation_by_response(&self, response_id: &str) -> Option<String> {
        self.response_index.read().await.get(response_id).cloned()
    }

    /// Fetch a full conversation by id.
    pub async fn get_conversation(&self, id: &str) -> Option<Conversation> {
        self.conversations.read().await.get(id).cloned()
    }

    /// Snapshot all conversations (for the DataFusion table provider / listing).
    pub async fn conversations_snapshot(&self) -> Vec<Conversation> {
        self.conversations.read().await.values().cloned().collect()
    }

    /// Delete a conversation (in-memory + durable) and drop any response-index entries
    /// pointing at it. No-op if absent.
    pub async fn delete_conversation(&self, id: &str) -> Result<()> {
        self.conversations.write().await.remove(id);
        self.response_index.write().await.retain(|_, cid| cid != id);
        match self.store.delete(&self.conversation_path(id)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    // ---- Dashboards --------------------------------------------------------

    /// Snapshot all dashboards (for the DataFusion table provider).
    pub async fn dashboards_snapshot(&self) -> Vec<Dashboard> {
        self.dashboards.read().await.values().cloned().collect()
    }

    /// Create or update a dashboard, stamping `id`/timestamps as needed. Returns
    /// the stored dashboard (with its id).
    pub async fn upsert_dashboard(&self, mut dashboard: Dashboard) -> Result<Dashboard> {
        let now = now_ns();
        if dashboard.id.is_empty() {
            dashboard.id = self.next_id("dash");
            dashboard.created_at_ns = now;
        }
        dashboard.updated_at_ns = now;

        let stored = {
            let mut dbs = self.dashboards.write().await;
            if let Some(existing) = dbs.get(&dashboard.id) {
                dashboard.created_at_ns = existing.created_at_ns;
            } else if dashboard.created_at_ns == 0 {
                dashboard.created_at_ns = now;
            }
            dbs.insert(dashboard.id.clone(), dashboard.clone());
            dashboard
        };
        let path = self.dashboard_path(&stored.id);
        self.put_json(&path, &stored).await?;
        Ok(stored)
    }

    /// Delete a dashboard (in-memory + durable). No-op if absent.
    pub async fn delete_dashboard(&self, id: &str) -> Result<()> {
        self.dashboards.write().await.remove(id);
        // Absent object → ignore NotFound.
        match self.store.delete(&self.dashboard_path(id)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Fetch a dashboard by id.
    pub async fn get_dashboard(&self, id: &str) -> Option<Dashboard> {
        self.dashboards.read().await.get(id).cloned()
    }
}

/// Read/write dashboards. Implemented for `Storage` (local, delegating to
/// [`AppStateStore`]'s inherent methods) and for the remote client over HTTP, so the
/// app has one interface across Local and Remote.
#[async_trait]
pub trait DashboardApi: Send + Sync {
    async fn list_dashboards(&self) -> Result<Vec<Dashboard>>;
    async fn get_dashboard(&self, id: &str) -> Result<Option<Dashboard>>;
    async fn save_dashboard(&self, dashboard: Dashboard) -> Result<Dashboard>;
    async fn delete_dashboard(&self, id: &str) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DashboardRow, RowPanel, SavedVisualization, DEFAULT_ROW_HEIGHT};

    fn mem_store() -> Arc<dyn ObjectStore> {
        Arc::new(object_store::memory::InMemory::new())
    }

    fn item(role: &str, text: &str) -> ConversationItem {
        ConversationItem {
            response_id: None,
            position: 0,
            role: role.into(),
            item_type: "message".into(),
            text: Some(text.into()),
            tool_name: None,
            tool_arguments: None,
            tool_output: None,
            created_at_ns: 0,
        }
    }

    #[tokio::test]
    async fn conversation_append_and_reload() {
        let store = mem_store();
        let s = AppStateStore::new(store.clone(), "app_state");
        let cid = s
            .append_response(
                None,
                Some("Title".into()),
                vec![item("user", "hi")],
                vec![ConversationItem {
                    response_id: Some("resp_1".into()),
                    ..item("assistant", "hello")
                }],
                "resp_1".into(),
            )
            .await
            .unwrap();

        // Second turn resolves the conversation via previous_response_id.
        let resolved = s.conversation_by_response("resp_1").await.unwrap();
        assert_eq!(resolved, cid);
        s.append_response(
            Some(cid.clone()),
            None,
            vec![item("user", "more")],
            vec![ConversationItem {
                response_id: Some("resp_2".into()),
                ..item("assistant", "sure")
            }],
            "resp_2".into(),
        )
        .await
        .unwrap();

        // Reload from durable storage → same 4 items.
        let s2 = AppStateStore::new(store, "app_state");
        s2.load().await.unwrap();
        let conv = s2.get_conversation(&cid).await.unwrap();
        assert_eq!(conv.items.len(), 4);
        assert_eq!(conv.items[3].text.as_deref(), Some("sure"));
        assert_eq!(
            s2.conversation_by_response("resp_2").await.as_deref(),
            Some(cid.as_str())
        );
    }

    #[tokio::test]
    async fn dashboard_upsert_delete_reload() {
        let store = mem_store();
        let s = AppStateStore::new(store.clone(), "app_state");
        let saved = s
            .upsert_dashboard(Dashboard {
                id: String::new(),
                title: "Errors".into(),
                created_at_ns: 0,
                updated_at_ns: 0,
                rows: vec![DashboardRow {
                    height: DEFAULT_ROW_HEIGHT,
                    panels: vec![RowPanel {
                        visualization: SavedVisualization {
                            seql: "logs last 1h".into(),
                            title: "Recent logs".into(),
                            shape: Some("table".into()),
                        },
                        weight: 1.0,
                    }],
                }],
            })
            .await
            .unwrap();
        assert!(!saved.id.is_empty());
        assert_eq!(saved.panel_count(), 1);

        let s2 = AppStateStore::new(store.clone(), "app_state");
        s2.load().await.unwrap();
        assert_eq!(s2.dashboards_snapshot().await.len(), 1);

        s.delete_dashboard(&saved.id).await.unwrap();
        let s3 = AppStateStore::new(store, "app_state");
        s3.load().await.unwrap();
        assert!(s3.dashboards_snapshot().await.is_empty());
    }
}
