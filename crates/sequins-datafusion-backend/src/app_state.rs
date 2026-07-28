//! DataFusion table providers projecting the durable app-state store
//! ([`sequins_metadata::AppStateStore`]) into queryable tables: `conversations`,
//! `messages`, and `dashboards`.
//!
//! Each provider snapshots the current store state at **scan time** (building a
//! fresh in-memory batch and delegating to a `MemTable`), so mutations are visible
//! to queries without rebuilding the `OnceCell`-cached `SessionContext`. Registered
//! alongside the signal tables in `build_session_ctx`, so the assistant's own
//! `run_sql` tool — and any SQL client — can read chat history and dashboards.

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::datasource::{MemTable, TableProvider};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use sequins_metadata::AppStateStore;

use crate::exec_err;
use sequins_traits::QueryError;

/// Register `conversations`, `messages`, and `dashboards` into `ctx`.
pub(crate) fn register_app_state_tables(
    ctx: &datafusion::prelude::SessionContext,
    store: Arc<AppStateStore>,
) -> Result<(), QueryError> {
    let providers: Vec<(&str, Arc<dyn TableProvider>)> = vec![
        (
            "conversations",
            Arc::new(AppStateTable::new(store.clone(), AppTable::Conversations)),
        ),
        (
            "messages",
            Arc::new(AppStateTable::new(store.clone(), AppTable::Messages)),
        ),
        (
            "dashboards",
            Arc::new(AppStateTable::new(store, AppTable::Dashboards)),
        ),
    ];
    for (name, provider) in providers {
        ctx.register_table(name, provider)
            .map_err(|e| exec_err(format!("Failed to register {name} table: {e}")))?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum AppTable {
    Conversations,
    Messages,
    Dashboards,
}

/// A table provider over one projection of the app-state store.
struct AppStateTable {
    store: Arc<AppStateStore>,
    table: AppTable,
    schema: SchemaRef,
}

impl std::fmt::Debug for AppStateTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppStateTable")
            .field("schema", &self.schema)
            .finish()
    }
}

impl AppStateTable {
    fn new(store: Arc<AppStateStore>, table: AppTable) -> Self {
        Self {
            store,
            schema: schema_for(table),
            table,
        }
    }

    /// Build a fresh batch from the current store snapshot.
    async fn snapshot_batch(&self) -> DfResult<RecordBatch> {
        match self.table {
            AppTable::Conversations => self.conversations_batch().await,
            AppTable::Messages => self.messages_batch().await,
            AppTable::Dashboards => self.dashboards_batch().await,
        }
        .map_err(|e| DataFusionError::Execution(format!("app-state snapshot: {e}")))
    }

    async fn conversations_batch(&self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let convs = self.store.conversations_snapshot().await;
        let ids: Vec<&str> = convs.iter().map(|c| c.id.as_str()).collect();
        let titles: Vec<Option<&str>> = convs.iter().map(|c| c.title.as_deref()).collect();
        let created: Vec<u64> = convs.iter().map(|c| c.created_at_ns).collect();
        let updated: Vec<u64> = convs.iter().map(|c| c.updated_at_ns).collect();
        let counts: Vec<u32> = convs.iter().map(|c| c.items.len() as u32).collect();
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(titles)),
                Arc::new(UInt64Array::from(created)),
                Arc::new(UInt64Array::from(updated)),
                Arc::new(UInt32Array::from(counts)),
            ],
        )
    }

    async fn messages_batch(&self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let convs = self.store.conversations_snapshot().await;
        let mut conv_id = Vec::new();
        let mut resp_id: Vec<Option<String>> = Vec::new();
        let mut position = Vec::new();
        let mut role = Vec::new();
        let mut item_type = Vec::new();
        let mut text: Vec<Option<String>> = Vec::new();
        let mut tool_name: Vec<Option<String>> = Vec::new();
        let mut tool_args: Vec<Option<String>> = Vec::new();
        let mut tool_output: Vec<Option<String>> = Vec::new();
        let mut created = Vec::new();
        for c in &convs {
            for it in &c.items {
                conv_id.push(c.id.clone());
                resp_id.push(it.response_id.clone());
                position.push(it.position);
                role.push(it.role.clone());
                item_type.push(it.item_type.clone());
                text.push(it.text.clone());
                tool_name.push(it.tool_name.clone());
                tool_args.push(it.tool_arguments.clone());
                tool_output.push(it.tool_output.clone());
                created.push(it.created_at_ns);
            }
        }
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(conv_id)) as ArrayRef,
                Arc::new(StringArray::from(resp_id)),
                Arc::new(UInt32Array::from(position)),
                Arc::new(StringArray::from(role)),
                Arc::new(StringArray::from(item_type)),
                Arc::new(StringArray::from(text)),
                Arc::new(StringArray::from(tool_name)),
                Arc::new(StringArray::from(tool_args)),
                Arc::new(StringArray::from(tool_output)),
                Arc::new(UInt64Array::from(created)),
            ],
        )
    }

    async fn dashboards_batch(&self) -> Result<RecordBatch, arrow::error::ArrowError> {
        let dashboards = self.store.dashboards_snapshot().await;
        let ids: Vec<&str> = dashboards.iter().map(|d| d.id.as_str()).collect();
        let titles: Vec<&str> = dashboards.iter().map(|d| d.title.as_str()).collect();
        let created: Vec<u64> = dashboards.iter().map(|d| d.created_at_ns).collect();
        let updated: Vec<u64> = dashboards.iter().map(|d| d.updated_at_ns).collect();
        let panel_count: Vec<u32> = dashboards.iter().map(|d| d.panel_count() as u32).collect();
        let panels_json: Vec<String> = dashboards
            .iter()
            .map(|d| serde_json::to_string(&d.rows).unwrap_or_default())
            .collect();
        RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(titles)),
                Arc::new(UInt64Array::from(created)),
                Arc::new(UInt64Array::from(updated)),
                Arc::new(UInt32Array::from(panel_count)),
                Arc::new(StringArray::from(panels_json)),
            ],
        )
    }
}

#[async_trait::async_trait]
impl TableProvider for AppStateTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::View
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        // Snapshot current state at scan time so mutations are always visible.
        let batch = self.snapshot_batch().await?;
        let mem = MemTable::try_new(self.schema.clone(), vec![vec![batch]])?;
        mem.scan(state, projection, filters, limit).await
    }
}

fn schema_for(table: AppTable) -> SchemaRef {
    let s = |name: &str| Field::new(name, DataType::Utf8, false);
    let s_null = |name: &str| Field::new(name, DataType::Utf8, true);
    let u64f = |name: &str| Field::new(name, DataType::UInt64, false);
    let u32f = |name: &str| Field::new(name, DataType::UInt32, false);
    let fields = match table {
        AppTable::Conversations => vec![
            s("id"),
            s_null("title"),
            u64f("created_at_ns"),
            u64f("updated_at_ns"),
            u32f("item_count"),
        ],
        AppTable::Messages => vec![
            s("conversation_id"),
            s_null("response_id"),
            u32f("position"),
            s("role"),
            s("item_type"),
            s_null("text"),
            s_null("tool_name"),
            s_null("tool_arguments"),
            s_null("tool_output"),
            u64f("created_at_ns"),
        ],
        AppTable::Dashboards => vec![
            s("id"),
            s("title"),
            u64f("created_at_ns"),
            u64f("updated_at_ns"),
            u32f("panel_count"),
            s("panels_json"),
        ],
    };
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use crate::DataFusionBackend;
    use sequins_metadata::ConversationItem;
    use sequins_storage::test_fixtures::TestStorageBuilder;
    use std::sync::Arc;

    fn item(role: &str, item_type: &str, text: &str, resp: Option<&str>) -> ConversationItem {
        ConversationItem {
            response_id: resp.map(str::to_string),
            position: 0,
            role: role.into(),
            item_type: item_type.into(),
            text: Some(text.into()),
            tool_name: None,
            tool_arguments: None,
            tool_output: None,
            created_at_ns: 0,
        }
    }

    #[tokio::test]
    async fn app_state_tables_are_queryable() {
        let (storage, _t) = TestStorageBuilder::new().build().await;
        storage
            .app_state()
            .append_response(
                None,
                Some("First chat".into()),
                vec![item("user", "message", "how many spans?", None)],
                vec![
                    ConversationItem {
                        tool_name: Some("run_sql".into()),
                        tool_output: Some("800".into()),
                        ..item("tool", "sequins.tool_result", "", Some("resp_1"))
                    },
                    item(
                        "assistant",
                        "message",
                        "There are 800 spans.",
                        Some("resp_1"),
                    ),
                ],
                "resp_1".into(),
            )
            .await
            .unwrap();

        let backend = DataFusionBackend::new(Arc::new(storage));
        let ctx = backend.session().await.unwrap();

        let convs = ctx
            .sql("SELECT count(*) AS c FROM conversations")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(convs[0].num_rows(), 1);

        // The tool result row is queryable in `messages`.
        let tool_rows = ctx
            .sql("SELECT tool_name, tool_output FROM messages WHERE item_type = 'sequins.tool_result'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(tool_rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);

        // Dashboards table exists and is empty.
        let dash = ctx
            .sql("SELECT count(*) AS c FROM dashboards")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(dash[0].num_rows(), 1);
    }
}
