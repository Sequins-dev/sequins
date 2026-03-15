//! LiveTableProvider - DataFusion TableProvider for live streaming queries
//!
//! Registers signal tables that back into the WAL broadcast channel, allowing
//! DataFusion's full optimizer pipeline to apply to live queries.

use crate::live_source_exec::LiveSourceExec;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use sequins_query::ast::Signal;
use std::any::Any;
use std::sync::Arc;
use tokio::sync::broadcast;

/// A DataFusion TableProvider backed by a WAL broadcast channel.
///
/// When DataFusion calls `scan()`, it creates a `LiveSourceExec` which subscribes
/// to the broadcast channel and emits RecordBatches as new data arrives.
#[derive(Debug)]
pub struct LiveTableProvider {
    signal: Signal,
    schema: SchemaRef,
    broadcast_tx: broadcast::Sender<(Signal, Arc<RecordBatch>)>,
}

impl LiveTableProvider {
    pub fn new(
        signal: Signal,
        schema: SchemaRef,
        broadcast_tx: broadcast::Sender<(Signal, Arc<RecordBatch>)>,
    ) -> Self {
        Self {
            signal,
            schema,
            broadcast_tx,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for LiveTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LiveSourceExec::new(
            self.signal,
            self.schema.clone(),
            self.broadcast_tx.clone(),
            projection.cloned(),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::TableType;
    use datafusion::execution::SessionStateBuilder;
    use sequins_query::ast::Signal;
    use std::sync::Arc;

    fn make_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    #[test]
    fn test_live_table_provider_returns_correct_schema() {
        let schema = make_test_schema();
        let (tx, _rx) = broadcast::channel::<(Signal, Arc<arrow::array::RecordBatch>)>(16);
        let provider = LiveTableProvider::new(Signal::Spans, schema.clone(), tx);

        // schema() should return the exact schema we passed in
        let returned_schema = provider.schema();
        assert_eq!(returned_schema, schema);
        assert_eq!(returned_schema.fields().len(), 2);
        assert_eq!(returned_schema.field(0).name(), "id");
        assert_eq!(returned_schema.field(1).name(), "value");
    }

    #[test]
    fn test_live_table_provider_table_type_is_temporary() {
        let schema = make_test_schema();
        let (tx, _rx) = broadcast::channel::<(Signal, Arc<arrow::array::RecordBatch>)>(16);
        let provider = LiveTableProvider::new(Signal::Logs, schema, tx);

        assert_eq!(provider.table_type(), TableType::Temporary);
    }

    #[tokio::test]
    async fn test_live_table_provider_scan_creates_live_source_exec() {
        let schema = make_test_schema();
        let (tx, _rx) = broadcast::channel::<(Signal, Arc<arrow::array::RecordBatch>)>(16);
        let provider = LiveTableProvider::new(Signal::Spans, schema.clone(), tx);

        // Build a minimal session state for the scan call
        let session_state = SessionStateBuilder::new().build();

        let plan = provider
            .scan(&session_state, None, &[], None)
            .await
            .unwrap();

        // The returned plan should be a LiveSourceExec
        assert!(
            plan.as_any().is::<LiveSourceExec>(),
            "scan() should return a LiveSourceExec"
        );

        // Its schema should match the provider schema
        assert_eq!(plan.schema(), schema);
    }

    #[tokio::test]
    async fn test_live_table_provider_scan_with_projection() {
        let schema = make_test_schema();
        let (tx, _rx) = broadcast::channel::<(Signal, Arc<arrow::array::RecordBatch>)>(16);
        let provider = LiveTableProvider::new(Signal::Metrics, schema.clone(), tx);

        let session_state = SessionStateBuilder::new().build();

        // Project only column 0 (id)
        let projection = vec![0usize];
        let plan = provider
            .scan(&session_state, Some(&projection), &[], None)
            .await
            .unwrap();

        assert!(plan.as_any().is::<LiveSourceExec>());

        // Projected schema should only have the first field
        assert_eq!(plan.schema().fields().len(), 1);
        assert_eq!(plan.schema().field(0).name(), "id");
    }
}
