//! LiveSourceExec - unbounded DataFusion ExecutionPlan for live streaming
//!
//! Wraps a broadcast receiver and filters by signal to provide a never-ending
//! stream of RecordBatches to DataFusion's pull-based execution engine.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    Statistics,
};
use futures::StreamExt;
use sequins_query::ast::Signal;
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

/// Custom DataFusion ExecutionPlan for live streaming RecordBatches from the WAL broadcast channel.
///
/// This plan has `Boundedness::Unbounded` and `EmissionType::Incremental`, meaning DataFusion
/// understands it will emit batches indefinitely. FilterExec and ProjectionExec placed above
/// it will apply predicates and projections to each batch as it arrives.
pub struct LiveSourceExec {
    signal: Signal,
    schema: SchemaRef,
    /// Column indices to project from each incoming batch, or `None` for all columns.
    projection: Option<Vec<usize>>,
    broadcast_tx: broadcast::Sender<(Signal, Arc<RecordBatch>)>,
    properties: PlanProperties,
}

impl LiveSourceExec {
    pub fn new(
        signal: Signal,
        schema: SchemaRef,
        broadcast_tx: broadcast::Sender<(Signal, Arc<RecordBatch>)>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
        use datafusion::physical_plan::Partitioning;

        // Apply projection to schema if specified
        let projected_schema = if let Some(ref proj) = projection {
            let fields: Vec<_> = proj.iter().map(|&i| schema.field(i).clone()).collect();
            Arc::new(datafusion::arrow::datatypes::Schema::new(fields))
        } else {
            schema.clone()
        };

        let properties = PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Unbounded {
                requires_infinite_memory: false,
            },
        );

        Self {
            signal,
            schema: projected_schema,
            projection,
            broadcast_tx,
            properties,
        }
    }
}

impl fmt::Debug for LiveSourceExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveSourceExec")
            .field("signal", &self.signal)
            .field("schema", &self.schema)
            .finish()
    }
}

impl DisplayAs for LiveSourceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "LiveSourceExec[{:?}]", self.signal)
    }
}

impl ExecutionPlan for LiveSourceExec {
    fn name(&self) -> &str {
        "LiveSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let signal = self.signal;
        let schema = self.schema.clone();
        let projection = self.projection.clone();
        let rx = self.broadcast_tx.subscribe();

        // Convert broadcast receiver to a stream of RecordBatches, filtering by signal.
        // Apply projection to each batch so the emitted data matches the declared schema.
        let stream = BroadcastStream::new(rx).filter_map(move |result| {
            let projection = projection.clone();
            async move {
                match result {
                    Ok((sig, batch)) if sig == signal => {
                        let out = if let Some(ref proj) = projection {
                            batch.project(proj).map_err(|e| {
                                datafusion::error::DataFusionError::ArrowError(Box::new(e), None)
                            })
                        } else {
                            Ok((*batch).clone())
                        };
                        Some(out)
                    }
                    Ok(_) => None,
                    Err(_) => None,
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::context::TaskContext;
    use futures::StreamExt;
    use sequins_query::ast::Signal;
    use std::sync::Arc;
    use std::time::Duration;

    fn make_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn make_test_batch(id: i64, name: &str) -> Arc<RecordBatch> {
        let schema = make_test_schema();
        let id_array = Int64Array::from(vec![id]);
        let name_array = StringArray::from(vec![name]);
        Arc::new(
            RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap(),
        )
    }

    fn make_task_context() -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }

    #[tokio::test]
    async fn test_live_source_exec_streams_incoming_batches() {
        let schema = make_test_schema();
        let (tx, _rx) = broadcast::channel(16);
        let exec = LiveSourceExec::new(Signal::Spans, schema.clone(), tx.clone(), None);

        // Start executing before sending any batches
        let ctx = make_task_context();
        let mut stream = exec.execute(0, ctx).unwrap();

        // Send a batch on the broadcast channel
        let batch = make_test_batch(42, "test");
        tx.send((Signal::Spans, batch)).unwrap();

        // Should receive the batch within timeout
        let result = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("timeout waiting for batch");

        let record_batch = result.unwrap().expect("stream error");
        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 2);
    }

    #[tokio::test]
    async fn test_live_source_exec_filters_by_signal() {
        let schema = make_test_schema();
        let (tx, _rx) = broadcast::channel(16);
        let exec = LiveSourceExec::new(Signal::Spans, schema.clone(), tx.clone(), None);

        let ctx = make_task_context();
        let mut stream = exec.execute(0, ctx).unwrap();

        // Send a Logs batch (should be filtered out)
        let logs_batch = make_test_batch(1, "logs_item");
        tx.send((Signal::Logs, logs_batch)).unwrap();

        // Send a Spans batch (should be received)
        let spans_batch = make_test_batch(2, "spans_item");
        tx.send((Signal::Spans, spans_batch)).unwrap();

        // The first item we receive should be the Spans batch, not Logs
        let result = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("timeout waiting for batch");

        let record_batch = result.unwrap().expect("stream error");
        assert_eq!(record_batch.num_rows(), 1);

        // Verify it's the spans batch (id=2)
        let id_col = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 2, "should receive spans batch, not logs");
    }

    #[tokio::test]
    async fn test_live_source_exec_schema_matches() {
        let schema = make_test_schema();
        let (tx, _rx) = broadcast::channel(16);
        let exec = LiveSourceExec::new(Signal::Logs, schema.clone(), tx.clone(), None);

        // Schema should match what was passed in
        assert_eq!(exec.schema(), schema);
        assert_eq!(exec.schema().fields().len(), 2);
    }

    #[tokio::test]
    async fn test_live_source_exec_with_projection() {
        let schema = make_test_schema();
        let (tx, _rx) = broadcast::channel(16);
        // Project only the first column (id)
        let exec = LiveSourceExec::new(Signal::Spans, schema.clone(), tx.clone(), Some(vec![0]));

        // Projected schema should only have 1 column
        assert_eq!(exec.schema().fields().len(), 1);
        assert_eq!(exec.schema().field(0).name(), "id");

        // The actual batch emitted must also be projected (not the full 2-column batch)
        let ctx = make_task_context();
        let mut stream = exec.execute(0, ctx).unwrap();

        let batch = make_test_batch(99, "should_be_dropped");
        tx.send((Signal::Spans, batch)).unwrap();

        let result = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("timeout waiting for projected batch");

        let record_batch = result.unwrap().expect("stream error");
        assert_eq!(
            record_batch.num_columns(),
            1,
            "projected batch should have 1 column"
        );
        assert_eq!(record_batch.schema().field(0).name(), "id");
        let id_col = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_col.value(0), 99);
    }

    #[test]
    fn test_live_source_exec_is_unbounded() {
        use datafusion::physical_plan::execution_plan::Boundedness;

        let schema = make_test_schema();
        let (tx, _rx) = broadcast::channel(16);
        let exec = LiveSourceExec::new(Signal::Spans, schema, tx, None);

        let props = exec.properties();
        assert!(
            matches!(props.boundedness, Boundedness::Unbounded { .. }),
            "LiveSourceExec should be unbounded"
        );
    }

    #[tokio::test]
    async fn test_broadcast_lag_does_not_terminate_stream() {
        // Use a channel with capacity = 2 so we can overflow it
        let capacity = 2;
        let schema = make_test_schema();
        let (tx, _rx) = broadcast::channel(capacity);
        let exec = LiveSourceExec::new(Signal::Spans, schema.clone(), tx.clone(), None);

        let ctx = make_task_context();
        let mut stream = exec.execute(0, ctx).unwrap();

        // Overfill the channel (capacity + 1 extra batches) before the consumer runs.
        // This causes the stream's lagged receiver to miss earlier messages.
        for i in 0..(capacity + 1) {
            let _ = tx.send((Signal::Spans, make_test_batch(i as i64, "overflow")));
        }

        // Send one more that should definitely arrive (channel has room now since we overfilled)
        let sentinel = make_test_batch(999, "sentinel");
        tx.send((Signal::Spans, sentinel)).unwrap();

        // The stream should still be alive and eventually yield without panicking.
        // BroadcastStream converts Lagged errors to None (filtered out); the stream continues.
        let result = tokio::time::timeout(Duration::from_millis(500), stream.next()).await;
        // We just verify the stream doesn't terminate unexpectedly; it may yield Ok or timeout.
        // The key assertion is that we didn't panic.
        assert!(
            result.is_ok(),
            "stream should remain alive after broadcast lag, not timeout"
        );
    }
}
