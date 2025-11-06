//! Custom DataFusion ExecutionPlan for HotTier
//!
//! This module provides the execution plan that:
//! - Lazily converts HotTier spans to RecordBatches
//! - Applies filter pushdown to minimize conversion overhead
//! - Streams results efficiently

use crate::hot_tier::HotTier;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::TaskContext;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    Statistics,
};
use futures::stream;
use sequins_core::models::{Span, Timestamp};
use std::any::Any;
use std::fmt;
use std::sync::Arc;

/// Custom execution plan for scanning HotTier
///
/// This plan converts the HotTier HashMap to RecordBatches on-demand,
/// with support for filter pushdown and projections.
pub struct HotTierScanExec {
    hot_tier: Arc<HotTier>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    filters: Vec<Expr>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl HotTierScanExec {
    pub fn new(
        hot_tier: Arc<HotTier>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filters: Vec<Expr>,
        limit: Option<usize>,
    ) -> Self {
        use datafusion::physical_expr::EquivalenceProperties;
        use datafusion::physical_plan::{ExecutionMode, Partitioning};

        // Create plan properties
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        );

        Self {
            hot_tier,
            schema,
            projection,
            filters,
            limit,
            properties,
        }
    }

    /// Convert filters to a predicate function
    ///
    /// For now, we'll support basic time range filters. More complex
    /// filter translation can be added later.
    fn build_predicate(&self) -> Box<dyn Fn(&Span) -> bool + Send + Sync> {
        // Extract time range from filters if present
        let (min_time, max_time) = self.extract_time_range();

        Box::new(move |span: &Span| {
            if let Some(min) = min_time {
                if span.start_time < min {
                    return false;
                }
            }
            if let Some(max) = max_time {
                if span.start_time > max {
                    return false;
                }
            }
            true
        })
    }

    /// Extract time range from filters
    ///
    /// This is a simplified implementation that looks for timestamp comparisons.
    /// A full implementation would translate all Expr types.
    fn extract_time_range(&self) -> (Option<Timestamp>, Option<Timestamp>) {
        // For Phase 2, we'll start with a simple implementation
        // that accepts all spans. Filter pushdown can be enhanced
        // in a future phase.
        //
        // TODO: Implement full Expr -> predicate translation
        (None, None)
    }
}

impl std::fmt::Debug for HotTierScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HotTierScanExec")
            .field("schema", &self.schema)
            .field("projection", &self.projection)
            .field("filters", &self.filters)
            .field("limit", &self.limit)
            .finish()
    }
}

impl DisplayAs for HotTierScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HotTierScanExec: projection={:?}, limit={:?}",
            self.projection, self.limit
        )
    }
}

impl ExecutionPlan for HotTierScanExec {
    fn name(&self) -> &str {
        "HotTierScanExec"
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
        // Get all spans from hot tier
        let spans = self.hot_tier.get_all_spans();

        // Apply filter pushdown
        let predicate = self.build_predicate();
        let filtered_spans: Vec<Span> = spans
            .into_iter()
            .filter(|span| predicate(span))
            .take(self.limit.unwrap_or(usize::MAX))
            .collect();

        // Convert to RecordBatch with full schema
        let full_schema = sequins_core::arrow_schema::span_schema();
        let mut batch = match crate::cold_tier::ColdTier::spans_to_record_batch(
            filtered_spans,
            full_schema.clone(),
        ) {
            Ok(batch) => batch,
            Err(e) => {
                return Err(datafusion::error::DataFusionError::Execution(format!(
                    "Failed to convert spans to RecordBatch: {}",
                    e
                )))
            }
        };

        // Apply projection if specified
        if let Some(ref projection_indices) = self.projection {
            let projected_columns: Vec<_> = projection_indices
                .iter()
                .map(|&i| batch.column(i).clone())
                .collect();

            let projected_fields: Vec<datafusion::arrow::datatypes::Field> = projection_indices
                .iter()
                .map(|&i| full_schema.field(i).clone())
                .collect();

            let projected_schema =
                std::sync::Arc::new(datafusion::arrow::datatypes::Schema::new(projected_fields));

            batch = datafusion::arrow::record_batch::RecordBatch::try_new(
                projected_schema,
                projected_columns,
            )
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?;
        }

        // Create a stream with a single batch
        let stream = stream::once(async move { Ok(batch) });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }

    fn statistics(&self) -> datafusion::error::Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema))
    }
}

/// Adapter to convert a stream of RecordBatches into SendableRecordBatchStream
struct RecordBatchStreamAdapter {
    schema: SchemaRef,
    stream: std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<RecordBatch, datafusion::error::DataFusionError>>
                + Send,
        >,
    >,
}

impl RecordBatchStreamAdapter {
    fn new<S>(schema: SchemaRef, stream: S) -> Self
    where
        S: futures::Stream<Item = Result<RecordBatch, datafusion::error::DataFusionError>>
            + Send
            + 'static,
    {
        Self {
            schema,
            stream: Box::pin(stream),
        }
    }
}

impl futures::Stream for RecordBatchStreamAdapter {
    type Item = Result<RecordBatch, datafusion::error::DataFusionError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}

impl datafusion::physical_plan::RecordBatchStream for RecordBatchStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
