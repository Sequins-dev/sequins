//! Union TableProvider that queries both hot and cold tiers for any signal type

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Union provider that queries both hot and cold tiers for any signal type
pub struct SignalUnionProvider {
    hot_provider: Arc<dyn TableProvider>,
    cold_provider: Arc<dyn TableProvider>,
    schema: SchemaRef,
}

impl std::fmt::Debug for SignalUnionProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignalUnionProvider")
            .field("schema", &self.schema)
            .finish()
    }
}

impl SignalUnionProvider {
    /// Create a new union provider with the given schema
    pub fn new(
        hot_provider: Arc<dyn TableProvider>,
        cold_provider: Arc<dyn TableProvider>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            hot_provider,
            cold_provider,
            schema,
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for SignalUnionProvider {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Do not push `limit` into either provider independently: each would
        // return up to `limit` rows, the UnionExec would concatenate up to
        // 2×limit rows, then a downstream GlobalLimitExec trims to `limit`.
        // That over-scans cold tier (expensive Vortex reads) for no benefit.
        // The caller's GlobalLimitExec enforces the final limit correctly.
        let _ = limit; // intentionally unused
        let hot_plan = self
            .hot_provider
            .scan(state, projection, filters, None)
            .await?;

        let cold_plan = self
            .cold_provider
            .scan(state, projection, filters, None)
            .await?;

        // Validate schema compatibility before creating UnionExec
        let hot_schema = hot_plan.schema();
        let cold_schema = cold_plan.schema();

        if hot_schema.fields().len() != cold_schema.fields().len() {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "Schema mismatch between hot tier ({} fields) and cold tier ({} fields)",
                hot_schema.fields().len(),
                cold_schema.fields().len()
            )));
        }

        // No dedup predicate is needed here.  The hot-tier compaction loop always
        // unlinks a completed batch from the chain (via CAS) *before* calling the
        // cold-flush callback, so a row can only ever be visible in one tier at a
        // time.  The brief window where a flushed batch is in neither tier is
        // acceptable for near-real-time analytics.
        UnionExec::try_new(vec![hot_plan, cold_plan])
    }
}

// Union provider integration tests live in sequins-datafusion-backend
// (test_union_provider_hot_and_cold, test_union_provider_with_filters)
// to avoid a circular dev-dependency between sequins-storage and sequins-datafusion-backend.
