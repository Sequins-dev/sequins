//! Custom DataFusion TableProvider for HotTier
//!
//! This module provides a production-ready TableProvider implementation that:
//! - Provides lazy conversion (only converts spans that pass filters)
//! - Supports filter pushdown to the hot tier HashMap
//! - Returns a custom ExecutionPlan (HotTierScanExec)
//!
//! This replaces the MemTable prototype from Phase 1.

use crate::hot_tier::HotTier;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::ExecutionPlan;
use sequins_core::arrow_schema;
use std::any::Any;
use std::sync::Arc;

/// Custom TableProvider for HotTier in-memory spans
///
/// This provider allows DataFusion to query the HotTier HashMap directly,
/// with support for filter pushdown and lazy conversion to RecordBatches.
pub struct HotTierTableProvider {
    hot_tier: Arc<HotTier>,
    schema: SchemaRef,
}

impl HotTierTableProvider {
    /// Create a new HotTierTableProvider
    pub fn new(hot_tier: Arc<HotTier>) -> Self {
        Self {
            hot_tier,
            schema: arrow_schema::span_schema(),
        }
    }
}

#[async_trait::async_trait]
impl TableProvider for HotTierTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        use crate::hot_tier_exec::HotTierScanExec;

        // Create the execution plan with filter pushdown
        Ok(Arc::new(HotTierScanExec::new(
            self.hot_tier.clone(),
            self.schema.clone(),
            projection.cloned(),
            filters.to_vec(),
            limit,
        )))
    }
}
