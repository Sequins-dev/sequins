//! Union TableProvider that queries both hot and cold tiers for any signal type

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::logical_expr::{Expr, TableType};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use std::any::Any;
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
        state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Get execution plans from both providers
        let hot_plan = self
            .hot_provider
            .scan(state, projection, filters, limit)
            .await?;

        let cold_plan = self
            .cold_provider
            .scan(state, projection, filters, limit)
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

        // Combine them with UnionExec
        UnionExec::try_new(vec![hot_plan, cold_plan])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datafusion_backend::DataFusionBackend;
    use crate::test_fixtures::{make_test_otlp_traces, TestStorageBuilder};
    use futures::StreamExt;
    use sequins_query::QueryApi;
    use sequins_types::ingest::OtlpIngest;

    #[tokio::test]
    async fn test_union_provider_hot_and_cold() {
        // Create storage with short flush interval to ensure data gets to cold tier
        let (storage, _temp) = TestStorageBuilder::new()
            .flush_interval(sequins_types::models::Duration::from_millis(100))
            .build()
            .await;

        // Ingest data to hot tier
        let request1 = make_test_otlp_traces(1, 5);
        storage.ingest_traces(request1).await.unwrap();

        // Wait for flush to cold tier
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        // Ingest more data to hot tier (should stay in hot tier)
        let request2 = make_test_otlp_traces(1, 3);
        storage.ingest_traces(request2).await.unwrap();

        // Create backend and query - should get data from both tiers
        let backend = DataFusionBackend::new(Arc::new(storage));
        let query = "spans last 1h LIMIT 100";
        let mut stream = backend.query(query).await.unwrap();

        // Collect results
        use sequins_query::flight::{decode_metadata, SeqlMetadata};
        let mut frames = Vec::new();
        while let Some(result) = stream.next().await {
            frames.push(result.unwrap());
        }

        // Should have data from both hot and cold tiers
        assert!(!frames.is_empty(), "Should have frames");

        // Verify we got data frame
        let has_data_frame = frames.iter().any(|f| {
            decode_metadata(&f.app_metadata)
                .map_or(false, |m| matches!(m, SeqlMetadata::Data { .. }))
        });
        assert!(has_data_frame, "Should have at least one data frame");
    }

    #[tokio::test]
    async fn test_union_provider_with_filters() {
        // Create storage
        let (storage, _temp) = TestStorageBuilder::new().build().await;

        // Ingest test data
        let request = make_test_otlp_traces(1, 10);
        storage.ingest_traces(request).await.unwrap();

        // Create backend and query with filters
        let backend = DataFusionBackend::new(Arc::new(storage));
        let query = "spans last 1h WHERE kind = server LIMIT 50";
        let mut stream = backend.query(query).await.unwrap();

        // Collect results
        use sequins_query::flight::{decode_metadata, SeqlMetadata};
        let mut frames = Vec::new();
        while let Some(result) = stream.next().await {
            frames.push(result.unwrap());
        }

        // Should execute successfully with filters
        assert!(!frames.is_empty(), "Should have frames from filtered query");

        // Verify complete frame exists
        let has_complete = frames.iter().any(|f| {
            decode_metadata(&f.app_metadata)
                .map_or(false, |m| matches!(m, SeqlMetadata::Complete { .. }))
        });
        assert!(has_complete, "Should have complete frame");
    }
}
