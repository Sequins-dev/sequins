//! Miscellaneous write methods (resources, scopes, span links, span events)

use super::cold_tier::ColdTier;
use crate::error::Result;
use arrow::array::RecordBatch;

impl ColdTier {
    pub async fn write_scopes(&self, batch: RecordBatch) -> Result<String> {
        self.write_signal_batch("scopes", batch, None).await
    }

    pub async fn write_span_links(&self, batch: RecordBatch) -> Result<String> {
        self.write_signal_batch("spans/links", batch, None).await
    }

    pub async fn write_span_events(&self, batch: RecordBatch) -> Result<String> {
        self.write_signal_batch("spans/events", batch, None).await
    }

    pub async fn write_resources(&self, batch: RecordBatch) -> Result<String> {
        self.write_signal_batch("resources", batch, None).await
    }
}
