//! DataFusion-based SeQL query backend
//!
//! Implements `QueryApi` and `QueryExec` from `sequins-query` using multi-root Substrait plans.
//!
//! # Architecture
//!
//! ```text
//!   QueryApi::query(seql)
//!       │
//!       ├─ compile(seql, ctx) → Substrait Plan bytes (with SeqlExtension)
//!       │
//!       ▼
//!   QueryExec::execute(plan_bytes)
//!       │
//!       ├─ decode Plan → extract SeqlExtension
//!       ├─ for each plan root: DefaultSubstraitConsumer::consume_rel() → LogicalPlan
//!       ├─ execute each LogicalPlan → RecordBatches → FlightData (tagged with table alias)
//!       │
//!       ▼
//!   SeqlStream<FlightData>
//! ```
//!
//! # Signal Tables Registered
//!
//! - All signal types via `registration::SIGNAL_TABLE_DEFS` — union providers (hot + cold)

use super::storage::Storage;
use datafusion::execution::context::SessionContext;
use sequins_query::error::QueryError;
use std::sync::Arc;

// Submodules
mod arrow_convert;
mod execution;
mod registration;
mod trait_impls;

// Re-exports are available if needed by other modules
// (Currently unused but kept for potential future use)

/// Convenience constructor for `QueryError::Execution`
pub(crate) fn exec_err(msg: impl Into<String>) -> QueryError {
    QueryError::Execution {
        message: msg.into(),
    }
}

/// DataFusion-based SeQL query executor
///
/// Wraps a [`Storage`] and translates SeQL queries into DataFusion SQL plans.
pub struct DataFusionBackend {
    storage: Arc<Storage>,
}

impl DataFusionBackend {
    /// Create a new backend wrapping the given storage
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    /// Build a `SessionContext` with all signal tables registered (hot + cold union)
    async fn make_session_ctx(&self) -> Result<SessionContext, QueryError> {
        let ctx = SessionContext::new();
        // Register overflow attribute extraction UDFs so that attr.* queries on
        // non-promoted attributes work (overflow_get_str, overflow_get_i64, etc.)
        sequins_otlp::register_overflow_udfs(&ctx);
        let hot_tier = self.storage.hot_tier_arc();
        let cold_tier = self.storage.cold_tier_arc();

        // Get cold tier base path
        let base_path = self
            .storage
            .config()
            .cold_tier
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.storage.config().cold_tier.uri);

        // Register all signal tables using data-driven approach
        for def in registration::SIGNAL_TABLE_DEFS {
            registration::register_union_table(
                &ctx,
                def,
                hot_tier.clone(),
                base_path,
                cold_tier.clone(),
            )
            .await?;
        }

        Ok(ctx)
    }

    /// Build a `SessionContext` with ONLY hot tier tables (for live query enrichment)
    ///
    /// This is used for enrichment during live queries because hot tier is fast
    /// while cold tier (Vortex) is too slow for real-time enrichment.
    pub fn make_hot_tier_session_ctx(&self) -> SessionContext {
        let ctx = SessionContext::new();
        sequins_otlp::register_overflow_udfs(&ctx);
        let hot_tier = self.storage.hot_tier_arc();

        // Register only hot tier providers, not the union hot+cold providers
        // We'll use blocking registration since enrichment setup is not performance-critical
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                for def in registration::hot_only_defs() {
                    let _ =
                        registration::register_hot_only_table(&ctx, def, hot_tier.clone()).await;
                }
            })
        });

        ctx
    }
}
