//! DataFusion-based SeQL query backend
//!
//! Implements query execution traits using multi-root Substrait plans.
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

use datafusion::execution::context::SessionContext;
use sequins_storage::Storage;
use sequins_traits::QueryError;
use std::sync::Arc;
use tokio::sync::OnceCell;

// Submodules
mod arrow_convert;
pub(crate) mod execution;
mod registration;
mod trait_impls;
pub(crate) mod union_provider;

/// Convenience constructor for `QueryError::Execution`
pub(crate) fn exec_err(msg: impl Into<String>) -> QueryError {
    QueryError::Execution {
        message: msg.into(),
    }
}

/// DataFusion-based SeQL query executor
///
/// Wraps a [`Storage`] and translates SeQL queries into DataFusion SQL plans.
///
/// The `SessionContext` is created lazily on first use and then cached. Both
/// the `BatchChain` (hot-tier) and `ListingTable` (cold-tier) providers inside
/// it reflect current data at scan time, so the context never becomes stale:
/// - Hot tier: BatchChain reads from the atomic head pointer on every scan.
/// - Cold tier: DataFusion's `ListingTable` lists files at scan time, not registration.
///
/// Caching avoids the per-query overhead of `infer_schema` across all signal tables
/// (which lists and reads every cold-tier Vortex file just to check schema compatibility).
pub struct DataFusionBackend {
    storage: Arc<Storage>,
    /// Lazily-initialised, permanently-cached session context. Initialised once on
    /// first query and reused for all subsequent queries.
    ctx_cache: OnceCell<SessionContext>,
}

impl DataFusionBackend {
    /// Create a new backend wrapping the given storage
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            ctx_cache: OnceCell::new(),
        }
    }

    /// Return the shared `SessionContext`, initialising it on first call.
    async fn make_session_ctx(&self) -> Result<SessionContext, QueryError> {
        // get_or_try_init ensures the expensive setup runs only once, even under
        // concurrent callers — subsequent calls return a clone of the cached context.
        let ctx = self
            .ctx_cache
            .get_or_try_init(|| self.build_session_ctx())
            .await?;
        Ok(ctx.clone())
    }

    async fn build_session_ctx(&self) -> Result<SessionContext, QueryError> {
        let ctx = SessionContext::new();
        sequins_attribute_codec::register_overflow_udfs(&ctx);
        let hot_tier = self.storage.hot_tier_arc();
        let cold_tier = self.storage.cold_tier_arc();

        let base_path = self
            .storage
            .config()
            .cold_tier
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.storage.config().cold_tier.uri);

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
}
