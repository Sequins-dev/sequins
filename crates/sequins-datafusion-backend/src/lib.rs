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
use seql_substrait::seql_ext::QueryScope;
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

pub use execution::MemtableFraming;

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
    /// Lazily-initialised, permanently-cached session contexts — one per
    /// [`QueryScope`] (indexed by `scope as usize`: All / HotOnly / ColdOnly).
    /// Each registers the appropriate tier providers and is reused across queries.
    ctx_cache: [OnceCell<SessionContext>; 3],
}

impl DataFusionBackend {
    /// Create a new backend wrapping the given storage
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            ctx_cache: std::array::from_fn(|_| OnceCell::new()),
        }
    }

    /// Return the `All`-scope session context (used for compiling SeQL — the
    /// scope only affects execution). Distributed execution selects a scoped
    /// context via [`Self::session_ctx_for_scope`].
    async fn make_session_ctx(&self) -> Result<SessionContext, QueryError> {
        self.session_ctx_for_scope(QueryScope::All).await
    }

    /// Return the cached `SessionContext` for `scope`, initialising it on first use.
    pub(crate) async fn session_ctx_for_scope(
        &self,
        scope: QueryScope,
    ) -> Result<SessionContext, QueryError> {
        let ctx = self.ctx_cache[scope as usize]
            .get_or_try_init(|| self.build_session_ctx(scope))
            .await?;
        Ok(ctx.clone())
    }

    /// Re-run an aggregating plan over caller-supplied in-memory batches per
    /// signal table, framing the result per `framing`.
    ///
    /// The distributed coordinator gathers the raw rows of the plan's primary
    /// signal from every cluster node, then calls this to re-aggregate over the
    /// union — so a cluster-wide `count`/percentile/health rollup is correct even
    /// though each node only holds its own hot data. `watermark_ns` is stamped on
    /// the emitted `Schema`/`Replace` frames. Signals not present in `tables` are
    /// registered as empty tables so every table reference still resolves.
    pub async fn execute_over_memtables(
        &self,
        tables: Vec<(String, Vec<arrow::record_batch::RecordBatch>)>,
        plan_bytes: Vec<u8>,
        framing: MemtableFraming,
        watermark_ns: u64,
    ) -> Result<sequins_traits::SeqlStream, QueryError> {
        let ctx = registration::build_memtable_ctx(&tables)?;
        execution::execute_over_memtables(plan_bytes, ctx, framing, watermark_ns).await
    }

    async fn build_session_ctx(&self, scope: QueryScope) -> Result<SessionContext, QueryError> {
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
                scope,
            )
            .await?;
        }

        Ok(ctx)
    }
}
