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
pub(crate) mod app_state;
pub mod arrow_convert;
pub(crate) mod execution;
pub mod registration;
mod trait_impls;
pub(crate) mod union_provider;

pub use execution::MemtableFraming;
pub use registration::{hot_signal_tables, hot_signal_type_for_table};

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
    /// A context registering **only** the app-state tables
    /// (`conversations`/`messages`/`dashboards`) — no signal/tier providers. Used by
    /// [`execute_app_state_sql`](Self::execute_app_state_sql) so reading chat history
    /// and dashboards never depends on (or triggers) cold-tier Vortex schema
    /// inference, keeping those reads fast and immune to telemetry-tier issues.
    app_state_ctx: OnceCell<SessionContext>,
}

impl DataFusionBackend {
    /// Create a new backend wrapping the given storage
    pub fn new(storage: Arc<Storage>) -> Self {
        Self {
            storage,
            ctx_cache: std::array::from_fn(|_| OnceCell::new()),
            app_state_ctx: OnceCell::new(),
        }
    }

    /// The [`Storage`] this backend wraps. Exposed so the distributed
    /// coordinator can reach the node-local hot tier for two-phase execution.
    pub fn storage(&self) -> &Arc<Storage> {
        &self.storage
    }

    /// Run a compiled Substrait plan as a **snapshot** over a caller-supplied
    /// [`SessionContext`], returning the framed `SeqlStream`.
    ///
    /// The distributed two-phase coordinator uses this to execute the query over
    /// a *distributed* session (one whose signal tables scan node-local hot tiers
    /// via `HotScanExec` and whose planner fans partial aggregates to workers),
    /// reusing this crate's snapshot framing (Schema/Data/Complete, auxiliary
    /// plans, column defs) instead of duplicating it.
    pub async fn execute_snapshot_with_ctx(
        &self,
        ctx: SessionContext,
        plan_bytes: Vec<u8>,
    ) -> Result<sequins_traits::SeqlStream, QueryError> {
        execution::execute_snapshot(&self.storage, plan_bytes, async move { Ok(ctx) }).await
    }

    /// Build a **cold-only** scan `ExecutionPlan` for a signal's registration
    /// table (the shared Vortex tier, unioned across all node prefixes).
    ///
    /// The distributed two-phase coordinator's leaf uses this so one designated
    /// task additionally contributes the shared cold data to its `Partial`
    /// aggregate — read exactly once cluster-wide, so a two-phase aggregation is
    /// correct even for windows whose data has aged into cold.
    pub async fn cold_scan_exec(
        &self,
        table_name: &str,
        projection: Option<&Vec<usize>>,
    ) -> Result<Arc<dyn datafusion::physical_plan::ExecutionPlan>, QueryError> {
        let ctx = self.session_ctx_for_scope(QueryScope::ColdOnly).await?;
        let provider = ctx
            .table_provider(table_name)
            .await
            .map_err(|e| exec_err(format!("cold_scan_exec: table {table_name}: {e}")))?;
        let state = ctx.state();
        provider
            .scan(&state, projection, &[], None)
            .await
            .map_err(|e| exec_err(format!("cold_scan_exec: scan {table_name}: {e}")))
    }

    /// Return the `All`-scope session context (used for compiling SeQL — the
    /// scope only affects execution). Distributed execution selects a scoped
    /// context via [`Self::session_ctx_for_scope`].
    async fn make_session_ctx(&self) -> Result<SessionContext, QueryError> {
        self.session_ctx_for_scope(QueryScope::All).await
    }

    /// The `All`-scope DataFusion [`SessionContext`] with every signal table
    /// registered (hot ∪ cold), for in-process tooling that needs direct
    /// DataFrame/SQL access alongside the SeQL query path — e.g. the assistant's
    /// data-exploration lane (`describe_schema`, `column_profile`, `run_sql`, …).
    ///
    /// The context is lazily built and cached like the query path's, so tables
    /// reflect current data at scan time. Read-only by construction: only the
    /// union signal-table providers are registered, so ad-hoc SQL can scan but
    /// not mutate storage.
    pub async fn session(&self) -> Result<SessionContext, QueryError> {
        self.session_ctx_for_scope(QueryScope::All).await
    }

    /// Execute a **read-only** plain SQL query (SELECT only) against the `All`-scope
    /// context and return a framed `SeqlStream` (shape `Table`). Lets clients read
    /// the app-state tables (`conversations`/`messages`/`dashboards`) and any other
    /// registered DataFusion table that SeQL does not address. DDL/DML/statements are
    /// rejected by [`read_only_sql_options`](execution::read_only_sql_options).
    pub async fn execute_sql_query(
        &self,
        sql: &str,
    ) -> Result<sequins_traits::SeqlStream, QueryError> {
        let ctx = self.session().await?;
        let watermark = self.storage.wal().last_seq();
        execution::execute_sql_snapshot(ctx, sql, watermark).await
    }

    /// Execute a **read-only** plain SQL query against a context that registers
    /// *only* the durable app-state tables (`conversations`/`messages`/`dashboards`).
    ///
    /// Unlike [`execute_sql_query`](Self::execute_sql_query) — which uses the `All`
    /// scope and so registers the hot+cold signal tables (triggering cold-tier Vortex
    /// schema inference) — this path never touches the telemetry tiers. Reading chat
    /// history therefore can't be blocked or destabilised by a cold-tier problem
    /// (e.g. a Vortex type it can't infer), and skips the cold-listing overhead. This
    /// is the entry point the desktop app uses for its conversation/message reads.
    pub async fn execute_app_state_sql(
        &self,
        sql: &str,
    ) -> Result<sequins_traits::SeqlStream, QueryError> {
        let ctx = self
            .app_state_ctx
            .get_or_try_init(|| async {
                let ctx = SessionContext::new();
                app_state::register_app_state_tables(&ctx, self.storage.app_state().clone())?;
                Ok::<_, QueryError>(ctx)
            })
            .await?
            .clone();
        let watermark = self.storage.wal().last_seq();
        execution::execute_sql_snapshot(ctx, sql, watermark).await
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

        let cold_uri = self.storage.config().cold_tier.uri.clone();
        // Register the cold-tier object store so DataFusion can resolve cloud
        // (s3://, gs://, az://) cold-table URLs; file:// uses its built-in store.
        registration::register_cold_object_store(&ctx, &cold_uri, &cold_tier).await;

        for def in registration::SIGNAL_TABLE_DEFS {
            registration::register_union_table(
                &ctx,
                def,
                hot_tier.clone(),
                &cold_uri,
                cold_tier.clone(),
                scope,
            )
            .await?;
        }

        // Project the durable app-state store (conversations/messages/dashboards)
        // into queryable tables alongside the signal tables, so the assistant's
        // `run_sql` and any SQL client can read chat history and dashboards.
        app_state::register_app_state_tables(&ctx, self.storage.app_state().clone())?;

        Ok(ctx)
    }
}
