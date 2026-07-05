//! Table registration helpers
//!
//! This module provides data-driven table registration that replaces
//! 18 repetitive registration methods with a single table definition array.

use crate::exec_err;
use crate::union_provider::SignalUnionProvider;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use seql_substrait::seql_ext::QueryScope;
use sequins_arrow_schema::SignalType;
use sequins_cold_tier::ColdTier;
use sequins_hot_tier::HotTier;
use sequins_traits::QueryError;
use std::sync::Arc;

/// Definition of a signal table for registration
#[derive(Clone, Copy)]
pub(super) struct SignalTableDef {
    /// Table name as it appears in SQL queries
    pub table_name: &'static str,
    /// Hot tier signal type
    pub hot_signal_type: SignalType,
    /// Cold tier path suffix (relative to cold_tier base path)
    pub cold_path: &'static str,
    /// Schema function from sequins_arrow_schema::arrow_schema
    pub schema_fn: fn() -> SchemaRef,
}

/// All signal tables with their registration metadata
///
/// This replaces 13 individual `register_*_union()` methods and 5 `register_*_hot_only()` methods.
pub(super) static SIGNAL_TABLE_DEFS: &[SignalTableDef] = &[
    SignalTableDef {
        table_name: "spans",
        hot_signal_type: SignalType::Spans,
        cold_path: "spans",
        schema_fn: sequins_arrow_schema::arrow_schema::span_schema,
    },
    SignalTableDef {
        table_name: "logs",
        hot_signal_type: SignalType::Logs,
        cold_path: "logs",
        schema_fn: sequins_arrow_schema::arrow_schema::log_schema,
    },
    SignalTableDef {
        table_name: "datapoints",
        hot_signal_type: SignalType::Metrics,
        cold_path: "metrics/data",
        schema_fn: sequins_arrow_schema::arrow_schema::series_data_point_schema,
    },
    SignalTableDef {
        table_name: "metrics",
        hot_signal_type: SignalType::MetricsMetadata,
        cold_path: "metrics/metadata",
        schema_fn: sequins_arrow_schema::arrow_schema::metric_schema,
    },
    SignalTableDef {
        table_name: "samples",
        hot_signal_type: SignalType::ProfileSamples,
        cold_path: "profiles/samples",
        schema_fn: sequins_arrow_schema::arrow_schema::profile_samples_schema,
    },
    SignalTableDef {
        table_name: "profiles",
        hot_signal_type: SignalType::ProfilesMetadata,
        cold_path: "profiles/metadata",
        schema_fn: sequins_arrow_schema::arrow_schema::profile_schema,
    },
    SignalTableDef {
        table_name: "histogram_data_points",
        hot_signal_type: SignalType::Histograms,
        cold_path: "metrics/histograms",
        schema_fn: sequins_arrow_schema::arrow_schema::histogram_series_data_point_schema,
    },
    SignalTableDef {
        table_name: "exp_histogram_data_points",
        hot_signal_type: SignalType::ExpHistograms,
        cold_path: "metrics/exp_histograms",
        schema_fn: sequins_arrow_schema::arrow_schema::exp_histogram_data_point_schema,
    },
    SignalTableDef {
        table_name: "profile_stacks",
        hot_signal_type: SignalType::ProfileStacks,
        cold_path: "profiles/stacks",
        schema_fn: sequins_arrow_schema::arrow_schema::profile_stacks_schema,
    },
    SignalTableDef {
        table_name: "profile_frames",
        hot_signal_type: SignalType::ProfileFrames,
        cold_path: "profiles/frames",
        schema_fn: sequins_arrow_schema::arrow_schema::profile_frames_schema,
    },
    SignalTableDef {
        table_name: "profile_mappings",
        hot_signal_type: SignalType::ProfileMappings,
        cold_path: "profiles/mappings",
        schema_fn: sequins_arrow_schema::arrow_schema::profile_mappings_schema,
    },
    SignalTableDef {
        table_name: "resources",
        hot_signal_type: SignalType::Resources,
        cold_path: "resources",
        schema_fn: sequins_arrow_schema::arrow_schema::resource_schema,
    },
    SignalTableDef {
        table_name: "scopes",
        hot_signal_type: SignalType::Scopes,
        cold_path: "scopes",
        schema_fn: sequins_arrow_schema::arrow_schema::scope_schema,
    },
    SignalTableDef {
        table_name: "span_links",
        hot_signal_type: SignalType::SpanLinks,
        cold_path: "spans/links",
        schema_fn: sequins_arrow_schema::arrow_schema::span_links_schema,
    },
    SignalTableDef {
        table_name: "span_events",
        hot_signal_type: SignalType::SpanEvents,
        cold_path: "spans/events",
        schema_fn: sequins_arrow_schema::arrow_schema::span_events_schema,
    },
];

/// Register the table provider for a signal, honouring the [`QueryScope`]:
/// - `HotOnly` → the in-memory `BatchChain` only (peer fan-out; no cold work).
/// - `ColdOnly` → the shared cold Vortex files only (coordinator's single read).
/// - `All` → a union of hot + cold (single-node / coordinator default).
pub(super) async fn register_union_table(
    ctx: &SessionContext,
    def: &SignalTableDef,
    hot_tier: Arc<HotTier>,
    cold_tier_base_path: &str,
    cold_tier: Arc<tokio::sync::RwLock<ColdTier>>,
    scope: QueryScope,
) -> Result<(), QueryError> {
    // Use the BatchChain for this signal type directly as a TableProvider.
    let hot_provider: Arc<dyn TableProvider> = hot_tier.chain_arc(&def.hot_signal_type);
    let schema = (def.schema_fn)();

    // Hot-only needs no cold-tier work at all (skips the expensive infer_schema).
    if scope == QueryScope::HotOnly {
        return register(ctx, def.table_name, hot_provider);
    }

    // Cold provider, or `None` when the cold tier is empty / schema-incompatible.
    let cold_provider =
        build_cold_provider(ctx, def, cold_tier_base_path, &cold_tier, &schema).await?;

    let provider: Arc<dyn TableProvider> = match scope {
        QueryScope::ColdOnly => match cold_provider {
            Some(cold) => cold,
            // Unreadable / absent cold data → an empty table (zero rows, no error).
            None => Arc::new(EmptyTable::new(schema)),
        },
        // All (and any unknown value) → hot ∪ cold, or hot-only when cold is absent.
        _ => match cold_provider {
            Some(cold) => Arc::new(SignalUnionProvider::new(hot_provider, cold, schema)),
            None => hot_provider,
        },
    };

    register(ctx, def.table_name, provider)
}

/// Register a provider under `table_name`, mapping registration errors.
fn register(
    ctx: &SessionContext,
    table_name: &str,
    provider: Arc<dyn TableProvider>,
) -> Result<(), QueryError> {
    ctx.register_table(table_name, provider)
        .map_err(|e| exec_err(format!("Failed to register {} table: {}", table_name, e)))?;
    Ok(())
}

/// Build the cold-tier `ListingTable` provider for a signal, or `None` when the
/// cold tier has an incompatible (old) schema and should be skipped.
async fn build_cold_provider(
    ctx: &SessionContext,
    def: &SignalTableDef,
    cold_tier_base_path: &str,
    cold_tier: &Arc<tokio::sync::RwLock<ColdTier>>,
    schema: &arrow::datatypes::SchemaRef,
) -> Result<Option<Arc<dyn TableProvider>>, QueryError> {
    let vortex_format = cold_tier.read().await.create_vortex_format();

    let cold_path = format!("file://{}/{}", cold_tier_base_path, def.cold_path);
    let cold_url = ListingTableUrl::parse(&cold_path).map_err(|e| {
        exec_err(format!(
            "Failed to parse cold tier URL '{}': {}",
            cold_path, e
        ))
    })?;

    let mut options = ListingOptions::new(vortex_format);
    options.file_extension = ".vortex".to_string();

    // Infer the cold-tier schema to detect schema evolution. Spawned in a separate
    // task so a panic inside Vortex's Arrow type conversion (e.g. Map<Utf8,
    // LargeBinary> for _overflow_attrs) is caught by Tokio rather than unwinding
    // through registration.
    let cold_url_infer = cold_url.clone();
    let options_infer = options.clone();
    let state = ctx.state();
    let inferred_schema = match tokio::spawn(async move {
        let infer_cfg = ListingTableConfig::new(cold_url_infer).with_listing_options(options_infer);
        infer_cfg.infer_schema(&state).await
    })
    .await
    {
        Ok(Ok(cfg)) => cfg.file_schema,
        _ => None, // Panic or error → treat as empty cold tier
    };

    // Compatible means every declared field name appears in the cold-tier files.
    let cold_schema_compatible = match &inferred_schema {
        None => true,
        Some(cold_schema) if cold_schema.fields().is_empty() => true,
        Some(cold_schema) => {
            let cold_names: std::collections::HashSet<&str> = cold_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            schema
                .fields()
                .iter()
                .all(|f| cold_names.contains(f.name().as_str()))
        }
    };

    if !cold_schema_compatible {
        eprintln!(
            "⚠️ [register_union_table] Cold tier '{}' has incompatible schema (likely old data). \
             Using hot-tier only until cold-tier data is cleared.",
            def.table_name
        );
        return Ok(None);
    }

    let config = ListingTableConfig::new(cold_url)
        .with_listing_options(options)
        .with_schema(schema.clone());
    let cold_table = ListingTable::try_new(config).map_err(|e| {
        exec_err(format!(
            "Failed to create ListingTable for {}: {}",
            def.table_name, e
        ))
    })?;
    Ok(Some(Arc::new(cold_table)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_storage::test_fixtures::TestStorageBuilder;

    #[tokio::test]
    async fn test_register_span_table_provider() {
        // Create storage with test config
        let (storage, _temp) = TestStorageBuilder::new().build().await;

        // Create DataFusion session context
        let ctx = SessionContext::new();

        // Get the spans table definition
        let spans_def = SIGNAL_TABLE_DEFS
            .iter()
            .find(|def| def.table_name == "spans")
            .expect("Should have spans table definition");

        // Register the spans table
        let hot_tier = storage.hot_tier_arc();
        let cold_tier = storage.cold_tier_arc();
        let base_path = storage
            .config()
            .cold_tier
            .uri
            .strip_prefix("file://")
            .unwrap();

        register_union_table(
            &ctx,
            spans_def,
            hot_tier,
            base_path,
            cold_tier,
            QueryScope::All,
        )
        .await
        .expect("Should register spans table");

        // Verify table is queryable
        let table = ctx
            .table_provider("spans")
            .await
            .expect("Should get spans table provider");

        // Verify schema has expected fields
        let schema = table.schema();
        assert!(schema.field_with_name("trace_id").is_ok());
        assert!(schema.field_with_name("span_id").is_ok());
        assert!(schema.field_with_name("name").is_ok());
    }

    #[tokio::test]
    async fn test_register_all_signal_table_providers() {
        // Create storage with test config
        let (storage, _temp) = TestStorageBuilder::new().build().await;

        // Create DataFusion session context
        let ctx = SessionContext::new();

        let hot_tier = storage.hot_tier_arc();
        let cold_tier = storage.cold_tier_arc();
        let base_path = storage
            .config()
            .cold_tier
            .uri
            .strip_prefix("file://")
            .unwrap();

        // Register all signal tables
        for def in SIGNAL_TABLE_DEFS {
            register_union_table(
                &ctx,
                def,
                hot_tier.clone(),
                base_path,
                cold_tier.clone(),
                QueryScope::All,
            )
            .await
            .expect("Should register table");
        }

        // Verify all tables are registered
        let expected_tables = vec![
            "spans",
            "logs",
            "datapoints",
            "metrics",
            "samples",
            "profiles",
            "histogram_data_points",
            "exp_histogram_data_points",
            "profile_stacks",
            "profile_frames",
            "profile_mappings",
            "resources",
            "scopes",
            "span_links",
            "span_events",
        ];

        for table_name in expected_tables {
            let table = ctx
                .table_provider(table_name)
                .await
                .unwrap_or_else(|_| panic!("Should have {} table registered", table_name));

            // Verify table has a schema
            let schema = table.schema();
            assert!(
                !schema.fields().is_empty(),
                "Table {} should have fields",
                table_name
            );
        }
    }
}
