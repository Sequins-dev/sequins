//! Table registration helpers
//!
//! This module provides data-driven table registration that replaces
//! 18 repetitive registration methods with a single table definition array.

use super::super::hot_tier::SignalType;
use super::super::union_provider::SignalUnionProvider;
use super::exec_err;
use arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::context::SessionContext;
use sequins_query::error::QueryError;
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
    /// Schema function from sequins_types::arrow_schema
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
        schema_fn: sequins_types::arrow_schema::span_schema,
    },
    SignalTableDef {
        table_name: "logs",
        hot_signal_type: SignalType::Logs,
        cold_path: "logs",
        schema_fn: sequins_types::arrow_schema::log_schema,
    },
    SignalTableDef {
        table_name: "datapoints",
        hot_signal_type: SignalType::Metrics,
        cold_path: "metrics/data",
        schema_fn: sequins_types::arrow_schema::series_data_point_schema,
    },
    SignalTableDef {
        table_name: "metrics",
        hot_signal_type: SignalType::MetricsMetadata,
        cold_path: "metrics/metadata",
        schema_fn: sequins_types::arrow_schema::metric_schema,
    },
    SignalTableDef {
        table_name: "samples",
        hot_signal_type: SignalType::ProfileSamples,
        cold_path: "profiles/samples",
        schema_fn: sequins_types::arrow_schema::profile_samples_schema,
    },
    SignalTableDef {
        table_name: "profiles",
        hot_signal_type: SignalType::ProfilesMetadata,
        cold_path: "profiles/metadata",
        schema_fn: sequins_types::arrow_schema::profile_schema,
    },
    SignalTableDef {
        table_name: "histogram_data_points",
        hot_signal_type: SignalType::Histograms,
        cold_path: "metrics/histograms",
        schema_fn: sequins_types::arrow_schema::histogram_series_data_point_schema,
    },
    SignalTableDef {
        table_name: "exp_histogram_data_points",
        hot_signal_type: SignalType::ExpHistograms,
        cold_path: "metrics/exp_histograms",
        schema_fn: sequins_types::arrow_schema::exp_histogram_data_point_schema,
    },
    SignalTableDef {
        table_name: "profile_stacks",
        hot_signal_type: SignalType::ProfileStacks,
        cold_path: "profiles/stacks",
        schema_fn: sequins_types::arrow_schema::profile_stacks_schema,
    },
    SignalTableDef {
        table_name: "profile_frames",
        hot_signal_type: SignalType::ProfileFrames,
        cold_path: "profiles/frames",
        schema_fn: sequins_types::arrow_schema::profile_frames_schema,
    },
    SignalTableDef {
        table_name: "profile_mappings",
        hot_signal_type: SignalType::ProfileMappings,
        cold_path: "profiles/mappings",
        schema_fn: sequins_types::arrow_schema::profile_mappings_schema,
    },
    SignalTableDef {
        table_name: "resources",
        hot_signal_type: SignalType::Resources,
        cold_path: "resources",
        schema_fn: sequins_types::arrow_schema::resource_schema,
    },
    SignalTableDef {
        table_name: "scopes",
        hot_signal_type: SignalType::Scopes,
        cold_path: "scopes",
        schema_fn: sequins_types::arrow_schema::scope_schema,
    },
    SignalTableDef {
        table_name: "span_links",
        hot_signal_type: SignalType::SpanLinks,
        cold_path: "spans/links",
        schema_fn: sequins_types::arrow_schema::span_links_schema,
    },
    SignalTableDef {
        table_name: "span_events",
        hot_signal_type: SignalType::SpanEvents,
        cold_path: "spans/events",
        schema_fn: sequins_types::arrow_schema::span_events_schema,
    },
];

/// Signal types that should be registered for hot-tier-only contexts
///
/// Used by `make_hot_tier_session_ctx()` for live query enrichment.
/// Only includes frequently-accessed signals that need fast reads.
pub(super) const HOT_ONLY_SIGNAL_COUNT: usize = 5;

pub(super) fn hot_only_defs() -> &'static [SignalTableDef] {
    &SIGNAL_TABLE_DEFS[..HOT_ONLY_SIGNAL_COUNT]
}

/// Register a union table (hot + cold tiers)
pub(super) async fn register_union_table(
    ctx: &SessionContext,
    def: &SignalTableDef,
    hot_tier: Arc<super::super::hot_tier::HotTier>,
    cold_tier_base_path: &str,
    cold_tier: Arc<tokio::sync::RwLock<super::super::cold_tier::ColdTier>>,
) -> Result<(), QueryError> {
    // Use the BatchChain for this signal type directly as a TableProvider.
    // BatchChain already implements TableProvider, so we just hand the Arc in.
    let hot_provider: Arc<dyn TableProvider> = hot_tier.chain_arc(&def.hot_signal_type);

    // Get Vortex format from cold tier
    let vortex_format = cold_tier.read().await.create_vortex_format();

    // Build cold tier listing table
    let cold_path = format!("file://{}/{}", cold_tier_base_path, def.cold_path);
    let cold_url = ListingTableUrl::parse(&cold_path).map_err(|e| {
        exec_err(format!(
            "Failed to parse cold tier URL '{}': {}",
            cold_path, e
        ))
    })?;

    let mut options = ListingOptions::new(vortex_format);
    options.file_extension = ".vortex".to_string();

    let schema = (def.schema_fn)();

    // Try to infer the actual schema from cold-tier files to detect schema evolution.
    // If files were written with an older schema (e.g. renamed columns), the inferred
    // schema will differ from the declared schema and we fall back to hot-tier only.
    let infer_config =
        ListingTableConfig::new(cold_url.clone()).with_listing_options(options.clone());

    let inferred_schema = match infer_config.infer_schema(&ctx.state()).await {
        Ok(cfg) => cfg.file_schema,
        Err(_) => None, // No files or can't read → treat as empty cold tier
    };

    // Check whether the inferred cold-tier schema is compatible with the declared schema.
    // Compatible means all declared field names appear in the cold-tier files.
    let cold_schema_compatible = match &inferred_schema {
        None => true, // No files → empty cold tier, no conflict
        Some(cold_schema) if cold_schema.fields().is_empty() => true, // empty schema
        Some(cold_schema) => {
            let declared_names: Vec<&str> =
                schema.fields().iter().map(|f| f.name().as_str()).collect();
            let cold_names: std::collections::HashSet<&str> = cold_schema
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect();
            declared_names.iter().all(|n| cold_names.contains(n))
        }
    };

    if !cold_schema_compatible {
        // Cold-tier files have a different schema (e.g. old column names from a previous
        // schema version). Skip the cold tier for this table so queries don't fail.
        // The stale files can be cleared manually to restore cold-tier reads.
        eprintln!(
            "⚠️ [register_union_table] Cold tier '{}' has incompatible schema (likely old data). \
             Using hot-tier only until cold-tier data is cleared.",
            def.table_name
        );
        ctx.register_table(def.table_name, hot_provider)
            .map_err(|e| {
                exec_err(format!(
                    "Failed to register hot-only table {}: {}",
                    def.table_name, e
                ))
            })?;
        return Ok(());
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

    let cold_provider: Arc<dyn TableProvider> = Arc::new(cold_table);

    // Create union provider
    let union_provider = SignalUnionProvider::new(hot_provider, cold_provider, schema);

    // Register table
    ctx.register_table(def.table_name, Arc::new(union_provider))
        .map_err(|e| {
            exec_err(format!(
                "Failed to register {} union table: {}",
                def.table_name, e
            ))
        })?;

    Ok(())
}

/// Register a hot-tier-only table
pub(super) async fn register_hot_only_table(
    ctx: &SessionContext,
    def: &SignalTableDef,
    hot_tier: Arc<super::super::hot_tier::HotTier>,
) -> Result<(), QueryError> {
    // Use the BatchChain for this signal type directly as a TableProvider.
    let provider: Arc<dyn TableProvider> = hot_tier.chain_arc(&def.hot_signal_type);

    ctx.register_table(def.table_name, provider).map_err(|e| {
        exec_err(format!(
            "Failed to register hot-only table {}: {}",
            def.table_name, e
        ))
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::TestStorageBuilder;

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

        register_union_table(&ctx, spans_def, hot_tier, base_path, cold_tier)
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
            register_union_table(&ctx, def, hot_tier.clone(), base_path, cold_tier.clone())
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
