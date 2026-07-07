//! Cold tier storage main struct
//!
//! # Schema versioning
//!
//! Each signal's Arrow schema is defined in `sequins-arrow-schema`.  When a
//! column is added, renamed, or removed the `COLD_TIER_SCHEMA_VERSION` constant
//! below must be incremented.  The DataFusion registration path in
//! `sequins-datafusion-backend` already infers the on-disk schema and skips the
//! cold tier for any partition whose schema is incompatible with the current
//! declared schema — no explicit migration is needed for additive changes.
//!
//! Destructive changes (column removals or renames) require a `SCHEMA_VERSION`
//! bump and a sweep of the cold-tier storage to either migrate or delete stale
//! partition files.

/// Incremented whenever a breaking change to any signal's Arrow schema is made.
///
/// Consumers can embed this in a sidecar or filename suffix to detect when
/// cold-tier files were written against an older schema.
pub const COLD_TIER_SCHEMA_VERSION: u32 = 1;

use super::series_index::SeriesIndex;
use crate::config::ColdTierConfig;
use crate::error::{Error, Result};
use arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Cold tier (Vortex) storage using object_store
pub struct ColdTier {
    pub config: ColdTierConfig,
    pub store: Arc<dyn ObjectStore>,
    /// Series index for metric time series (protected by RwLock for concurrent access)
    pub series_index: Arc<RwLock<SeriesIndex>>,
    /// This node's stable id, stamped into every cold filename so concurrent
    /// nodes writing to the one shared cold dataset never collide.
    pub(crate) node_id: String,
    /// Monotonic per-instance counter, also stamped into filenames, so two
    /// flushes from this node in the same nanosecond still get distinct names.
    write_seq: std::sync::atomic::AtomicU64,
    // Note: SessionContext removed - we create fresh contexts per query
    // to avoid state pollution. Will revisit when implementing production version.
}

impl ColdTier {
    /// Create new cold tier storage
    ///
    /// # Errors
    ///
    /// Returns an error if the object store URI is invalid
    pub fn new(config: ColdTierConfig) -> Result<Self> {
        // Parse the URI and create the appropriate object store
        let store = Self::create_store(&config)?;

        // Initialize empty series index (will be loaded on first use)
        let series_index = Arc::new(RwLock::new(SeriesIndex::new()));

        Ok(Self {
            config,
            store,
            series_index,
            node_id: "local".to_string(),
            write_seq: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Set this node's id (used to stamp globally-unique cold filenames on a
    /// shared dataset). Defaults to `"local"` for single-node use.
    pub fn with_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.node_id = node_id.into();
        self
    }

    /// A filename-safe token unique to this node and this write: `{node_id}-{seq}`.
    /// Stamped into every cold filename by [`Self::write_record_batch`] so multiple
    /// nodes sharing one cold dataset never overwrite each other's files.
    pub(crate) fn write_token(&self) -> String {
        let seq = self
            .write_seq
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let sanitized: String = self
            .node_id
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        format!("{sanitized}-{seq}")
    }

    /// Load the series index from storage (called at startup)
    pub async fn load_series_index(&self) -> Result<()> {
        let base_path = crate::store_base_path(&self.config.uri);

        let loaded_index = SeriesIndex::load(self.store.clone(), base_path).await?;

        let mut index = self.series_index.write().await;
        *index = loaded_index;

        Ok(())
    }

    /// Create an object store from the cold-tier config.
    ///
    /// Supports:
    /// - Local filesystem: `file:///path` or `/path`
    /// - AWS S3 (and S3-compatibles): `s3://bucket/path`
    /// - Google Cloud Storage: `gs://bucket/path`
    /// - Azure Blob Storage: `az://container/path` or `azure://container/path`
    ///
    /// Connection settings for cloud stores (region, endpoint, HTTP, addressing,
    /// optional static credentials) come from [`ColdTierConfig::object_store`].
    /// Credentials default to the provider's standard chain — instance profile,
    /// IRSA / workload identity — so a properly-configured pod needs no static
    /// credentials in config.
    fn create_store(config: &ColdTierConfig) -> Result<Arc<dyn ObjectStore>> {
        use object_store::local::LocalFileSystem;

        let uri = config.uri.as_str();
        let os = &config.object_store;

        // Local filesystem
        if uri.starts_with("file://") || uri.starts_with('/') {
            let path = uri.strip_prefix("file://").unwrap_or(uri);

            // Create the directory if it doesn't exist
            std::fs::create_dir_all(path).map_err(|e| {
                Error::Storage(format!("Failed to create storage directory: {}", e))
            })?;

            // Use LocalFileSystem without prefix - we'll use full paths in queries
            let store = LocalFileSystem::new();
            return Ok(Arc::new(store));
        }

        // AWS S3
        if uri.starts_with("s3://") {
            use object_store::aws::AmazonS3Builder;

            let url = url::Url::parse(uri)
                .map_err(|e| Error::Config(format!("Invalid S3 URI '{}': {}", uri, e)))?;

            let bucket = url
                .host_str()
                .ok_or_else(|| Error::Config(format!("S3 URI missing bucket name: {}", uri)))?;

            // Base on `from_env` so the default AWS credential chain (instance
            // profile, IRSA / web-identity, the credentials injected by the
            // platform) is picked up with no configuration. Connection settings
            // come from the cold-tier config, not the environment.
            let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
            if let Some(region) = &os.region {
                builder = builder.with_region(region);
            }
            if let Some(endpoint) = &os.endpoint {
                builder = builder.with_endpoint(endpoint);
            }
            if os.allow_http {
                builder = builder.with_allow_http(true);
            }
            if let Some(vhost) = os.virtual_hosted_style {
                builder = builder.with_virtual_hosted_style_request(vhost);
            }
            if let (Some(key), Some(secret)) = (&os.access_key_id, &os.secret_access_key) {
                builder = builder
                    .with_access_key_id(key)
                    .with_secret_access_key(secret);
            }

            let store = builder
                .build()
                .map_err(|e| Error::Config(format!("Failed to create S3 store: {}", e)))?;

            return Ok(Arc::new(store));
        }

        // Google Cloud Storage
        if uri.starts_with("gs://") {
            use object_store::gcp::GoogleCloudStorageBuilder;

            let url = url::Url::parse(uri)
                .map_err(|e| Error::Config(format!("Invalid GCS URI '{}': {}", uri, e)))?;

            let bucket = url
                .host_str()
                .ok_or_else(|| Error::Config(format!("GCS URI missing bucket name: {}", uri)))?;

            // `from_env` picks up GOOGLE_APPLICATION_CREDENTIALS and the default
            // workload-identity credential chain; the bucket comes from the URI.
            let store = GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()
                .map_err(|e| Error::Config(format!("Failed to create GCS store: {}", e)))?;

            return Ok(Arc::new(store));
        }

        // Azure Blob Storage
        if uri.starts_with("az://") || uri.starts_with("azure://") {
            use object_store::azure::MicrosoftAzureBuilder;

            let stripped_uri = uri
                .strip_prefix("az://")
                .or_else(|| uri.strip_prefix("azure://"))
                .unwrap();

            let url = url::Url::parse(&format!("https://{}", stripped_uri))
                .map_err(|e| Error::Config(format!("Invalid Azure URI '{}': {}", uri, e)))?;

            let container = url.host_str().ok_or_else(|| {
                Error::Config(format!("Azure URI missing container name: {}", uri))
            })?;

            // `from_env` picks up AZURE_STORAGE_* and the default credential chain
            // (managed / workload identity); the container comes from the URI.
            let mut builder = MicrosoftAzureBuilder::from_env().with_container_name(container);
            if os.allow_http {
                builder = builder.with_allow_http(true);
            }
            let store = builder
                .build()
                .map_err(|e| Error::Config(format!("Failed to create Azure store: {}", e)))?;

            return Ok(Arc::new(store));
        }

        Err(Error::Config(format!(
            "Unsupported object store URI: {}. Supported: file://, s3://, gs://, az://",
            uri
        )))
    }

    /// Dispatch a hot-tier flush to the appropriate per-signal write path.
    ///
    /// Called by the hot-tier compaction loop when a completed `BatchChain` node
    /// is evicted.  The batch is already in Arrow format so no conversion is needed.
    pub async fn write_signal(
        &self,
        signal: sequins_arrow_schema::SignalType,
        batch: RecordBatch,
    ) -> Result<()> {
        use sequins_arrow_schema::SignalType;
        if batch.num_rows() == 0 {
            return Ok(());
        }
        match signal {
            SignalType::Spans => {
                self.write_spans(batch).await?;
            }
            SignalType::Logs => {
                self.write_logs(batch).await?;
            }
            SignalType::SpanLinks => {
                self.write_signal_batch("spans/links", batch, None).await?;
            }
            SignalType::SpanEvents => {
                self.write_signal_batch("spans/events", batch, None).await?;
            }
            SignalType::MetricsMetadata => {
                self.write_metrics(batch).await?;
            }
            SignalType::Metrics => {
                self.write_signal_batch("metrics/data", batch, None).await?;
            }
            SignalType::Histograms => {
                self.write_signal_batch("metrics/histograms", batch, None)
                    .await?;
            }
            SignalType::ExpHistograms => {
                self.write_signal_batch("metrics/exp_histograms", batch, None)
                    .await?;
            }
            SignalType::ProfilesMetadata => {
                self.write_profiles(batch).await?;
            }
            SignalType::ProfileSamples => {
                self.write_profile_samples(batch).await?;
            }
            SignalType::ProfileStacks => {
                self.write_profile_stacks(batch).await?;
            }
            SignalType::ProfileFrames => {
                self.write_profile_frames(batch).await?;
            }
            SignalType::ProfileMappings => {
                self.write_profile_mappings(batch).await?;
            }
            SignalType::Resources => {
                self.write_resources(batch).await?;
            }
            SignalType::Scopes => {
                self.write_scopes(batch).await?;
            }
        }
        Ok(())
    }

    /// Write a `RecordBatch` to `<base>/<signal_name>/<partition>`, returning the partition path.
    ///
    /// All per-signal write methods that do nothing but partition + write delegate here.
    /// `companion_bytes` is forwarded verbatim to `write_record_batch`.
    pub async fn write_signal_batch(
        &self,
        signal_name: &str,
        batch: RecordBatch,
        companion_bytes: Option<crate::indexed_layout::strategy::CompanionIndexBytes>,
    ) -> Result<String> {
        use super::helpers;
        use sequins_types::models::Timestamp;

        if batch.num_rows() == 0 {
            return Ok(String::new());
        }

        let partition_path = helpers::generate_partition_path(
            signal_name,
            &Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?,
        );

        let base_path = crate::store_base_path(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch.clone(), batch.schema(), &full_path, companion_bytes)
            .await?;

        Ok(partition_path)
    }
}
