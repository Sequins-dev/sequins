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
        let store = Self::create_store(&config.uri)?;

        // Initialize empty series index (will be loaded on first use)
        let series_index = Arc::new(RwLock::new(SeriesIndex::new()));

        Ok(Self {
            config,
            store,
            series_index,
        })
    }

    /// Load the series index from storage (called at startup)
    pub async fn load_series_index(&self) -> Result<()> {
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);

        let loaded_index = SeriesIndex::load(self.store.clone(), base_path).await?;

        let mut index = self.series_index.write().await;
        *index = loaded_index;

        Ok(())
    }

    /// Create an object store from a URI
    ///
    /// Supports:
    /// - Local filesystem: `file:///path` or `/path`
    /// - AWS S3: `s3://bucket/path` (requires AWS credentials in environment)
    /// - Google Cloud Storage: `gs://bucket/path` (requires GCS credentials)
    /// - Azure Blob Storage: `az://container/path` or `azure://container/path` (requires Azure credentials)
    ///
    /// # Environment Variables for Cloud Storage
    ///
    /// **AWS S3:**
    /// - `AWS_ACCESS_KEY_ID` - AWS access key
    /// - `AWS_SECRET_ACCESS_KEY` - AWS secret key
    /// - `AWS_REGION` - AWS region (default: us-east-1)
    /// - `AWS_ENDPOINT` - Custom S3 endpoint (optional, for S3-compatible stores)
    ///
    /// **Google Cloud Storage:**
    /// - `GOOGLE_SERVICE_ACCOUNT` - Path to service account JSON file
    /// - Or default application credentials
    ///
    /// **Azure Blob Storage:**
    /// - `AZURE_STORAGE_ACCOUNT_NAME` - Storage account name
    /// - `AZURE_STORAGE_ACCOUNT_KEY` - Storage account key
    /// - Or default Azure credentials
    fn create_store(uri: &str) -> Result<Arc<dyn ObjectStore>> {
        use object_store::local::LocalFileSystem;

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

            let mut builder = AmazonS3Builder::new().with_bucket_name(bucket);

            // Get credentials from environment
            if let Ok(access_key) = std::env::var("AWS_ACCESS_KEY_ID") {
                builder = builder.with_access_key_id(access_key);
            }
            if let Ok(secret_key) = std::env::var("AWS_SECRET_ACCESS_KEY") {
                builder = builder.with_secret_access_key(secret_key);
            }
            if let Ok(region) = std::env::var("AWS_REGION") {
                builder = builder.with_region(region);
            }
            if let Ok(endpoint) = std::env::var("AWS_ENDPOINT") {
                builder = builder.with_endpoint(endpoint);
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

            let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

            // Get credentials from environment
            if let Ok(service_account) = std::env::var("GOOGLE_SERVICE_ACCOUNT") {
                builder = builder.with_service_account_path(service_account);
            }

            let store = builder
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

            let mut builder = MicrosoftAzureBuilder::new().with_container_name(container);

            // Get credentials from environment
            if let Ok(account_name) = std::env::var("AZURE_STORAGE_ACCOUNT_NAME") {
                builder = builder.with_account(account_name);
            }
            if let Ok(account_key) = std::env::var("AZURE_STORAGE_ACCOUNT_KEY") {
                builder = builder.with_access_key(account_key);
            }
            // Note: SAS token support varies by object_store version
            // For now, rely on account key or default credentials

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

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch.clone(), batch.schema(), &full_path, companion_bytes)
            .await?;

        Ok(partition_path)
    }
}
