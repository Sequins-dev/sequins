use crate::config::ColdTierConfig;
use crate::error::{Error, Result};
use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::Schema;
use datafusion::prelude::*;
use object_store::{path::Path as ObjectPath, ObjectStore};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use sequins_core::arrow_schema;
use sequins_core::models::{
    LogEntry, LogQuery, Metric, MetricQuery, MetricType, Profile, ProfileQuery, Span, SpanKind,
    SpanStatus, Timestamp, TraceQuery,
};
use std::sync::Arc;

/// Cold tier (Parquet) storage using object_store
pub struct ColdTier {
    config: ColdTierConfig,
    store: Arc<dyn ObjectStore>,
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

        Ok(Self { config, store })
    }

    /// Create an object store from a URI
    fn create_store(uri: &str) -> Result<Arc<dyn ObjectStore>> {
        use object_store::local::LocalFileSystem;

        // For now, only support local filesystem
        // TODO: Add S3, GCS, Azure support
        if uri.starts_with("file://") || uri.starts_with('/') {
            let path = uri.strip_prefix("file://").unwrap_or(uri);

            // Create the directory if it doesn't exist
            std::fs::create_dir_all(path).map_err(|e| {
                Error::Storage(format!("Failed to create storage directory: {}", e))
            })?;

            // Use LocalFileSystem without prefix - we'll use full paths in queries
            let store = LocalFileSystem::new();
            Ok(Arc::new(store))
        } else {
            Err(Error::Config(format!(
                "Unsupported object store URI: {}",
                uri
            )))
        }
    }

    /// Write spans to Parquet
    ///
    /// # Errors
    ///
    /// Returns an error if writing to Parquet or object store fails
    pub async fn write_spans(&self, spans: Vec<Span>) -> Result<String> {
        if spans.is_empty() {
            return Ok(String::new());
        }

        let schema = arrow_schema::span_schema();
        let batch = Self::spans_to_record_batch(spans, schema.clone())?;

        let partition_path = Self::generate_partition_path(
            "spans",
            &Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?,
        );

        // Prepend base path since object store has no prefix
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch, schema, &full_path).await?;

        Ok(partition_path)
    }

    /// Write logs to Parquet
    ///
    /// # Errors
    ///
    /// Returns an error if writing to Parquet or object store fails
    pub async fn write_logs(&self, logs: Vec<LogEntry>) -> Result<String> {
        if logs.is_empty() {
            return Ok(String::new());
        }

        let schema = arrow_schema::log_schema();
        let batch = Self::logs_to_record_batch(logs, schema.clone())?;

        let partition_path = Self::generate_partition_path(
            "logs",
            &Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?,
        );

        // Prepend base path since object store has no prefix
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch, schema, &full_path).await?;

        Ok(partition_path)
    }

    /// Write metrics to Parquet
    ///
    /// # Errors
    ///
    /// Returns an error if writing to Parquet or object store fails
    pub async fn write_metrics(&self, metrics: Vec<Metric>) -> Result<String> {
        if metrics.is_empty() {
            return Ok(String::new());
        }

        let schema = arrow_schema::metric_schema();
        let batch = Self::metrics_to_record_batch(metrics, schema.clone())?;

        let partition_path = Self::generate_partition_path(
            "metrics",
            &Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?,
        );

        // Prepend base path since object store has no prefix
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch, schema, &full_path).await?;

        Ok(partition_path)
    }

    /// Write profiles to Parquet
    ///
    /// # Errors
    ///
    /// Returns an error if writing to Parquet or object store fails
    pub async fn write_profiles(&self, profiles: Vec<Profile>) -> Result<String> {
        if profiles.is_empty() {
            return Ok(String::new());
        }

        let schema = arrow_schema::profile_schema();
        let batch = Self::profiles_to_record_batch(profiles, schema.clone())?;

        let partition_path = Self::generate_partition_path(
            "profiles",
            &Timestamp::now()
                .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?,
        );

        // Prepend base path since object store has no prefix
        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);
        let full_path = format!("{}/{}", base_path, partition_path);

        self.write_record_batch(batch, schema, &full_path).await?;

        Ok(partition_path)
    }

    /// Generate a partition path based on telemetry type and timestamp
    fn generate_partition_path(telemetry_type: &str, timestamp: &Timestamp) -> String {
        // Format: {type}/year=YYYY/month=MM/day=DD/{timestamp}.parquet

        // Parse datetime to extract year, month, day
        // For simplicity, use seconds for partitioning
        let secs = timestamp.as_secs();
        let days = secs / 86400;
        let year = 1970 + days / 365;
        let month = ((days % 365) / 30) + 1;
        let day = ((days % 365) % 30) + 1;

        format!(
            "{}/year={}/month={:02}/day={:02}/{}.parquet",
            telemetry_type,
            year,
            month,
            day,
            timestamp.as_nanos()
        )
    }

    /// Delete old Parquet files based on retention period
    ///
    /// # Errors
    ///
    /// Returns an error if directory listing or file deletion fails
    pub async fn cleanup_old_files(
        &self,
        telemetry_type: &str,
        retention_period: sequins_core::Duration,
    ) -> Result<usize> {
        use object_store::path::Path as ObjectPath;

        let base_path = self
            .config
            .uri
            .strip_prefix("file://")
            .unwrap_or(&self.config.uri);

        // Calculate cutoff timestamp
        let now = Timestamp::now()
            .map_err(|e| Error::Storage(format!("Failed to get current time: {}", e)))?;
        let cutoff = now - retention_period;
        let cutoff_nanos = cutoff.as_nanos();

        // List all files in the telemetry type directory
        let type_path = format!("{}/{}", base_path, telemetry_type);

        // Check if directory exists
        if !std::path::Path::new(&type_path).exists() {
            return Ok(0);
        }

        let mut deleted_count = 0;

        // Walk through year/month/day directories and find old files
        if let Ok(entries) = std::fs::read_dir(&type_path) {
            for year_entry in entries.flatten() {
                if let Ok(year_meta) = year_entry.metadata() {
                    if !year_meta.is_dir() {
                        continue;
                    }

                    // Read month directories
                    if let Ok(month_entries) = std::fs::read_dir(year_entry.path()) {
                        for month_entry in month_entries.flatten() {
                            if let Ok(month_meta) = month_entry.metadata() {
                                if !month_meta.is_dir() {
                                    continue;
                                }

                                // Read day directories
                                if let Ok(day_entries) = std::fs::read_dir(month_entry.path()) {
                                    for day_entry in day_entries.flatten() {
                                        if let Ok(day_meta) = day_entry.metadata() {
                                            if !day_meta.is_dir() {
                                                continue;
                                            }

                                            // Read parquet files in day directory
                                            if let Ok(file_entries) =
                                                std::fs::read_dir(day_entry.path())
                                            {
                                                for file_entry in file_entries.flatten() {
                                                    if let Some(filename) =
                                                        file_entry.file_name().to_str()
                                                    {
                                                        if filename.ends_with(".parquet") {
                                                            // Extract timestamp from filename
                                                            // Format: {timestamp}.parquet
                                                            if let Some(timestamp_str) =
                                                                filename.strip_suffix(".parquet")
                                                            {
                                                                if let Ok(file_timestamp) =
                                                                    timestamp_str.parse::<i64>()
                                                                {
                                                                    // Delete if older than retention period
                                                                    if file_timestamp
                                                                        < cutoff_nanos
                                                                    {
                                                                        // Delete the file using object store
                                                                        let file_path = file_entry.path();
                                                                        let relative_path = file_path
                                                                            .strip_prefix(base_path)
                                                                            .map_err(|e| {
                                                                                Error::Storage(
                                                                                    format!(
                                                                                "Failed to strip prefix: {}",
                                                                                e
                                                                            ),
                                                                                )
                                                                            })?
                                                                            .to_str()
                                                                            .ok_or_else(|| {
                                                                                Error::Storage(
                                                                                    "Invalid path"
                                                                                        .to_string(),
                                                                                )
                                                                            })?;

                                                                        let object_path =
                                                                            ObjectPath::from(
                                                                                relative_path,
                                                                            );
                                                                        self.store
                                                                            .delete(&object_path)
                                                                            .await
                                                                            .map_err(|e| {
                                                                                Error::Storage(
                                                                                    format!(
                                                                                    "Failed to delete file: {}",
                                                                                    e
                                                                                ),
                                                                                )
                                                                            })?;

                                                                        deleted_count += 1;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(deleted_count)
    }

    /// Write a RecordBatch to Parquet and upload to object store
    async fn write_record_batch(
        &self,
        batch: RecordBatch,
        schema: Arc<Schema>,
        path: &str,
    ) -> Result<()> {
        // Create a buffer to write Parquet data
        let mut buffer = Vec::new();

        // Configure writer properties with Bloom filters
        // Bloom filters improve query performance for equality predicates
        // on high-cardinality columns (trace_id, span_id, service_name, etc.)
        let props = WriterProperties::builder()
            .set_compression(self.config.compression.into())
            .set_max_row_group_size(self.config.row_group_size)
            // Enable Bloom filters for frequently filtered columns
            .set_bloom_filter_enabled(true)
            .build();

        // Write to Parquet
        {
            let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;
            writer.write(&batch)?;
            writer.close()?;
        }

        // Upload to object store
        let object_path = ObjectPath::from(path);
        self.store
            .put(&object_path, buffer.into())
            .await
            .map_err(Error::ObjectStore)?;

        Ok(())
    }

    /// Convert spans to Arrow RecordBatch
    pub fn spans_to_record_batch(spans: Vec<Span>, schema: Arc<Schema>) -> Result<RecordBatch> {
        use arrow::array::{
            FixedSizeBinaryArray, Int64Array, StringArray, TimestampNanosecondArray,
        };

        let num_rows = spans.len();

        // Handle empty case - return empty RecordBatch
        if num_rows == 0 {
            use arrow::array::{new_empty_array, ArrayRef};
            let empty_columns: Vec<ArrayRef> = schema
                .fields()
                .iter()
                .map(|field| new_empty_array(field.data_type()))
                .collect();

            return RecordBatch::try_new(schema, empty_columns).map_err(Error::Arrow);
        }

        // Prepare buffers for each field
        let mut trace_ids = Vec::with_capacity(num_rows * 16);
        let mut span_ids = Vec::with_capacity(num_rows * 8);
        let mut parent_span_ids = Vec::with_capacity(num_rows);
        let mut service_names = Vec::with_capacity(num_rows);
        let mut operation_names = Vec::with_capacity(num_rows);
        let mut span_kinds = Vec::with_capacity(num_rows);
        let mut statuses = Vec::with_capacity(num_rows);
        let mut start_times = Vec::with_capacity(num_rows);
        let mut end_times = Vec::with_capacity(num_rows);
        let mut durations = Vec::with_capacity(num_rows);
        let mut attributes = Vec::with_capacity(num_rows);
        let mut events = Vec::with_capacity(num_rows);

        for span in spans {
            // IDs
            trace_ids.extend_from_slice(&span.trace_id.to_bytes());
            span_ids.extend_from_slice(&span.span_id.to_bytes());

            // Parent span ID (nullable)
            if let Some(parent) = span.parent_span_id {
                parent_span_ids.push(Some(parent.to_bytes().to_vec()));
            } else {
                parent_span_ids.push(None);
            }

            // Metadata
            service_names.push(span.service_name);
            operation_names.push(span.operation_name);

            // Span kind
            let kind_str = match span.span_kind {
                SpanKind::Unspecified => "unspecified",
                SpanKind::Internal => "internal",
                SpanKind::Server => "server",
                SpanKind::Client => "client",
                SpanKind::Producer => "producer",
                SpanKind::Consumer => "consumer",
            };
            span_kinds.push(kind_str);

            // Status
            let status_str = match span.status {
                SpanStatus::Unset => "unset",
                SpanStatus::Ok => "ok",
                SpanStatus::Error => "error",
            };
            statuses.push(status_str);

            // Timing
            start_times.push(span.start_time.as_nanos());
            end_times.push(span.end_time.as_nanos());
            durations.push(span.duration.as_nanos());

            // Attributes as JSON
            let attrs_json = serde_json::to_string(&span.attributes)
                .map_err(|e| Error::Storage(format!("Failed to serialize attributes: {}", e)))?;
            attributes.push(attrs_json);

            // Events as JSON
            let events_json = serde_json::to_string(&span.events)
                .map_err(|e| Error::Storage(format!("Failed to serialize events: {}", e)))?;
            events.push(events_json);
        }

        // Build Arrow arrays
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                trace_ids.chunks_exact(16),
            )?) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                span_ids.chunks_exact(8),
            )?) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                parent_span_ids.into_iter(),
                8,
            )?) as ArrayRef,
            Arc::new(StringArray::from(service_names)) as ArrayRef,
            Arc::new(StringArray::from(operation_names)) as ArrayRef,
            Arc::new(StringArray::from(span_kinds)) as ArrayRef,
            Arc::new(StringArray::from(statuses)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(start_times)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(end_times)) as ArrayRef,
            Arc::new(Int64Array::from(durations)) as ArrayRef,
            Arc::new(StringArray::from(attributes)) as ArrayRef,
            Arc::new(StringArray::from(events)) as ArrayRef,
        ];

        RecordBatch::try_new(schema, arrays).map_err(Error::Arrow)
    }

    /// Convert logs to Arrow RecordBatch
    fn logs_to_record_batch(logs: Vec<LogEntry>, schema: Arc<Schema>) -> Result<RecordBatch> {
        use arrow::array::{
            FixedSizeBinaryArray, StringArray, TimestampNanosecondArray, UInt8Array,
        };

        let num_rows = logs.len();

        // Prepare buffers for each field
        let mut log_ids = Vec::with_capacity(num_rows * 16);
        let mut timestamps = Vec::with_capacity(num_rows);
        let mut observed_timestamps = Vec::with_capacity(num_rows);
        let mut service_names = Vec::with_capacity(num_rows);
        let mut severities = Vec::with_capacity(num_rows);
        let mut severity_numbers = Vec::with_capacity(num_rows);
        let mut bodies = Vec::with_capacity(num_rows);
        let mut trace_ids = Vec::with_capacity(num_rows);
        let mut span_ids = Vec::with_capacity(num_rows);
        let mut attributes = Vec::with_capacity(num_rows);
        let mut resources = Vec::with_capacity(num_rows);

        for log in logs {
            // ID (UUID as 16 bytes)
            log_ids.extend_from_slice(log.id.as_uuid().as_bytes());

            // Timing
            timestamps.push(log.timestamp.as_nanos());
            observed_timestamps.push(log.observed_timestamp.as_nanos());

            // Metadata
            service_names.push(log.service_name);
            severities.push(log.severity.as_str());
            severity_numbers.push(log.severity.to_number());
            bodies.push(log.body);

            // Trace context (nullable)
            if let Some(trace_id) = log.trace_id {
                trace_ids.push(Some(trace_id.to_bytes().to_vec()));
            } else {
                trace_ids.push(None);
            }

            if let Some(span_id) = log.span_id {
                span_ids.push(Some(span_id.to_bytes().to_vec()));
            } else {
                span_ids.push(None);
            }

            // Attributes as JSON
            let attrs_json = serde_json::to_string(&log.attributes)
                .map_err(|e| Error::Storage(format!("Failed to serialize attributes: {}", e)))?;
            attributes.push(attrs_json);

            // Resource as JSON
            let resource_json = serde_json::to_string(&log.resource)
                .map_err(|e| Error::Storage(format!("Failed to serialize resource: {}", e)))?;
            resources.push(resource_json);
        }

        // Build Arrow arrays
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                log_ids.chunks_exact(16),
            )?) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(observed_timestamps)) as ArrayRef,
            Arc::new(StringArray::from(service_names)) as ArrayRef,
            Arc::new(StringArray::from(severities)) as ArrayRef,
            Arc::new(UInt8Array::from(severity_numbers)) as ArrayRef,
            Arc::new(StringArray::from(bodies)) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                trace_ids.into_iter(),
                16,
            )?) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                span_ids.into_iter(),
                8,
            )?) as ArrayRef,
            Arc::new(StringArray::from(attributes)) as ArrayRef,
            Arc::new(StringArray::from(resources)) as ArrayRef,
        ];

        RecordBatch::try_new(schema, arrays).map_err(Error::Arrow)
    }

    /// Convert metrics to Arrow RecordBatch
    fn metrics_to_record_batch(metrics: Vec<Metric>, schema: Arc<Schema>) -> Result<RecordBatch> {
        use arrow::array::{FixedSizeBinaryArray, StringArray};

        let num_rows = metrics.len();

        // Prepare buffers for each field
        let mut metric_ids = Vec::with_capacity(num_rows * 16);
        let mut names = Vec::with_capacity(num_rows);
        let mut descriptions = Vec::with_capacity(num_rows);
        let mut units = Vec::with_capacity(num_rows);
        let mut metric_types = Vec::with_capacity(num_rows);
        let mut service_names = Vec::with_capacity(num_rows);

        for metric in metrics {
            // ID (UUID as 16 bytes)
            metric_ids.extend_from_slice(metric.id.as_uuid().as_bytes());

            // Metadata
            names.push(metric.name);
            descriptions.push(metric.description);
            units.push(metric.unit);

            // Metric type
            let type_str = match metric.metric_type {
                MetricType::Gauge => "gauge",
                MetricType::Counter => "counter",
                MetricType::Histogram => "histogram",
                MetricType::Summary => "summary",
            };
            metric_types.push(type_str);

            service_names.push(metric.service_name);
        }

        // Build Arrow arrays
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                metric_ids.chunks_exact(16),
            )?) as ArrayRef,
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(StringArray::from(descriptions)) as ArrayRef,
            Arc::new(StringArray::from(units)) as ArrayRef,
            Arc::new(StringArray::from(metric_types)) as ArrayRef,
            Arc::new(StringArray::from(service_names)) as ArrayRef,
        ];

        RecordBatch::try_new(schema, arrays).map_err(Error::Arrow)
    }

    /// Convert profiles to Arrow RecordBatch
    fn profiles_to_record_batch(
        profiles: Vec<Profile>,
        schema: Arc<Schema>,
    ) -> Result<RecordBatch> {
        use arrow::array::{
            BinaryArray, FixedSizeBinaryArray, StringArray, TimestampNanosecondArray,
        };

        let num_rows = profiles.len();

        // Prepare buffers for each field
        let mut profile_ids = Vec::with_capacity(num_rows * 16);
        let mut timestamps = Vec::with_capacity(num_rows);
        let mut service_names = Vec::with_capacity(num_rows);
        let mut profile_types = Vec::with_capacity(num_rows);
        let mut sample_types = Vec::with_capacity(num_rows);
        let mut sample_units = Vec::with_capacity(num_rows);
        let mut trace_ids = Vec::with_capacity(num_rows);
        let mut data: Vec<Vec<u8>> = Vec::with_capacity(num_rows);

        for profile in profiles {
            // ID (UUID as 16 bytes)
            profile_ids.extend_from_slice(profile.id.as_uuid().as_bytes());

            // Timing
            timestamps.push(profile.timestamp.as_nanos());

            // Metadata
            service_names.push(profile.service_name);
            profile_types.push(profile.profile_type.as_str());
            sample_types.push(profile.sample_type);
            sample_units.push(profile.sample_unit);

            // Trace context (nullable)
            if let Some(trace_id) = profile.trace_id {
                trace_ids.push(Some(trace_id.to_bytes().to_vec()));
            } else {
                trace_ids.push(None);
            }

            // Profile data (binary)
            data.push(profile.data);
        }

        // Convert data to references for BinaryArray
        let data_refs: Vec<&[u8]> = data.iter().map(|v| v.as_slice()).collect();

        // Build Arrow arrays
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(FixedSizeBinaryArray::try_from_iter(
                profile_ids.chunks_exact(16),
            )?) as ArrayRef,
            Arc::new(TimestampNanosecondArray::from(timestamps)) as ArrayRef,
            Arc::new(StringArray::from(service_names)) as ArrayRef,
            Arc::new(StringArray::from(profile_types)) as ArrayRef,
            Arc::new(StringArray::from(sample_types)) as ArrayRef,
            Arc::new(StringArray::from(sample_units)) as ArrayRef,
            Arc::new(FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                trace_ids.into_iter(),
                16,
            )?) as ArrayRef,
            Arc::new(BinaryArray::from_vec(data_refs)) as ArrayRef,
        ];

        RecordBatch::try_new(schema, arrays).map_err(Error::Arrow)
    }

    /// Create a fresh SessionContext with the object store registered
    fn create_session_context(&self) -> Result<datafusion::execution::context::SessionContext> {
        use datafusion::execution::context::SessionContext;
        use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
        use std::sync::Arc;

        let runtime_config = RuntimeConfig::new();
        let runtime_env = RuntimeEnv::new(runtime_config)
            .map_err(|e| Error::Storage(format!("Failed to create runtime env: {}", e)))?;

        let ctx = SessionContext::new_with_config_rt(
            datafusion::prelude::SessionConfig::default(),
            Arc::new(runtime_env),
        );

        // Register the object store
        let url = url::Url::parse(&self.config.uri)
            .map_err(|e| Error::Storage(format!("Invalid URI: {}", e)))?;

        ctx.register_object_store(&url, self.store.clone());

        Ok(ctx)
    }

    /// Query traces from Parquet files
    ///
    /// # Errors
    ///
    /// Returns an error if DataFusion query fails
    pub async fn query_traces(&self, query: &TraceQuery) -> Result<Vec<Span>> {
        // Create fresh SessionContext
        let ctx = self.create_session_context()?;

        // Register parquet directory as a table using ListingTable
        // This properly handles the object store paths
        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        // Use directory path - ListingTable will recursively find parquet files
        let table_path = format!("{}/spans/", &self.config.uri);

        let listing_url = match ListingTableUrl::parse(&table_path) {
            Ok(url) => url,
            Err(_) => return Ok(Vec::new()),
        };

        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![]); // Enable recursive directory scanning

        let config = match ListingTableConfig::new(listing_url)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
        {
            Ok(cfg) => cfg,
            Err(_) => return Ok(Vec::new()),
        };

        let table = Arc::new(
            ListingTable::try_new(config)
                .map_err(|e| Error::Storage(format!("Failed to create listing table: {}", e)))?,
        );
        ctx.register_table("spans", table)
            .map_err(|e| Error::Storage(format!("Failed to register table: {}", e)))?;

        // Build and execute SQL query
        let sql = self.build_trace_query_sql(query)?;
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| Error::Storage(format!("DataFusion SQL query failed: {}", e)))?;

        // Collect results as RecordBatches
        let batches = df
            .collect()
            .await
            .map_err(|e| Error::Storage(format!("Failed to collect query results: {}", e)))?;

        // Convert to Spans
        Self::record_batches_to_spans(batches)
    }

    /// Build SQL query for trace queries
    fn build_trace_query_sql(&self, query: &TraceQuery) -> Result<String> {
        let mut conditions = Vec::new();

        // Time range (required)
        // Cast timestamp to BIGINT for comparison with nanosecond integers
        conditions.push(format!(
            "CAST(start_time_ns AS BIGINT) >= {}",
            query.start_time.as_nanos()
        ));
        conditions.push(format!(
            "CAST(start_time_ns AS BIGINT) <= {}",
            query.end_time.as_nanos()
        ));

        // Service filter (optional)
        if let Some(service) = &query.service {
            conditions.push(format!("service_name = '{}'", service.replace('\'', "''")));
        }

        // Duration filters (optional)
        if let Some(min_duration) = query.min_duration {
            conditions.push(format!("duration_ns >= {}", min_duration));
        }
        if let Some(max_duration) = query.max_duration {
            conditions.push(format!("duration_ns <= {}", max_duration));
        }

        // Error filter (optional)
        if let Some(has_error) = query.has_error {
            if has_error {
                conditions.push("status = 'error'".to_string());
            } else {
                conditions.push("status != 'error'".to_string());
            }
        }

        let where_clause = conditions.join(" AND ");
        let limit_clause = query
            .limit
            .map(|l| format!(" LIMIT {}", l))
            .unwrap_or_default();

        // Build full SQL query
        let sql = format!(
            "SELECT * FROM spans WHERE {} ORDER BY start_time_ns DESC{}",
            where_clause, limit_clause
        );

        Ok(sql)
    }

    /// Query logs from Parquet files
    ///
    /// # Errors
    ///
    /// Returns an error if DataFusion query fails
    pub async fn query_logs(&self, query: &LogQuery) -> Result<Vec<LogEntry>> {
        let ctx = self.create_session_context()?;

        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        // Use directory path - ListingTable will recursively find parquet files
        let table_path = format!("{}/logs/", &self.config.uri);

        let listing_url = match ListingTableUrl::parse(&table_path) {
            Ok(url) => url,
            Err(_) => return Ok(Vec::new()),
        };

        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![]); // Enable recursive directory scanning

        let config = match ListingTableConfig::new(listing_url)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
        {
            Ok(cfg) => cfg,
            Err(_) => return Ok(Vec::new()),
        };

        let table = Arc::new(
            ListingTable::try_new(config)
                .map_err(|e| Error::Storage(format!("Failed to create listing table: {}", e)))?,
        );
        ctx.register_table("logs", table)
            .map_err(|e| Error::Storage(format!("Failed to register table: {}", e)))?;

        // Build and execute SQL query
        let sql = self.build_log_query_sql(query)?;
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| Error::Storage(format!("DataFusion SQL query failed: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| Error::Storage(format!("Failed to collect query results: {}", e)))?;

        // Convert to LogEntries
        Self::record_batches_to_logs(batches)
    }

    /// Build SQL query for log queries
    fn build_log_query_sql(&self, query: &LogQuery) -> Result<String> {
        let mut conditions = Vec::new();

        // Time range (required)
        // Cast timestamp to BIGINT for comparison with nanosecond integers
        conditions.push(format!(
            "CAST(timestamp_ns AS BIGINT) >= {}",
            query.start_time.as_nanos()
        ));
        conditions.push(format!(
            "CAST(timestamp_ns AS BIGINT) <= {}",
            query.end_time.as_nanos()
        ));

        // Service filter (optional)
        if let Some(service) = &query.service {
            conditions.push(format!("service_name = '{}'", service.replace('\'', "''")));
        }

        // Severity filter (optional)
        if let Some(severity) = &query.severity {
            conditions.push(format!("severity = '{}'", severity.replace('\'', "''")));
        }

        // Full-text search (optional)
        if let Some(search) = &query.search {
            // Use LIKE for simple substring matching
            let escaped = search.replace('\'', "''").replace('%', "\\%");
            conditions.push(format!("body LIKE '%{}%'", escaped));
        }

        // Trace ID filter (optional)
        if let Some(trace_id) = &query.trace_id {
            // Convert TraceId bytes to hex string for SQL comparison
            let hex = trace_id
                .to_bytes()
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();
            conditions.push(format!("trace_id = '{}'", hex));
        }

        let where_clause = conditions.join(" AND ");
        let limit_clause = query
            .limit
            .map(|l| format!(" LIMIT {}", l))
            .unwrap_or_default();

        // Build full SQL query
        let sql = format!(
            "SELECT * FROM logs WHERE {} ORDER BY timestamp_ns DESC{}",
            where_clause, limit_clause
        );

        Ok(sql)
    }

    /// Query metrics from Parquet files
    ///
    /// # Errors
    ///
    /// Returns an error if DataFusion query fails
    pub async fn query_metrics(&self, query: &MetricQuery) -> Result<Vec<Metric>> {
        let ctx = self.create_session_context()?;

        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        // Use directory path - ListingTable will recursively find parquet files
        let table_path = format!("{}/metrics/", &self.config.uri);

        let listing_url = match ListingTableUrl::parse(&table_path) {
            Ok(url) => url,
            Err(_) => return Ok(Vec::new()),
        };

        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![]); // Enable recursive directory scanning

        let config = match ListingTableConfig::new(listing_url)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
        {
            Ok(cfg) => cfg,
            Err(_) => return Ok(Vec::new()),
        };

        let table = Arc::new(
            ListingTable::try_new(config)
                .map_err(|e| Error::Storage(format!("Failed to create listing table: {}", e)))?,
        );
        ctx.register_table("metrics", table)
            .map_err(|e| Error::Storage(format!("Failed to register table: {}", e)))?;

        // Build and execute SQL query
        let sql = self.build_metric_query_sql(query)?;
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| Error::Storage(format!("DataFusion SQL query failed: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| Error::Storage(format!("Failed to collect query results: {}", e)))?;

        // Convert to Metrics
        Self::record_batches_to_metrics(batches)
    }

    /// Build SQL query for metric queries
    fn build_metric_query_sql(&self, query: &MetricQuery) -> Result<String> {
        let mut conditions = Vec::new();

        // NOTE: Metrics are metadata without timestamps.
        // start_time/end_time in MetricQuery are for MetricDataPoints (not implemented yet).
        // For now, we don't filter by time for Metric metadata queries.

        // Metric name filter (optional)
        if let Some(name) = &query.name {
            conditions.push(format!("name = '{}'", name.replace('\'', "''")));
        }

        // Service filter (optional)
        if let Some(service) = &query.service {
            conditions.push(format!("service_name = '{}'", service.replace('\'', "''")));
        }

        let limit_clause = query
            .limit
            .map(|l| format!(" LIMIT {}", l))
            .unwrap_or_default();

        // Build full SQL query
        let sql = if conditions.is_empty() {
            format!("SELECT * FROM metrics{}", limit_clause)
        } else {
            let where_clause = conditions.join(" AND ");
            format!(
                "SELECT * FROM metrics WHERE {}{}",
                where_clause, limit_clause
            )
        };

        Ok(sql)
    }

    /// Query profiles from Parquet files
    ///
    /// # Errors
    ///
    /// Returns an error if DataFusion query fails
    pub async fn query_profiles(&self, query: &ProfileQuery) -> Result<Vec<Profile>> {
        let ctx = self.create_session_context()?;

        use datafusion::datasource::file_format::parquet::ParquetFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };

        // Use directory path - ListingTable will recursively find parquet files
        let table_path = format!("{}/profiles/", &self.config.uri);

        let listing_url = match ListingTableUrl::parse(&table_path) {
            Ok(url) => url,
            Err(_) => return Ok(Vec::new()),
        };

        let file_format = Arc::new(ParquetFormat::default());
        let listing_options = ListingOptions::new(file_format)
            .with_file_extension(".parquet")
            .with_table_partition_cols(vec![]); // Enable recursive directory scanning

        let config = match ListingTableConfig::new(listing_url)
            .with_listing_options(listing_options)
            .infer_schema(&ctx.state())
            .await
        {
            Ok(cfg) => cfg,
            Err(_) => return Ok(Vec::new()),
        };

        let table = Arc::new(
            ListingTable::try_new(config)
                .map_err(|e| Error::Storage(format!("Failed to create listing table: {}", e)))?,
        );
        ctx.register_table("profiles", table)
            .map_err(|e| Error::Storage(format!("Failed to register table: {}", e)))?;

        // Build and execute SQL query
        let sql = self.build_profile_query_sql(query)?;
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| Error::Storage(format!("DataFusion SQL query failed: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| Error::Storage(format!("Failed to collect query results: {}", e)))?;

        // Convert to Profiles
        Self::record_batches_to_profiles(batches)
    }

    /// Build SQL query for profile queries
    fn build_profile_query_sql(&self, query: &ProfileQuery) -> Result<String> {
        let mut conditions = Vec::new();

        // Time range (required)
        // Cast timestamp to BIGINT for comparison with nanosecond integers
        conditions.push(format!(
            "CAST(timestamp_ns AS BIGINT) >= {}",
            query.start_time.as_nanos()
        ));
        conditions.push(format!(
            "CAST(timestamp_ns AS BIGINT) <= {}",
            query.end_time.as_nanos()
        ));

        // Service filter (optional)
        if let Some(service) = &query.service {
            conditions.push(format!("service_name = '{}'", service.replace('\'', "''")));
        }

        // Profile type filter (optional)
        if let Some(profile_type) = &query.profile_type {
            conditions.push(format!(
                "profile_type = '{}'",
                profile_type.replace('\'', "''")
            ));
        }

        // Trace ID filter (optional)
        if let Some(trace_id) = &query.trace_id {
            // Convert TraceId bytes to hex string for SQL comparison
            let hex = trace_id
                .to_bytes()
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>();
            conditions.push(format!("trace_id = '{}'", hex));
        }

        let where_clause = conditions.join(" AND ");
        let limit_clause = query
            .limit
            .map(|l| format!(" LIMIT {}", l))
            .unwrap_or_default();

        // Build full SQL query
        let sql = format!(
            "SELECT * FROM profiles WHERE {} ORDER BY timestamp_ns DESC{}",
            where_clause, limit_clause
        );

        Ok(sql)
    }

    /// Convert Arrow RecordBatch to Span structs
    ///
    /// This is a helper for the MemTable prototype and future RecordBatch conversions
    fn record_batches_to_spans(batches: Vec<RecordBatch>) -> Result<Vec<Span>> {
        use arrow::array::{
            Array, FixedSizeBinaryArray, Int64Array, StringArray, TimestampNanosecondArray,
        };
        use sequins_core::models::{Duration, SpanId, TraceId};

        let mut spans = Vec::new();

        for batch in batches {
            let num_rows = batch.num_rows();

            // Extract columns
            let trace_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| Error::Storage("Invalid trace_id column".to_string()))?;

            let span_ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| Error::Storage("Invalid span_id column".to_string()))?;

            let parent_span_ids = batch
                .column(2)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| Error::Storage("Invalid parent_span_id column".to_string()))?;

            let service_names = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid service_name column".to_string()))?;

            let operation_names = batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid operation_name column".to_string()))?;

            let span_kinds = batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid span_kind column".to_string()))?;

            let statuses = batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid status column".to_string()))?;

            let start_times = batch
                .column(7)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Storage("Invalid start_time_ns column".to_string()))?;

            let end_times = batch
                .column(8)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Storage("Invalid end_time_ns column".to_string()))?;

            let durations = batch
                .column(9)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| Error::Storage("Invalid duration_ns column".to_string()))?;

            let attributes_json = batch
                .column(10)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid attributes column".to_string()))?;

            let events_json = batch
                .column(11)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid events column".to_string()))?;

            // Convert each row to a Span
            for row_idx in 0..num_rows {
                let trace_id = TraceId::from_bytes(
                    trace_ids
                        .value(row_idx)
                        .try_into()
                        .map_err(|_| Error::Storage("Invalid trace_id bytes".to_string()))?,
                );

                let span_id = SpanId::from_bytes(
                    span_ids
                        .value(row_idx)
                        .try_into()
                        .map_err(|_| Error::Storage("Invalid span_id bytes".to_string()))?,
                );

                let parent_span_id = if parent_span_ids.is_null(row_idx) {
                    None
                } else {
                    Some(SpanId::from_bytes(
                        parent_span_ids.value(row_idx).try_into().map_err(|_| {
                            Error::Storage("Invalid parent_span_id bytes".to_string())
                        })?,
                    ))
                };

                let service_name = service_names.value(row_idx).to_string();
                let operation_name = operation_names.value(row_idx).to_string();

                let span_kind = match span_kinds.value(row_idx) {
                    "internal" => SpanKind::Internal,
                    "server" => SpanKind::Server,
                    "client" => SpanKind::Client,
                    "producer" => SpanKind::Producer,
                    "consumer" => SpanKind::Consumer,
                    _ => SpanKind::Unspecified,
                };

                let status = match statuses.value(row_idx) {
                    "ok" => SpanStatus::Ok,
                    "error" => SpanStatus::Error,
                    _ => SpanStatus::Unset,
                };

                let start_time = Timestamp::from_nanos(start_times.value(row_idx));
                let end_time = Timestamp::from_nanos(end_times.value(row_idx));
                let duration = Duration::from_nanos(durations.value(row_idx));

                // Parse attributes from JSON
                let attributes = if attributes_json.is_null(row_idx) {
                    std::collections::HashMap::new()
                } else {
                    serde_json::from_str(attributes_json.value(row_idx))
                        .unwrap_or_else(|_| std::collections::HashMap::new())
                };

                // Parse events from JSON
                let events = if events_json.is_null(row_idx) {
                    Vec::new()
                } else {
                    serde_json::from_str(events_json.value(row_idx)).unwrap_or_else(|_| Vec::new())
                };

                spans.push(Span {
                    trace_id,
                    span_id,
                    parent_span_id,
                    service_name,
                    operation_name,
                    start_time,
                    end_time,
                    duration,
                    attributes,
                    events,
                    span_kind,
                    status,
                });
            }
        }

        Ok(spans)
    }

    /// Convert Arrow RecordBatch to LogEntry structs
    ///
    /// Similar to record_batches_to_spans, but for log entries
    fn record_batches_to_logs(batches: Vec<RecordBatch>) -> Result<Vec<LogEntry>> {
        use arrow::array::{
            Array, FixedSizeBinaryArray, StringArray, TimestampNanosecondArray, UInt8Array,
        };
        use sequins_core::models::{AttributeValue, LogId, LogSeverity, SpanId, TraceId};
        use std::collections::HashMap;
        use std::str::FromStr;
        use uuid::Uuid;

        let mut logs = Vec::new();

        for batch in batches {
            let num_rows = batch.num_rows();

            // Extract columns
            let log_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| Error::Storage("Invalid log_id column".to_string()))?;

            let timestamps = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Storage("Invalid timestamp_ns column".to_string()))?;

            let observed_timestamps = batch
                .column(2)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| {
                    Error::Storage("Invalid observed_timestamp_ns column".to_string())
                })?;

            let service_names = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid service_name column".to_string()))?;

            let severities = batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid severity column".to_string()))?;

            let severity_numbers = batch
                .column(5)
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| Error::Storage("Invalid severity_number column".to_string()))?;

            let bodies = batch
                .column(6)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid body column".to_string()))?;

            let trace_ids = batch
                .column(7)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| Error::Storage("Invalid trace_id column".to_string()))?;

            let span_ids = batch
                .column(8)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| Error::Storage("Invalid span_id column".to_string()))?;

            let attributes = batch
                .column(9)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid attributes column".to_string()))?;

            let resources = batch
                .column(10)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid resource column".to_string()))?;

            // Convert each row
            for row_idx in 0..num_rows {
                let id_bytes: [u8; 16] = log_ids
                    .value(row_idx)
                    .try_into()
                    .map_err(|_| Error::Storage("Invalid log_id bytes".to_string()))?;
                let id = LogId::from_uuid(Uuid::from_bytes(id_bytes));

                let timestamp = Timestamp::from_nanos(timestamps.value(row_idx));
                let observed_timestamp = Timestamp::from_nanos(observed_timestamps.value(row_idx));
                let service_name = service_names.value(row_idx).to_string();

                // Parse severity from string, or convert from number if string parsing fails
                let severity = LogSeverity::from_str(severities.value(row_idx))
                    .unwrap_or_else(|_| LogSeverity::from_number(severity_numbers.value(row_idx)));

                let body = bodies.value(row_idx).to_string();

                let trace_id = if trace_ids.is_null(row_idx) {
                    None
                } else {
                    Some(TraceId::from_bytes(
                        trace_ids
                            .value(row_idx)
                            .try_into()
                            .map_err(|_| Error::Storage("Invalid trace_id bytes".to_string()))?,
                    ))
                };

                let span_id = if span_ids.is_null(row_idx) {
                    None
                } else {
                    Some(SpanId::from_bytes(
                        span_ids
                            .value(row_idx)
                            .try_into()
                            .map_err(|_| Error::Storage("Invalid span_id bytes".to_string()))?,
                    ))
                };

                let attributes_map: HashMap<String, AttributeValue> = if attributes.is_null(row_idx)
                {
                    HashMap::new()
                } else {
                    serde_json::from_str(attributes.value(row_idx))
                        .unwrap_or_else(|_| HashMap::new())
                };

                let resource_map: HashMap<String, String> = if resources.is_null(row_idx) {
                    HashMap::new()
                } else {
                    serde_json::from_str(resources.value(row_idx))
                        .unwrap_or_else(|_| HashMap::new())
                };

                logs.push(LogEntry {
                    id,
                    timestamp,
                    observed_timestamp,
                    service_name,
                    severity,
                    body,
                    attributes: attributes_map,
                    trace_id,
                    span_id,
                    resource: resource_map,
                });
            }
        }

        Ok(logs)
    }

    /// Convert Arrow RecordBatch to Metric structs
    ///
    /// Similar to record_batches_to_spans, but for metric metadata
    fn record_batches_to_metrics(batches: Vec<RecordBatch>) -> Result<Vec<Metric>> {
        use arrow::array::{Array, FixedSizeBinaryArray, StringArray};
        use sequins_core::models::{MetricId, MetricType};
        use uuid::Uuid;

        let mut metrics = Vec::new();

        for batch in batches {
            let num_rows = batch.num_rows();

            // Extract columns
            let metric_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| Error::Storage("Invalid metric_id column".to_string()))?;

            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid name column".to_string()))?;

            let descriptions = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid description column".to_string()))?;

            let units = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid unit column".to_string()))?;

            let metric_types = batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid metric_type column".to_string()))?;

            let service_names = batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid service_name column".to_string()))?;

            // Convert each row
            for row_idx in 0..num_rows {
                let id_bytes: [u8; 16] = metric_ids
                    .value(row_idx)
                    .try_into()
                    .map_err(|_| Error::Storage("Invalid metric_id bytes".to_string()))?;
                let id = MetricId::from_uuid(Uuid::from_bytes(id_bytes));

                let name = names.value(row_idx).to_string();
                let description = if descriptions.is_null(row_idx) {
                    String::new()
                } else {
                    descriptions.value(row_idx).to_string()
                };

                let unit = if units.is_null(row_idx) {
                    String::new()
                } else {
                    units.value(row_idx).to_string()
                };

                // Parse metric type from string
                let metric_type = match metric_types.value(row_idx).to_lowercase().as_str() {
                    "counter" => MetricType::Counter,
                    "histogram" => MetricType::Histogram,
                    "summary" => MetricType::Summary,
                    _ => MetricType::Gauge, // Default to Gauge
                };

                let service_name = service_names.value(row_idx).to_string();

                metrics.push(Metric {
                    id,
                    name,
                    description,
                    unit,
                    metric_type,
                    service_name,
                });
            }
        }

        Ok(metrics)
    }

    /// Convert Arrow RecordBatch to Profile structs
    ///
    /// Similar to record_batches_to_spans, but for continuous profiling data
    fn record_batches_to_profiles(batches: Vec<RecordBatch>) -> Result<Vec<Profile>> {
        use arrow::array::{
            Array, BinaryArray, FixedSizeBinaryArray, StringArray, TimestampNanosecondArray,
        };
        use sequins_core::models::{ProfileId, ProfileType, TraceId};
        use std::str::FromStr;
        use uuid::Uuid;

        let mut profiles = Vec::new();

        for batch in batches {
            let num_rows = batch.num_rows();

            // Extract columns
            let profile_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| Error::Storage("Invalid profile_id column".to_string()))?;

            let timestamps = batch
                .column(1)
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .ok_or_else(|| Error::Storage("Invalid timestamp_ns column".to_string()))?;

            let service_names = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid service_name column".to_string()))?;

            let profile_types = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid profile_type column".to_string()))?;

            let sample_types = batch
                .column(4)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid sample_type column".to_string()))?;

            let sample_units = batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| Error::Storage("Invalid sample_unit column".to_string()))?;

            let trace_ids = batch
                .column(6)
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| Error::Storage("Invalid trace_id column".to_string()))?;

            let data_arrays = batch
                .column(7)
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| Error::Storage("Invalid data column".to_string()))?;

            // Convert each row
            for row_idx in 0..num_rows {
                let id_bytes: [u8; 16] = profile_ids
                    .value(row_idx)
                    .try_into()
                    .map_err(|_| Error::Storage("Invalid profile_id bytes".to_string()))?;
                let id = ProfileId::from_uuid(Uuid::from_bytes(id_bytes));

                let timestamp = Timestamp::from_nanos(timestamps.value(row_idx));
                let service_name = service_names.value(row_idx).to_string();

                let profile_type = ProfileType::from_str(profile_types.value(row_idx))
                    .unwrap_or(ProfileType::Other);

                let sample_type = sample_types.value(row_idx).to_string();
                let sample_unit = sample_units.value(row_idx).to_string();

                let trace_id = if trace_ids.is_null(row_idx) {
                    None
                } else {
                    Some(TraceId::from_bytes(
                        trace_ids
                            .value(row_idx)
                            .try_into()
                            .map_err(|_| Error::Storage("Invalid trace_id bytes".to_string()))?,
                    ))
                };

                let data = data_arrays.value(row_idx).to_vec();

                profiles.push(Profile {
                    id,
                    timestamp,
                    service_name,
                    profile_type,
                    sample_type,
                    sample_unit,
                    data,
                    trace_id,
                });
            }
        }

        Ok(profiles)
    }

    /// PROTOTYPE: Test unified queries with MemTable
    ///
    /// This is a quick validation - will be replaced with custom TableProvider
    ///
    /// **WARNING**: This is a prototype method and will be removed in Phase 2.
    /// Do not use in production code.
    ///
    /// # Errors
    ///
    /// Returns an error if DataFusion query fails
    pub async fn query_traces_memtable_prototype(
        &self,
        hot_tier: &crate::hot_tier::HotTier,
        query: &TraceQuery,
    ) -> Result<Vec<Span>> {
        use datafusion::datasource::MemTable;

        // Convert hot tier to RecordBatch
        let hot_spans = hot_tier.get_all_spans();
        let has_hot_data = !hot_spans.is_empty();

        // Create a new session context for this query
        let ctx = SessionContext::new();

        // Register hot tier as MemTable if not empty
        if has_hot_data {
            let hot_batch = Self::spans_to_record_batch(hot_spans, arrow_schema::span_schema())?;

            let hot_table =
                MemTable::try_new(arrow_schema::span_schema(), vec![vec![hot_batch]])
                    .map_err(|e| Error::Storage(format!("Failed to create MemTable: {}", e)))?;

            ctx.register_table("hot_spans_mem", Arc::new(hot_table))
                .map_err(|e| Error::Storage(format!("Failed to register MemTable: {}", e)))?;
        }

        // Check if cold tier has any parquet files
        let spans_path = format!("{}/spans", self.config.uri.trim_start_matches("file://"));
        let cold_has_data =
            std::path::Path::new(&spans_path).exists() && has_parquet_files(&spans_path);

        // Register cold tier Parquet files if they exist
        if cold_has_data {
            use datafusion::datasource::file_format::parquet::ParquetFormat;
            use datafusion::datasource::listing::{
                ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
            };
            use object_store::local::LocalFileSystem;

            // Create listing table for the spans directory
            // IMPORTANT: Directories MUST have a trailing slash for DataFusion
            let spans_path_raw = format!("{}/spans", self.config.uri.trim_start_matches("file://"));
            let spans_path = if spans_path_raw.starts_with('/') {
                format!("file://{}/", spans_path_raw) // Note the trailing slash
            } else {
                format!("file:///{}/", spans_path_raw) // Note the trailing slash
            };

            let table_path = ListingTableUrl::parse(&spans_path)
                .map_err(|e| Error::Storage(format!("Invalid table path: {}", e)))?;

            // Register object store with runtime_env (not ctx directly)
            let local_store = LocalFileSystem::new();
            let root_url = url::Url::parse("file:///")
                .map_err(|e| Error::Storage(format!("Invalid URL: {}", e)))?;
            ctx.runtime_env()
                .register_object_store(&root_url, Arc::new(local_store));

            let file_format = ParquetFormat::default();
            let listing_options =
                ListingOptions::new(Arc::new(file_format)).with_file_extension(".parquet");

            let config = ListingTableConfig::new(table_path)
                .with_listing_options(listing_options)
                .with_schema(arrow_schema::span_schema());

            let table = ListingTable::try_new(config)
                .map_err(|e| Error::Storage(format!("Failed to create listing table: {}", e)))?;

            ctx.register_table("cold_spans_parquet", Arc::new(table))
                .map_err(|e| Error::Storage(format!("Failed to register table: {}", e)))?;
        }

        // Build unified SQL query based on available data
        // Note: Cast i64 timestamps to proper Timestamp type using CAST
        let sql = if !has_hot_data && cold_has_data {
            // Only cold tier data
            format!(
                "SELECT * FROM cold_spans_parquet
                 WHERE start_time_ns >= to_timestamp_nanos({})
                   AND start_time_ns <= to_timestamp_nanos({})
                 ORDER BY start_time_ns DESC
                 LIMIT {}",
                query.start_time.as_nanos(),
                query.end_time.as_nanos(),
                query.limit.unwrap_or(100)
            )
        } else if has_hot_data && !cold_has_data {
            // Only hot tier data
            format!(
                "SELECT * FROM hot_spans_mem
                 WHERE start_time_ns >= to_timestamp_nanos({})
                   AND start_time_ns <= to_timestamp_nanos({})
                 ORDER BY start_time_ns DESC
                 LIMIT {}",
                query.start_time.as_nanos(),
                query.end_time.as_nanos(),
                query.limit.unwrap_or(100)
            )
        } else if has_hot_data && cold_has_data {
            // Both tiers have data - UNION ALL
            format!(
                "SELECT * FROM (
                    SELECT * FROM hot_spans_mem
                    UNION ALL
                    SELECT * FROM cold_spans_parquet
                 )
                 WHERE start_time_ns >= to_timestamp_nanos({})
                   AND start_time_ns <= to_timestamp_nanos({})
                 ORDER BY start_time_ns DESC
                 LIMIT {}",
                query.start_time.as_nanos(),
                query.end_time.as_nanos(),
                query.limit.unwrap_or(100)
            )
        } else {
            // No data in either tier
            return Ok(Vec::new());
        };

        // Execute query
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| Error::Storage(format!("SQL query failed: {}", e)))?;

        let batches = df
            .collect()
            .await
            .map_err(|e| Error::Storage(format!("Failed to collect results: {}", e)))?;

        // Convert back to Spans
        Self::record_batches_to_spans(batches)
    }
}

/// Helper function to check if a directory contains any parquet files recursively
fn has_parquet_files(path: &str) -> bool {
    use std::path::Path;

    fn check_dir_recursive(path: &Path) -> bool {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                let entry_path = entry.path();
                if entry_path.is_file() {
                    if let Some(ext) = entry_path.extension() {
                        if ext == "parquet" {
                            return true;
                        }
                    }
                } else if entry_path.is_dir() && check_dir_recursive(&entry_path) {
                    return true;
                }
            }
        }
        false
    }

    check_dir_recursive(Path::new(path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CompressionCodec;
    use sequins_core::models::{
        Duration, LogId, LogSeverity, MetricId, MetricType, ProfileId, ProfileType, SpanId,
        SpanKind, SpanStatus, TraceId,
    };
    use std::collections::HashMap;
    use tempfile::TempDir;

    fn create_test_config(temp_dir: &TempDir) -> ColdTierConfig {
        ColdTierConfig {
            uri: format!("file://{}", temp_dir.path().display()),
            enable_bloom_filters: false,
            compression: CompressionCodec::Snappy,
            row_group_size: 1000,
            index_path: None,
        }
    }

    fn create_test_span() -> Span {
        let start = Timestamp::now().unwrap();
        let end = start + Duration::from_secs(1);
        Span {
            trace_id: TraceId::from_bytes([1; 16]),
            span_id: SpanId::from_bytes([1; 8]),
            parent_span_id: None,
            service_name: "test-service".to_string(),
            operation_name: "test-op".to_string(),
            start_time: start,
            end_time: end,
            duration: Duration::from_secs(1),
            attributes: HashMap::new(),
            events: Vec::new(),
            span_kind: SpanKind::Internal,
            status: SpanStatus::Ok,
        }
    }

    fn create_test_log() -> LogEntry {
        LogEntry {
            id: LogId::new(),
            timestamp: Timestamp::now().unwrap(),
            observed_timestamp: Timestamp::now().unwrap(),
            service_name: "test-service".to_string(),
            severity: LogSeverity::Info,
            body: "Test log".to_string(),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            resource: HashMap::new(),
        }
    }

    fn create_test_metric() -> Metric {
        Metric {
            id: MetricId::new(),
            name: "test.metric".to_string(),
            description: "Test metric".to_string(),
            unit: "ms".to_string(),
            service_name: "test-service".to_string(),
            metric_type: MetricType::Gauge,
        }
    }

    fn create_test_profile() -> Profile {
        Profile {
            id: ProfileId::new(),
            timestamp: Timestamp::now().unwrap(),
            service_name: "test-service".to_string(),
            profile_type: ProfileType::Cpu,
            sample_type: "samples".to_string(),
            sample_unit: "count".to_string(),
            data: vec![1, 2, 3],
            trace_id: None,
        }
    }

    #[tokio::test]
    async fn test_cold_tier_new() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let cold_tier = ColdTier::new(config);
        assert!(cold_tier.is_ok());
    }

    #[tokio::test]
    async fn test_write_spans() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let cold_tier = ColdTier::new(config).unwrap();

        let spans = vec![create_test_span()];
        let result = cold_tier.write_spans(spans).await;
        if let Err(e) = &result {
            eprintln!("Error writing spans: {:?}", e);
        }
        assert!(result.is_ok());

        let path = result.unwrap();
        assert!(!path.is_empty());
        assert!(path.starts_with("spans/"));
    }

    #[tokio::test]
    async fn test_write_logs() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let cold_tier = ColdTier::new(config).unwrap();

        let logs = vec![create_test_log()];
        let result = cold_tier.write_logs(logs).await;
        assert!(result.is_ok());

        let path = result.unwrap();
        assert!(!path.is_empty());
        assert!(path.starts_with("logs/"));
    }

    #[tokio::test]
    async fn test_write_metrics() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let cold_tier = ColdTier::new(config).unwrap();

        let metrics = vec![create_test_metric()];
        let result = cold_tier.write_metrics(metrics).await;
        assert!(result.is_ok());

        let path = result.unwrap();
        assert!(!path.is_empty());
        assert!(path.starts_with("metrics/"));
    }

    #[tokio::test]
    async fn test_write_profiles() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let cold_tier = ColdTier::new(config).unwrap();

        let profiles = vec![create_test_profile()];
        let result = cold_tier.write_profiles(profiles).await;
        assert!(result.is_ok());

        let path = result.unwrap();
        assert!(!path.is_empty());
        assert!(path.starts_with("profiles/"));
    }

    #[tokio::test]
    async fn test_write_empty_spans() {
        let temp_dir = TempDir::new().unwrap();
        let config = create_test_config(&temp_dir);
        let cold_tier = ColdTier::new(config).unwrap();

        let spans: Vec<Span> = vec![];
        let result = cold_tier.write_spans(spans).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "");
    }

    #[test]
    fn test_partition_path_generation() {
        let timestamp = Timestamp::from_secs(1609459200); // 2021-01-01 00:00:00 UTC
        let path = ColdTier::generate_partition_path("spans", &timestamp);

        assert!(path.starts_with("spans/year="));
        assert!(path.contains("/month="));
        assert!(path.contains("/day="));
        assert!(path.ends_with(".parquet"));
    }
}
