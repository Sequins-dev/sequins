use super::{MaintenanceStats, Storage};
use crate::error::Result;
use crate::hot_tier::batch_chain::BatchMeta;
use std::sync::Arc;

impl Storage {
    /// Run periodic maintenance tasks.
    ///
    /// Cold-tier flushing is now handled by the per-chain compactor background
    /// tasks spawned at HotTier construction time.  This method handles any
    /// remaining bookkeeping (e.g. future eviction policies).
    ///
    /// # Errors
    ///
    /// Returns an error if any maintenance step fails.
    pub async fn run_maintenance_internal(&self) -> Result<MaintenanceStats> {
        Ok(MaintenanceStats {
            entries_evicted: 0,
            batches_flushed: 0,
        })
    }

    /// Generate test data for development/debugging purposes.
    ///
    /// Creates synthetic traces with nested spans and pushes them directly into
    /// the hot-tier BatchChains.  Useful for testing the UI without needing an
    /// external data generator.
    ///
    /// # Returns
    ///
    /// The number of spans created on success.
    ///
    /// # Errors
    ///
    /// Returns an error if batch construction or chain push fails.
    pub fn generate_test_data(&self) -> Result<usize> {
        use arrow::array::{
            new_null_array, Int64Array, StringViewArray, TimestampNanosecondArray, UInt32Array,
            UInt8Array,
        };
        let now_ns = self.clock.now_ns() as i64;

        // Build raw column vectors directly.
        let mut trace_ids: Vec<String> = Vec::new();
        let mut span_ids: Vec<String> = Vec::new();
        let mut parent_span_ids: Vec<Option<String>> = Vec::new();
        let mut names: Vec<String> = Vec::new();
        let mut kinds: Vec<u8> = Vec::new();
        let mut statuses: Vec<u8> = Vec::new();
        let mut start_times: Vec<i64> = Vec::new();
        let mut end_times: Vec<i64> = Vec::new();
        let mut durations: Vec<i64> = Vec::new();
        let mut resource_ids: Vec<u32> = Vec::new();
        let mut scope_ids: Vec<u32> = Vec::new();

        let test_traces = [
            (
                "web-service",
                "http-request",
                vec![
                    ("db-query", "SELECT * FROM users"),
                    ("cache-lookup", "GET user:123"),
                ],
            ),
            (
                "api-gateway",
                "route-request",
                vec![
                    ("auth-check", "verify-token"),
                    ("rate-limit", "check-quota"),
                ],
            ),
            (
                "worker-service",
                "process-job",
                vec![
                    ("fetch-data", "GET /api/data"),
                    ("transform", "apply-rules"),
                    ("publish-result", "POST /api/results"),
                ],
            ),
        ];

        for (trace_idx, (service_name, root_operation, child_operations)) in
            test_traces.into_iter().enumerate()
        {
            // Register the resource so it appears in the resources chain.
            let mut resource_attrs = std::collections::HashMap::new();
            resource_attrs.insert("service.name".to_string(), service_name.to_string());
            resource_attrs.insert("service.version".to_string(), "1.0.0".to_string());
            let resource_id = self
                .hot_tier
                .register_resource(&resource_attrs)
                .map_err(|e| {
                    crate::error::Error::Storage(format!("register_resource failed: {}", e))
                })?;

            let ts_offset = (trace_idx as i64) * 1_000_000_000;
            let trace_hex = format!("{:032x}", trace_idx as u128);
            let root_span_hex = format!("{:016x}", (trace_idx * 1000) as u64);
            let root_start_ns = now_ns + ts_offset;
            let root_duration_ns = (100 + child_operations.len() as i64 * 20) * 1_000_000;
            let root_end_ns = root_start_ns + root_duration_ns;

            // Root span (Server kind=2, Status=Ok=1)
            trace_ids.push(trace_hex.clone());
            span_ids.push(root_span_hex.clone());
            parent_span_ids.push(None);
            names.push(root_operation.to_string());
            kinds.push(2u8); // Server
            statuses.push(1u8); // Ok
            start_times.push(root_start_ns);
            end_times.push(root_end_ns);
            durations.push(root_duration_ns);
            resource_ids.push(resource_id);
            scope_ids.push(0);

            let mut current_time = root_start_ns + 5_000_000;
            for (child_idx, (child_operation, _details)) in child_operations.into_iter().enumerate()
            {
                let child_span_hex = format!("{:016x}", (trace_idx * 1000 + child_idx + 1) as u64);
                let child_duration_ns = 15_000_000i64;
                let child_end = current_time + child_duration_ns;

                trace_ids.push(trace_hex.clone());
                span_ids.push(child_span_hex);
                parent_span_ids.push(Some(root_span_hex.clone()));
                names.push(child_operation.to_string());
                kinds.push(3u8); // Client
                statuses.push(1u8); // Ok
                start_times.push(current_time);
                end_times.push(child_end);
                durations.push(child_duration_ns);
                resource_ids.push(resource_id);
                scope_ids.push(0);

                current_time = child_end + 2_000_000;
            }
        }

        let span_count = trace_ids.len();

        if span_count > 0 {
            // Use the full span schema (includes promoted attribute columns + overflow map)
            // so the batch is compatible with the DataFusion table provider.
            let schema = sequins_types::arrow_schema::span_schema();

            // Build the 11 core column arrays.
            let mut columns: Vec<Arc<dyn arrow::array::Array>> = vec![
                Arc::new(StringViewArray::from(
                    trace_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringViewArray::from(
                    span_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as _,
                Arc::new(StringViewArray::from(parent_span_ids)) as _,
                Arc::new(StringViewArray::from(
                    names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )) as _,
                Arc::new(UInt8Array::from(kinds)) as _,
                Arc::new(UInt8Array::from(statuses)) as _,
                Arc::new(TimestampNanosecondArray::from(start_times)) as _,
                Arc::new(TimestampNanosecondArray::from(end_times)) as _,
                Arc::new(Int64Array::from(durations)) as _,
                Arc::new(UInt32Array::from(resource_ids)) as _,
                Arc::new(UInt32Array::from(scope_ids)) as _,
            ];

            // Add null arrays for any extra schema fields (promoted attrs + overflow map).
            let num_extra = schema.fields().len() - 11;
            for field in schema.fields().iter().skip(11) {
                columns.push(new_null_array(field.data_type(), span_count));
            }
            let _ = num_extra; // used implicitly above

            let batch =
                arrow::record_batch::RecordBatch::try_new(schema, columns).map_err(|e| {
                    crate::error::Error::Storage(format!("Failed to build test span batch: {}", e))
                })?;

            let meta = BatchMeta {
                min_timestamp: 0,
                max_timestamp: i64::MAX,
                row_count: batch.num_rows(),
            };
            self.hot_tier.spans.push(Arc::new(batch), meta);
        }

        Ok(span_count)
    }
}
