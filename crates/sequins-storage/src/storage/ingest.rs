use super::Storage;
use crate::hot_tier::batch_chain::BatchMeta;
use crate::wal::WalPayload;
use sequins_query::ast::Signal;
use std::sync::Arc;

/// Build a simple `BatchMeta` from a batch — uses row count only for now.
///
/// Timestamp range optimisation can be added later once we decide which
/// column to use per signal type.
fn simple_meta(row_count: usize) -> BatchMeta {
    BatchMeta {
        min_timestamp: 0,
        max_timestamp: i64::MAX,
        row_count,
    }
}

impl Storage {
    /// Register a resource from an OTLP proto Resource, returning its content-addressed ID.
    fn register_resource(
        &self,
        resource: Option<&opentelemetry_proto::tonic::resource::v1::Resource>,
    ) -> sequins_types::error::Result<u32> {
        let resource_attrs = resource
            .map(|r| sequins_otlp::convert_resource_attributes(&r.attributes))
            .unwrap_or_default();

        self.hot_tier
            .register_resource(&resource_attrs)
            .map_err(|e| {
                sequins_types::error::Error::Other(format!("Failed to register resource: {}", e))
            })
    }

    /// Register a scope from an OTLP InstrumentationScope, returning its content-addressed ID.
    fn register_scope(
        &self,
        scope: Option<&opentelemetry_proto::tonic::common::v1::InstrumentationScope>,
    ) -> sequins_types::error::Result<u32> {
        let scope_model = sequins_otlp::convert_otlp_scope(scope);
        self.hot_tier.register_scope(&scope_model).map_err(|e| {
            sequins_types::error::Error::Other(format!("Failed to register scope: {}", e))
        })
    }
}

#[async_trait::async_trait]
impl sequins_types::OtlpIngest for Storage {
    #[tracing::instrument(skip_all, name = "ingest_traces")]
    async fn ingest_traces(
        &self,
        request: opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest,
    ) -> sequins_types::error::Result<
        opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse,
    > {
        use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceResponse;

        self.wal
            .append(WalPayload::Traces(request.clone()), self.clock.now_ns())
            .await
            .map_err(|e| {
                sequins_types::error::Error::Other(format!("Failed to append to WAL: {}", e))
            })?;

        let catalog = sequins_types::arrow_schema::default_schema_catalog();

        let mut span_items = Vec::new();
        let mut span_events = Vec::new();
        let mut span_links = Vec::new();

        for resource_spans in request.resource_spans {
            let resource = resource_spans.resource.as_ref();
            let resource_id = self.register_resource(resource)?;

            for scope_spans in resource_spans.scope_spans {
                let scope_id = self.register_scope(scope_spans.scope.as_ref())?;

                for otlp_span in scope_spans.spans {
                    let trace_id_hex = otlp_span
                        .trace_id
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>();
                    let span_id_hex = otlp_span
                        .span_id
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<String>();

                    for event in &otlp_span.events {
                        span_events.push((
                            trace_id_hex.clone(),
                            span_id_hex.clone(),
                            event.clone(),
                        ));
                    }
                    for link in &otlp_span.links {
                        span_links.push((trace_id_hex.clone(), span_id_hex.clone(), link.clone()));
                    }

                    span_items.push((otlp_span, resource_id, scope_id));
                }
            }
        }

        if !span_items.is_empty() {
            match sequins_otlp::otlp_spans_to_batch(span_items, catalog) {
                Ok(batch) => {
                    let batch = Arc::new(batch);
                    let meta = simple_meta(batch.num_rows());
                    self.hot_tier.spans.push(Arc::clone(&batch), meta);
                    let _ = self.live_broadcast.send((Signal::Spans, batch));
                }
                Err(e) => tracing::warn!(error = %e, "Failed to convert spans to batch"),
            }
        }

        if !span_events.is_empty() {
            match sequins_otlp::otlp_span_events_to_batch(span_events) {
                Ok(batch) => {
                    let meta = simple_meta(batch.num_rows());
                    self.hot_tier.span_events.push(Arc::new(batch), meta);
                }
                Err(e) => tracing::warn!(error = %e, "Failed to convert span events to batch"),
            }
        }

        if !span_links.is_empty() {
            match sequins_otlp::otlp_span_links_to_batch(span_links) {
                Ok(batch) => {
                    let meta = simple_meta(batch.num_rows());
                    self.hot_tier.span_links.push(Arc::new(batch), meta);
                }
                Err(e) => tracing::warn!(error = %e, "Failed to convert span links to batch"),
            }
        }

        Ok(ExportTraceServiceResponse {
            partial_success: None,
        })
    }

    #[tracing::instrument(skip_all, name = "ingest_logs")]
    async fn ingest_logs(
        &self,
        request: opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest,
    ) -> sequins_types::error::Result<
        opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse,
    > {
        use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceResponse;

        self.wal
            .append(WalPayload::Logs(request.clone()), self.clock.now_ns())
            .await
            .map_err(|e| {
                sequins_types::error::Error::Other(format!("Failed to append to WAL: {}", e))
            })?;

        let catalog = sequins_types::arrow_schema::default_schema_catalog();

        let mut log_items: Vec<(
            opentelemetry_proto::tonic::logs::v1::LogRecord,
            u32,
            u32,
            String,
        )> = Vec::new();

        for resource_logs in request.resource_logs {
            let resource = resource_logs.resource.as_ref();
            let resource_id = self.register_resource(resource)?;
            let service_name = sequins_otlp::extract_service_name(resource);

            for scope_logs in resource_logs.scope_logs {
                let scope_id = self.register_scope(scope_logs.scope.as_ref())?;

                for otlp_log in scope_logs.log_records {
                    log_items.push((otlp_log, resource_id, scope_id, service_name.clone()));
                }
            }
        }

        if !log_items.is_empty() {
            match sequins_otlp::otlp_logs_to_batch(log_items, catalog) {
                Ok(batch) => {
                    let batch = Arc::new(batch);
                    let meta = simple_meta(batch.num_rows());
                    self.hot_tier.logs.push(Arc::clone(&batch), meta);
                    let _ = self.live_broadcast.send((Signal::Logs, batch));
                }
                Err(e) => tracing::warn!(error = %e, "Failed to convert logs to batch"),
            }
        }

        Ok(ExportLogsServiceResponse {
            partial_success: None,
        })
    }

    #[tracing::instrument(skip_all, name = "ingest_metrics")]
    async fn ingest_metrics(
        &self,
        request: opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest,
    ) -> sequins_types::error::Result<
        opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse,
    > {
        use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceResponse;

        self.wal
            .append(WalPayload::Metrics(request.clone()), self.clock.now_ns())
            .await
            .map_err(|e| {
                sequins_types::error::Error::Other(format!("Failed to append to WAL: {}", e))
            })?;

        // Collect (OtlpMetric, resource_id, scope_id, service_name) tuples for direct conversion
        let mut items: Vec<(
            opentelemetry_proto::tonic::metrics::v1::Metric,
            u32,
            u32,
            String,
        )> = Vec::new();

        for resource_metrics in request.resource_metrics {
            let resource = resource_metrics.resource.as_ref();
            let resource_id = self.register_resource(resource)?;
            let service_name = sequins_otlp::extract_service_name(resource);

            for scope_metrics in resource_metrics.scope_metrics {
                let scope_id = self.register_scope(scope_metrics.scope.as_ref())?;

                for otlp_metric in scope_metrics.metrics {
                    items.push((otlp_metric, resource_id, scope_id, service_name.clone()));
                }
            }
        }

        if items.is_empty() {
            return Ok(ExportMetricsServiceResponse {
                partial_success: None,
            });
        }

        // Gauge/counter data points → hot tier + broadcast
        match sequins_otlp::otlp_datapoints_to_batch(&items) {
            Ok(batch) if batch.num_rows() > 0 => {
                let batch = Arc::new(batch);
                let meta = simple_meta(batch.num_rows());
                self.hot_tier.datapoints.push(Arc::clone(&batch), meta);
                let _ = self.live_broadcast.send((Signal::Datapoints, batch));
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "Failed to convert datapoints to batch"),
        }

        // Explicit histogram data points → hot tier + broadcast
        match sequins_otlp::otlp_histograms_to_batch(&items) {
            Ok(batch) if batch.num_rows() > 0 => {
                let batch = Arc::new(batch);
                let meta = simple_meta(batch.num_rows());
                self.hot_tier
                    .histogram_datapoints
                    .push(Arc::clone(&batch), meta);
                let _ = self.live_broadcast.send((Signal::Histograms, batch));
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "Failed to convert histogram datapoints to batch"),
        }

        // Exponential histogram data points → hot tier
        match sequins_otlp::otlp_exp_histograms_to_batch(&items) {
            Ok(batch) if batch.num_rows() > 0 => {
                let meta = simple_meta(batch.num_rows());
                self.hot_tier
                    .exponential_histogram_datapoints
                    .push(Arc::new(batch), meta);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(error = %e, "Failed to convert exp histogram datapoints to batch")
            }
        }

        // Metric metadata → hot tier + broadcast, deduplicated by metric_id.
        // Only items whose metric_id has not been seen before are pushed, matching
        // the same DashSet pattern used by register_resource / register_scope.
        let new_metric_items: Vec<_> = items
            .iter()
            .filter(|(metric, resource_id, scope_id, _service_name)| {
                let mtype = sequins_otlp::otlp_metric_type(metric);
                let id = sequins_types::models::MetricId::from_fields(
                    &metric.name,
                    &metric.description,
                    &metric.unit,
                    mtype,
                    *resource_id,
                    *scope_id,
                );
                self.hot_tier.is_new_metric(*id.as_uuid().as_bytes())
            })
            .cloned()
            .collect();

        if !new_metric_items.is_empty() {
            match sequins_otlp::otlp_metrics_to_batch(&new_metric_items) {
                Ok(batch) if batch.num_rows() > 0 => {
                    let batch = Arc::new(batch);
                    let meta = simple_meta(batch.num_rows());
                    self.hot_tier.metrics.push(Arc::clone(&batch), meta);
                    let _ = self.live_broadcast.send((Signal::Metrics, batch));
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "Failed to convert metrics to batch"),
            }
        }

        Ok(ExportMetricsServiceResponse {
            partial_success: None,
        })
    }

    #[tracing::instrument(skip_all, name = "ingest_profiles")]
    async fn ingest_profiles(
        &self,
        request: opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest,
    ) -> sequins_types::error::Result<
        opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceResponse,
    >{
        use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceResponse;

        self.wal
            .append(WalPayload::Profiles(request.clone()), self.clock.now_ns())
            .await
            .map_err(|e| {
                sequins_types::error::Error::Other(format!("Failed to append to WAL: {}", e))
            })?;

        let dictionary = request.dictionary.as_ref();

        // Collect (OtlpProfile, resource_id, scope_id, service_name) tuples
        let mut items: Vec<(
            opentelemetry_proto::tonic::profiles::v1development::Profile,
            u32,
            u32,
            String,
        )> = Vec::new();

        for resource_profile in request.resource_profiles {
            let resource = resource_profile.resource.as_ref();
            let resource_id = self.register_resource(resource)?;
            let service_name = sequins_otlp::extract_service_name(resource);

            for scope_profile in resource_profile.scope_profiles {
                let scope_id = self.register_scope(scope_profile.scope.as_ref())?;

                for otlp_profile in scope_profile.profiles {
                    items.push((otlp_profile, resource_id, scope_id, service_name.clone()));
                }
            }
        }

        if items.is_empty() {
            return Ok(ExportProfilesServiceResponse {
                partial_success: None,
            });
        }

        match sequins_otlp::otlp_profiles_to_batches(&items, dictionary) {
            Ok(batches) => {
                if batches.profiles.num_rows() > 0 {
                    let batch = Arc::new(batches.profiles);
                    let meta = simple_meta(batch.num_rows());
                    self.hot_tier.profiles.push(Arc::clone(&batch), meta);
                    let _ = self.live_broadcast.send((Signal::Profiles, batch));
                }
                if batches.frames.num_rows() > 0 {
                    if let Some(new_frames) = self.hot_tier.filter_new_frames(&batches.frames) {
                        let batch = Arc::new(new_frames);
                        let meta = simple_meta(batch.num_rows());
                        self.hot_tier.frames.push(Arc::clone(&batch), meta);
                        let _ = self.live_broadcast.send((Signal::Frames, batch));
                    }
                }
                if batches.stacks.num_rows() > 0 {
                    if let Some(new_stacks) = self.hot_tier.filter_new_stacks(&batches.stacks) {
                        let batch = Arc::new(new_stacks);
                        let meta = simple_meta(batch.num_rows());
                        self.hot_tier.stacks.push(Arc::clone(&batch), meta);
                        let _ = self.live_broadcast.send((Signal::Stacks, batch));
                    }
                }
                if batches.samples.num_rows() > 0 {
                    let batch = Arc::new(batches.samples);
                    let meta = simple_meta(batch.num_rows());
                    self.hot_tier.samples.push(Arc::clone(&batch), meta);
                    let _ = self.live_broadcast.send((Signal::Samples, batch));
                }
                if batches.mappings.num_rows() > 0 {
                    if let Some(new_mappings) = self.hot_tier.filter_new_mappings(&batches.mappings)
                    {
                        let batch = Arc::new(new_mappings);
                        let meta = simple_meta(batch.num_rows());
                        self.hot_tier.mappings.push(Arc::clone(&batch), meta);
                        let _ = self.live_broadcast.send((Signal::Mappings, batch));
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "Failed to convert profiles to batches"),
        }

        Ok(ExportProfilesServiceResponse {
            partial_success: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{
        make_test_otlp_logs, make_test_otlp_metrics, make_test_otlp_profiles_with_samples,
        make_test_otlp_traces, TestStorageBuilder,
    };
    use sequins_types::OtlpIngest;
    use std::time::Duration;

    #[tokio::test]
    async fn test_ingest_traces_broadcasts_to_channel() {
        let (storage, _tmp) = TestStorageBuilder::new().build().await;
        let mut rx = storage.live_broadcast.subscribe();

        let request = make_test_otlp_traces(1, 3);
        storage.ingest_traces(request).await.unwrap();

        // A broadcast message should be available immediately
        let result = tokio::time::timeout(Duration::from_millis(200), async {
            rx.recv().await.unwrap()
        })
        .await
        .expect("timeout: no broadcast received after ingest_traces");

        let (signal, batch) = result;
        assert_eq!(signal, Signal::Spans);
        assert!(batch.num_rows() > 0, "broadcast batch should be non-empty");
    }

    #[tokio::test]
    async fn test_ingest_logs_broadcasts_to_channel() {
        let (storage, _tmp) = TestStorageBuilder::new().build().await;
        let mut rx = storage.live_broadcast.subscribe();

        let request = make_test_otlp_logs(1, 5);
        storage.ingest_logs(request).await.unwrap();

        let result = tokio::time::timeout(Duration::from_millis(200), async {
            rx.recv().await.unwrap()
        })
        .await
        .expect("timeout: no broadcast received after ingest_logs");

        let (signal, batch) = result;
        assert_eq!(signal, Signal::Logs);
        assert!(batch.num_rows() > 0, "broadcast batch should be non-empty");
    }

    #[tokio::test]
    async fn test_ingest_metrics_broadcasts_to_channel() {
        let (storage, _tmp) = TestStorageBuilder::new().build().await;
        let mut rx = storage.live_broadcast.subscribe();

        let request = make_test_otlp_metrics(1, 2, 3);
        storage.ingest_metrics(request).await.unwrap();

        // Metrics broadcasts Datapoints signal
        let result = tokio::time::timeout(Duration::from_millis(200), async {
            rx.recv().await.unwrap()
        })
        .await
        .expect("timeout: no broadcast received after ingest_metrics");

        let (signal, batch) = result;
        assert_eq!(signal, Signal::Datapoints);
        assert!(batch.num_rows() > 0, "broadcast batch should be non-empty");
    }

    #[tokio::test]
    async fn test_ingest_profiles_broadcasts_samples() {
        let (storage, _tmp) = TestStorageBuilder::new().build().await;
        let mut rx = storage.live_broadcast.subscribe();

        let request = make_test_otlp_profiles_with_samples();
        storage.ingest_profiles(request).await.unwrap();

        // Drain broadcasts until we see Signal::Samples (Profiles may arrive first)
        let found = tokio::time::timeout(Duration::from_millis(200), async {
            loop {
                let (signal, batch) = rx.recv().await.unwrap();
                if signal == Signal::Samples {
                    assert!(
                        batch.num_rows() > 0,
                        "samples broadcast batch should be non-empty"
                    );
                    return;
                }
            }
        })
        .await;

        assert!(
            found.is_ok(),
            "timeout: Signal::Samples not broadcast after ingest_profiles"
        );
    }

    #[tokio::test]
    async fn test_multiple_receivers_get_broadcast() {
        let (storage, _tmp) = TestStorageBuilder::new().build().await;
        let mut rx1 = storage.live_broadcast.subscribe();
        let mut rx2 = storage.live_broadcast.subscribe();

        let request = make_test_otlp_traces(1, 2);
        storage.ingest_traces(request).await.unwrap();

        // Both receivers should get the same batch
        let (sig1, batch1) = tokio::time::timeout(Duration::from_millis(200), rx1.recv())
            .await
            .expect("rx1 timeout")
            .unwrap();

        let (sig2, batch2) = tokio::time::timeout(Duration::from_millis(200), rx2.recv())
            .await
            .expect("rx2 timeout")
            .unwrap();

        assert_eq!(sig1, Signal::Spans);
        assert_eq!(sig2, Signal::Spans);
        assert_eq!(batch1.num_rows(), batch2.num_rows());
    }
}
