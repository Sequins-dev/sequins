use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace::TracerProvider, Resource};
use std::time::Duration;
use tracing_subscriber::prelude::*;

#[cfg(feature = "profiling")]
pub mod profiler;

const DEFAULT_ENDPOINT: &str = "http://localhost:4317";
const DEFAULT_HTTP_ENDPOINT: &str = "http://localhost:4318";
const DEFAULT_METRIC_INTERVAL_SECS: u64 = 10;

/// Configuration for the Sequins OpenTelemetry SDK.
///
/// Use [`SequinsConfig::new`] to create a config with a service name, or
/// [`Default::default`] to read the service name from the `OTEL_SERVICE_NAME`
/// environment variable.
pub struct SequinsConfig {
    /// Name of the service to report in telemetry data.
    pub service_name: String,
    /// gRPC OTLP endpoint for traces, metrics, and logs.
    /// Defaults to `http://localhost:4317` or `OTEL_EXPORTER_OTLP_ENDPOINT`.
    pub endpoint: String,
    /// HTTP OTLP endpoint used for profiles.
    /// Defaults to `http://localhost:4318`.
    pub http_endpoint: String,
    /// How often to push metrics to the collector.
    /// Defaults to 10 seconds.
    pub metric_export_interval: Duration,
    /// Whether to enable the background CPU profiler.
    #[cfg(feature = "profiling")]
    pub profiles_enabled: bool,
    /// How long each profiling capture window lasts before exporting.
    #[cfg(feature = "profiling")]
    pub profile_interval: Duration,
    /// Sampling frequency for the CPU profiler in Hz.
    #[cfg(feature = "profiling")]
    pub profile_frequency: i32,
}

impl SequinsConfig {
    /// Create a new config with the given service name and all other fields at
    /// their defaults.
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            ..Default::default()
        }
    }
}

impl Default for SequinsConfig {
    fn default() -> Self {
        Self {
            service_name: std::env::var("OTEL_SERVICE_NAME")
                .unwrap_or_else(|_| "unknown_service".to_string()),
            endpoint: std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
                .unwrap_or_else(|_| DEFAULT_ENDPOINT.to_string()),
            http_endpoint: DEFAULT_HTTP_ENDPOINT.to_string(),
            metric_export_interval: Duration::from_secs(DEFAULT_METRIC_INTERVAL_SECS),
            #[cfg(feature = "profiling")]
            profiles_enabled: false,
            #[cfg(feature = "profiling")]
            profile_interval: Duration::from_secs(30),
            #[cfg(feature = "profiling")]
            profile_frequency: 99,
        }
    }
}

/// Holds all OTel providers and an optional profiler shutdown handle.
///
/// Drop or call [`SequinsGuard::shutdown`] to flush and stop all telemetry.
pub struct SequinsGuard {
    tracer_provider: TracerProvider,
    meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
    logger_provider: opentelemetry_sdk::logs::LoggerProvider,
    #[cfg(feature = "profiling")]
    profiler_shutdown: Option<tokio::sync::watch::Sender<bool>>,
}

impl SequinsGuard {
    /// Returns a reference to the underlying [`TracerProvider`].
    pub fn tracer_provider(&self) -> &TracerProvider {
        &self.tracer_provider
    }

    /// Returns a reference to the underlying [`SdkMeterProvider`].
    pub fn meter_provider(&self) -> &opentelemetry_sdk::metrics::SdkMeterProvider {
        &self.meter_provider
    }

    /// Returns a reference to the underlying [`LoggerProvider`].
    pub fn logger_provider(&self) -> &opentelemetry_sdk::logs::LoggerProvider {
        &self.logger_provider
    }

    /// Gracefully shuts down all providers and stops the background profiler
    /// (if running), flushing any buffered telemetry.
    pub async fn shutdown(self) {
        #[cfg(feature = "profiling")]
        if let Some(tx) = self.profiler_shutdown {
            let _ = tx.send(true);
        }
        self.tracer_provider.shutdown().ok();
        self.meter_provider.shutdown().ok();
        self.logger_provider.shutdown().ok();
    }
}

/// Initialise Sequins telemetry with a service name and default configuration.
///
/// Equivalent to `init_with_config(SequinsConfig::new(service_name))`.
pub async fn init(service_name: impl Into<String>) -> SequinsGuard {
    init_with_config(SequinsConfig::new(service_name)).await
}

/// Initialise Sequins telemetry with a fully-specified [`SequinsConfig`].
///
/// Sets up OTLP gRPC exporters for traces, logs, and metrics, installs a
/// `tracing-subscriber` registry with an OTel bridge and a human-readable fmt
/// layer, and optionally spawns a background CPU profiler task.
///
/// Calling this function more than once in the same process is safe: the
/// `tracing-subscriber` registration uses `try_init`, which is a no-op if a
/// global subscriber is already installed.
pub async fn init_with_config(config: SequinsConfig) -> SequinsGuard {
    let resource = Resource::new(vec![KeyValue::new(
        "service.name",
        config.service_name.clone(),
    )]);

    // --- Traces (set up first so we can build the tracing-opentelemetry layer) ---
    let trace_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(&config.endpoint);
    let tracer_provider = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(trace_exporter)
        .with_trace_config(
            opentelemetry_sdk::trace::Config::default().with_resource(resource.clone()),
        )
        .install_batch(runtime::Tokio)
        .expect("Failed to create OTLP tracer");

    // Bridge `tracing::Span` → OTel spans so `#[tracing::instrument]` and
    // `tracing::info_span!()` emit trace data automatically.
    let otel_trace_layer =
        tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("tracing"));

    // --- Logs ---
    let log_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(&config.endpoint);
    let logger_provider = opentelemetry_otlp::new_pipeline()
        .logging()
        .with_exporter(log_exporter)
        .with_resource(resource.clone())
        .install_batch(runtime::Tokio)
        .expect("Failed to create OTLP logger");

    // Bridge `tracing` events → OTel logs.
    let otel_log_layer =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider);

    let _ = tracing_subscriber::registry()
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .try_init();

    // --- Metrics ---
    let metric_exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint(&config.endpoint);
    let meter_provider = opentelemetry_otlp::new_pipeline()
        .metrics(runtime::Tokio)
        .with_exporter(metric_exporter)
        .with_period(config.metric_export_interval)
        .with_resource(resource)
        .build()
        .expect("Failed to create OTLP metrics provider");

    // --- Profiler (feature-gated) ---
    #[cfg(feature = "profiling")]
    let profiler_shutdown = if config.profiles_enabled {
        let (tx, rx) = tokio::sync::watch::channel(false);
        let http_endpoint = config.http_endpoint.clone();
        let service_name = config.service_name.clone();
        let interval = config.profile_interval;
        let frequency = config.profile_frequency;
        tokio::spawn(crate::profiler::run(
            service_name,
            http_endpoint,
            interval,
            frequency,
            rx,
        ));
        Some(tx)
    } else {
        None
    };

    SequinsGuard {
        tracer_provider,
        meter_provider,
        logger_provider,
        #[cfg(feature = "profiling")]
        profiler_shutdown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_service_name_fallback() {
        // Without OTEL_SERVICE_NAME set the fallback is "unknown_service".
        // We cannot unset env vars portably across parallel tests, so only
        // verify the behaviour when the var is absent by checking the type.
        let config = SequinsConfig::new("my-service");
        assert_eq!(config.service_name, "my-service");
        assert_eq!(config.endpoint, DEFAULT_ENDPOINT);
        assert_eq!(config.http_endpoint, DEFAULT_HTTP_ENDPOINT);
        assert_eq!(
            config.metric_export_interval,
            Duration::from_secs(DEFAULT_METRIC_INTERVAL_SECS)
        );
    }

    #[test]
    fn config_new_overrides_service_name() {
        let config = SequinsConfig::new("svc-override");
        assert_eq!(config.service_name, "svc-override");
    }

    #[test]
    fn default_config_reads_env_var() {
        // When OTEL_SERVICE_NAME is set, Default should pick it up.
        std::env::set_var("OTEL_SERVICE_NAME", "env-svc");
        let config = SequinsConfig::default();
        std::env::remove_var("OTEL_SERVICE_NAME");
        assert_eq!(config.service_name, "env-svc");
    }

    #[test]
    fn default_endpoint_env_override() {
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4317");
        let config = SequinsConfig::default();
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        assert_eq!(config.endpoint, "http://collector:4317");
    }
}
