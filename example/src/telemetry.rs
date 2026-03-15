use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{runtime, trace::TracerProvider, Resource};
use std::time::Duration;
use tracing_subscriber::prelude::*;

pub fn init_logs() -> opentelemetry_sdk::logs::LoggerProvider {
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4317");

    let logger_provider = opentelemetry_otlp::new_pipeline()
        .logging()
        .with_exporter(exporter)
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            "rust-test-app",
        )]))
        .install_batch(runtime::Tokio)
        .expect("Failed to create OTLP logger");

    let otel_layer =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider);

    tracing_subscriber::registry()
        .with(otel_layer)
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    logger_provider
}

pub fn init_tracer() -> TracerProvider {
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4317");

    opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .with_trace_config(opentelemetry_sdk::trace::Config::default().with_resource(
            Resource::new(vec![KeyValue::new("service.name", "rust-test-app")]),
        ))
        .install_batch(runtime::Tokio)
        .expect("Failed to create OTLP tracer")
}

pub fn init_metrics() -> opentelemetry_sdk::metrics::SdkMeterProvider {
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4317");

    opentelemetry_otlp::new_pipeline()
        .metrics(runtime::Tokio)
        .with_exporter(exporter)
        .with_period(Duration::from_secs(10))
        .with_resource(Resource::new(vec![KeyValue::new(
            "service.name",
            "rust-test-app",
        )]))
        .build()
        .expect("Failed to create OTLP metrics provider")
}

pub fn init() -> (
    opentelemetry_sdk::logs::LoggerProvider,
    TracerProvider,
    opentelemetry_sdk::metrics::SdkMeterProvider,
) {
    let logger = init_logs();
    let tracer = init_tracer();
    let meter = init_metrics();
    (logger, tracer, meter)
}
