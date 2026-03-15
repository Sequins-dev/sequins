mod profiler;
mod telemetry;
mod traffic;

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tracing::info;

#[tokio::main]
async fn main() {
    let (logger_provider, tracer_provider, meter_provider) = telemetry::init();

    info!("Sequins test-app started — continuous traffic generator");

    let shutdown = Arc::new(AtomicBool::new(false));

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let traffic_handle = tokio::task::spawn_local(traffic::run(
                tracer_provider.clone(),
                meter_provider.clone(),
                shutdown.clone(),
            ));
            let profiler_handle = tokio::task::spawn_local(profiler::run(shutdown.clone()));

            tokio::signal::ctrl_c()
                .await
                .expect("Failed to listen for ctrl-c");
            info!("Shutdown signal received, stopping...");
            shutdown.store(true, Ordering::Relaxed);

            let _ = tokio::join!(traffic_handle, profiler_handle);
        })
        .await;

    info!("Flushing telemetry...");
    tracer_provider.force_flush();
    let _ = meter_provider.force_flush();
    let _ = logger_provider.force_flush();

    let _ = tracer_provider.shutdown();
    let _ = meter_provider.shutdown();
    let _ = logger_provider.shutdown();

    opentelemetry::global::shutdown_tracer_provider();
    info!("Shutdown complete");
}
