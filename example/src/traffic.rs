use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::trace::{SpanKind, TraceContextExt, Tracer, TracerProvider as _};
use opentelemetry::{Context, KeyValue};
use opentelemetry_sdk::{metrics::SdkMeterProvider, trace::TracerProvider};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tracing::{error, info, warn};

static ENDPOINTS: &[(&str, &str)] = &[
    ("GET", "/api/users"),
    ("POST", "/api/orders"),
    ("GET", "/api/health"),
    ("GET", "/api/products"),
    ("DELETE", "/api/sessions"),
];

#[inline(never)]
fn process_row(row: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(row.as_bytes());
    hasher.finalize().to_vec()
}

#[inline(never)]
fn simulate_db_query(row_count: usize) -> Vec<Vec<u8>> {
    let mut rows: Vec<String> = (0..row_count)
        .map(|i| format!("row_{i}_data_{}", i * 17 + 3))
        .collect();
    rows.sort();
    rows.iter().map(|r| process_row(r)).collect()
}

#[inline(never)]
fn build_response_payload() -> serde_json::Value {
    let users: Vec<serde_json::Value> = (0..50)
        .map(|i| {
            let scores: Vec<f64> = (0..20).map(|j| (i * 20 + j) as f64 * 0.37).collect();
            serde_json::json!({
                "id": i,
                "name": format!("user_{i}"),
                "scores": scores,
            })
        })
        .collect();
    serde_json::json!({ "users": users, "total": users.len() })
}

#[inline(never)]
fn simulate_serialization() -> String {
    let payload = build_response_payload();
    let serialized = serde_json::to_string(&payload).unwrap_or_default();
    // Deserialize back to validate
    let _parsed: serde_json::Value = serde_json::from_str(&serialized).unwrap_or_default();
    serialized
}

pub async fn run(
    tracer_provider: TracerProvider,
    meter_provider: SdkMeterProvider,
    shutdown: Arc<AtomicBool>,
) {
    let tracer = tracer_provider.tracer("rust-test-app");
    let meter = meter_provider.meter("rust-test-app");

    let request_counter = meter
        .u64_counter("http.server.request.count")
        .with_description("Total number of HTTP requests")
        .with_unit("requests")
        .init();

    let request_duration = meter
        .f64_histogram("http.server.request.duration")
        .with_description("HTTP request duration")
        .with_unit("ms")
        .init();

    let active_connections = meter
        .i64_up_down_counter("http.server.active_connections")
        .with_description("Number of active HTTP connections")
        .init();

    let cpu_usage = meter
        .f64_gauge("process.runtime.cpu.utilization")
        .with_description("CPU utilization percentage")
        .with_unit("percent")
        .init();

    let memory_usage = meter
        .u64_gauge("process.runtime.memory.usage")
        .with_description("Memory usage in bytes")
        .with_unit("bytes")
        .init();

    let db_query_duration = meter
        .f64_histogram("db.client.query.duration")
        .with_description("Database query duration")
        .with_unit("ms")
        .init();

    let cache_hit_counter = meter
        .u64_counter("cache.hits")
        .with_description("Cache hit count")
        .init();

    let cache_miss_counter = meter
        .u64_counter("cache.misses")
        .with_description("Cache miss count")
        .init();

    active_connections.add(1, &[KeyValue::new("state", "active")]);

    let mut rng = rand::thread_rng();
    let mut request_num: u64 = 0;

    while !shutdown.load(Ordering::Relaxed) {
        request_num += 1;
        let (method, route) = ENDPOINTS[rng.gen_range(0..ENDPOINTS.len())];
        let is_error = rng.gen_bool(0.05);
        let status_code = if is_error {
            500i64
        } else if method == "POST" {
            201
        } else {
            200
        };

        let row_count = rng.gen_range(50usize..=200);
        let request_start = std::time::Instant::now();

        let root_span = tracer
            .span_builder(format!("{method} {route}"))
            .with_kind(SpanKind::Server)
            .with_attributes(vec![
                KeyValue::new("http.method", method),
                KeyValue::new("http.route", route),
                KeyValue::new("http.status_code", status_code),
                KeyValue::new("request.id", request_num as i64),
            ])
            .start(&tracer);

        let root_cx = Context::current_with_span(root_span);

        if is_error {
            error!(
                method,
                route, status_code, request_num, "Request failed with internal server error"
            );
        } else {
            info!(method, route, request_num, "Handling request");
        }

        request_counter.add(
            1,
            &[
                KeyValue::new("http.method", method),
                KeyValue::new("http.route", route),
                KeyValue::new("http.status_code", status_code),
            ],
        );

        // DB query span with real CPU work
        let db_start = std::time::Instant::now();
        {
            let _guard = root_cx.clone().attach();
            let db_span = tracer
                .span_builder("db-query")
                .with_kind(SpanKind::Client)
                .with_attributes(vec![
                    KeyValue::new("db.system", "postgresql"),
                    KeyValue::new("db.rows", row_count as i64),
                ])
                .start(&tracer);
            let _db_cx = Context::current_with_span(db_span);

            info!(
                db_system = "postgresql",
                rows = row_count,
                "Executing DB query"
            );

            let hashes = tokio::task::spawn_blocking(move || simulate_db_query(row_count))
                .await
                .unwrap_or_default();

            info!(rows_returned = hashes.len(), "DB query complete");
        }
        let db_elapsed = db_start.elapsed().as_secs_f64() * 1000.0;

        db_query_duration.record(
            db_elapsed,
            &[
                KeyValue::new("db.system", "postgresql"),
                KeyValue::new("db.operation", "SELECT"),
            ],
        );

        // Cache check span
        {
            let _guard = root_cx.clone().attach();
            let _cache_span = tracer
                .span_builder("cache-check")
                .with_kind(SpanKind::Client)
                .with_attributes(vec![KeyValue::new("cache.type", "redis")])
                .start(&tracer);

            let cache_hit = rng.gen_bool(0.75);
            if cache_hit {
                info!(cache_hit = true, "Cache hit");
                cache_hit_counter.add(1, &[KeyValue::new("cache.type", "redis")]);
            } else {
                warn!(cache_hit = false, "Cache miss");
                cache_miss_counter.add(1, &[KeyValue::new("cache.type", "redis")]);
            }
        }

        // Serialization span with real CPU work
        {
            let _guard = root_cx.clone().attach();
            let _ser_span = tracer
                .span_builder("serialize-response")
                .with_kind(SpanKind::Internal)
                .start(&tracer);

            let payload = tokio::task::spawn_blocking(simulate_serialization)
                .await
                .unwrap_or_default();

            info!(response_bytes = payload.len(), "Response serialized");
        }

        if is_error {
            error!(route, status_code, "Request completed with error");
        } else {
            info!(route, status_code, "Request completed");
        }

        drop(root_cx);

        let elapsed_ms = request_start.elapsed().as_secs_f64() * 1000.0;
        request_duration.record(
            elapsed_ms,
            &[
                KeyValue::new("http.method", method),
                KeyValue::new("http.route", route),
                KeyValue::new("http.status_code", status_code),
            ],
        );

        // Update resource metrics with some variation
        let cpu = 15.0 + rng.gen_range(0.0f64..30.0);
        let mem = 480_000_000u64 + rng.gen_range(0u64..100_000_000);
        cpu_usage.record(cpu, &[]);
        memory_usage.record(mem, &[]);

        let sleep_ms = rng.gen_range(200u64..=500);
        tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
    }

    active_connections.add(-1, &[KeyValue::new("state", "active")]);
    info!("Traffic loop shutting down");
}
