# OTLP Implementation

[← Back to Index](INDEX.md)

**Related Documentation:** [data-models.md](data-models.md) | [database.md](database.md) | [workspace-and-crates.md](workspace-and-crates.md)

---

## Endpoint Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   OTLP Endpoints                            │
│                                                             │
│  ┌───────────────┐  ┌────────────────┐  ┌───────────────┐ │
│  │ OTLP/gRPC    │  │ OTLP/HTTP      │  │ OTLP/HTTP     │ │
│  │ (Port 4317)   │  │ Protobuf       │  │ +JSON         │ │
│  │               │  │ (Port 4318)    │  │ (Port 4318)   │ │
│  └───────┬───────┘  └────────┬───────┘  └───────┬───────┘ │
│          │                   │                   │         │
│          └───────────────────┴───────────────────┘         │
│                              │                             │
└──────────────────────────────┼─────────────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  Ingestion Pipeline │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  TieredStorage      │
                    │  (hot tier + flush) │
                    └─────────────────────┘
```

## gRPC Service Implementation

```rust
use tonic::{transport::Server, Request, Response, Status};
use opentelemetry_proto::tonic::collector::trace::v1::{
    trace_service_server::{TraceService, TraceServiceServer},
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};

pub struct OtlpTraceService {
    ingester: Arc<IngestionPipeline>,
}

impl TraceService for OtlpTraceService {
    async fn export(
        &self,
        request: Request<ExportTraceServiceRequest>,
    ) -> Result<Response<ExportTraceServiceResponse>, Status> {
        let traces = request.into_inner();

        self.ingester
            .ingest_traces(traces)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ExportTraceServiceResponse {
            partial_success: None,
        }))
    }
}

pub async fn start_grpc_server(port: u16, ingester: Arc<IngestionPipeline>) -> Result<()> {
    let addr = format!("127.0.0.1:{}", port).parse()?;
    let service = OtlpTraceService { ingester };

    Server::builder()
        .add_service(TraceServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
```

## HTTP Service Implementation

```rust
use axum::{
    extract::State,
    http::StatusCode,
    routing::post,
    Router,
};

pub async fn start_http_server(port: u16, ingester: Arc<IngestionPipeline>) -> Result<()> {
    let app = Router::new()
        .route("/v1/traces", post(handle_traces))
        .route("/v1/logs", post(handle_logs))
        .route("/v1/metrics", post(handle_metrics))
        .with_state(ingester);

    let addr = format!("127.0.0.1:{}", port).parse()?;
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await?;

    Ok(())
}

async fn handle_traces(
    State(ingester): State<Arc<IngestionPipeline>>,
    body: bytes::Bytes,
) -> Result<StatusCode, StatusCode> {
    // Try protobuf first, fall back to JSON
    let result = parse_protobuf_traces(&body)
        .or_else(|_| parse_json_traces(&body));

    match result {
        Ok(traces) => {
            ingester.ingest_traces(traces).await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            Ok(StatusCode::OK)
        }
        Err(_) => Err(StatusCode::BAD_REQUEST),
    }
}
```

## Ingestion Pipeline

```rust
pub struct IngestionPipeline {
    db: Arc<Mutex<Connection>>,
    tx: mpsc::Sender<IngestionEvent>,
}

pub enum IngestionEvent {
    Traces(Vec<Trace>),
    Logs(Vec<LogEntry>),
    Metrics(Vec<MetricDataPoint>),
    Profiles(Vec<Profile>),
}

impl IngestionPipeline {
    pub async fn ingest_traces(&self, traces: ExportTraceServiceRequest) -> Result<()> {
        // 1. Parse protobuf into our internal models
        let parsed_traces = self.parse_traces(traces)?;

        // 2. Extract services
        let services = self.extract_services(&parsed_traces);

        // 3. Enrich data (calculate durations, build relationships)
        let enriched = self.enrich_traces(parsed_traces)?;

        // 4. Send to background worker for storage
        self.tx.send(IngestionEvent::Traces(enriched)).await?;

        Ok(())
    }

    async fn storage_worker(mut rx: mpsc::Receiver<IngestionEvent>, db: Arc<Mutex<Connection>>) {
        while let Some(event) = rx.recv().await {
            match event {
                IngestionEvent::Traces(traces) => {
                    if let Err(e) = Self::store_traces(&db, traces) {
                        eprintln!("Failed to store traces: {}", e);
                    }
                }
                // Handle other event types...
            }
        }
    }
}
```

## Protocol Support

### OTLP/gRPC (Port 4317)

**Advantages:**
- Standard OTLP protocol
- Binary protobuf encoding (efficient)
- HTTP/2 multiplexing
- Streaming support
- Best performance

**Usage:**
```rust
// Configure OpenTelemetry SDK
let tracer = opentelemetry_otlp::new_pipeline()
    .tracing()
    .with_exporter(
        opentelemetry_otlp::new_exporter()
            .tonic()
            .with_endpoint("http://localhost:4317")
    )
    .install_batch(opentelemetry::runtime::Tokio)?;
```

### OTLP/HTTP with Protobuf (Port 4318)

**Advantages:**
- HTTP/1.1 or HTTP/2
- Protobuf encoding
- Firewall-friendly
- No gRPC dependencies

**Usage:**
```rust
let tracer = opentelemetry_otlp::new_pipeline()
    .tracing()
    .with_exporter(
        opentelemetry_otlp::new_exporter()
            .http()
            .with_endpoint("http://localhost:4318/v1/traces")
    )
    .install_batch(opentelemetry::runtime::Tokio)?;
```

### OTLP/HTTP with JSON (Port 4318)

**Advantages:**
- Human-readable
- Easy debugging
- No protobuf dependencies
- Browser-compatible

**Usage:**
```bash
curl -X POST http://localhost:4318/v1/traces \
  -H "Content-Type: application/json" \
  -d '{
    "resourceSpans": [...]
  }'
```

## Parsing and Enrichment

### Trace Parsing

```rust
impl IngestionPipeline {
    fn parse_traces(&self, request: ExportTraceServiceRequest) -> Result<Vec<Trace>> {
        let mut traces = Vec::new();

        for resource_span in request.resource_spans {
            let resource_attrs = extract_attributes(&resource_span.resource);

            for scope_span in resource_span.scope_spans {
                for span in scope_span.spans {
                    let trace = self.parse_span(span, &resource_attrs)?;
                    traces.push(trace);
                }
            }
        }

        Ok(traces)
    }

    fn parse_span(&self, span: Span, resource_attrs: &HashMap<String, String>) -> Result<Trace> {
        // Extract trace and span IDs
        let trace_id = TraceId::from_bytes(span.trace_id);
        let span_id = SpanId::from_bytes(span.span_id);

        // Extract service name from resource attributes
        let service_name = resource_attrs
            .get("service.name")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        // Parse timestamps
        let start_time = span.start_time_unix_nano as i64;
        let end_time = span.end_time_unix_nano as i64;
        let duration_ns = end_time - start_time;

        // Parse status
        let status = match span.status.code {
            0 => SpanStatus::Unset,
            1 => SpanStatus::Ok,
            2 => SpanStatus::Error,
            _ => SpanStatus::Unset,
        };

        Ok(Trace {
            trace_id,
            span_id,
            service_name,
            start_time,
            end_time,
            duration_ns,
            status,
            // ... parse other fields
        })
    }

    fn enrich_traces(&self, traces: Vec<Trace>) -> Result<Vec<Trace>> {
        // Group spans by trace_id
        let mut trace_map: HashMap<TraceId, Vec<Span>> = HashMap::new();

        for span in traces {
            trace_map.entry(span.trace_id).or_default().push(span);
        }

        // Calculate trace-level metrics
        let enriched_traces = trace_map
            .into_iter()
            .map(|(trace_id, spans)| {
                let root_span = find_root_span(&spans);
                let span_count = spans.len();
                let start_time = spans.iter().map(|s| s.start_time).min().unwrap();
                let end_time = spans.iter().map(|s| s.end_time).max().unwrap();
                let duration_ns = end_time - start_time;

                Trace {
                    trace_id,
                    root_span_id: root_span.span_id,
                    service_name: root_span.service_name,
                    start_time,
                    end_time,
                    duration_ns,
                    span_count,
                    status: calculate_trace_status(&spans),
                }
            })
            .collect();

        Ok(enriched_traces)
    }
}
```

## Service Discovery

```rust
impl IngestionPipeline {
    fn extract_services(&self, traces: &[Trace]) -> Vec<Service> {
        let mut services: HashMap<String, Service> = HashMap::new();

        for trace in traces {
            let service_name = &trace.service_name;

            services.entry(service_name.clone()).or_insert_with(|| Service {
                id: Uuid::new_v4().to_string(),
                name: service_name.clone(),
                instance_id: trace.resource_attrs.get("service.instance.id")
                    .cloned()
                    .unwrap_or_else(|| service_name.clone()),
                first_seen: trace.start_time,
                last_seen: trace.end_time,
                attributes: trace.resource_attrs.clone(),
            });
        }

        services.into_values().collect()
    }
}
```

## Error Handling

### Partial Success

OTLP supports partial success responses:

```rust
impl TraceService for OtlpTraceService {
    async fn export(&self, request: Request<ExportTraceServiceRequest>)
        -> Result<Response<ExportTraceServiceResponse>, Status>
    {
        let traces = request.into_inner();

        match self.ingester.ingest_traces(traces).await {
            Ok(_) => Ok(Response::new(ExportTraceServiceResponse {
                partial_success: None,
            })),
            Err(e) if e.is_recoverable() => {
                // Return partial success for recoverable errors
                Ok(Response::new(ExportTraceServiceResponse {
                    partial_success: Some(PartialSuccess {
                        rejected_spans: e.rejected_count(),
                        error_message: e.to_string(),
                    }),
                }))
            }
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}
```

### Retry Logic

Clients should retry on:
- Network errors
- 5xx server errors
- Timeout errors

Clients should NOT retry on:
- 4xx client errors (bad request, invalid data)
- Authentication failures

---

**Last Updated:** 2025-11-05
