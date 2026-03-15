//! Ingest command implementation

use anyhow::{Context, Result};
use opentelemetry_proto::tonic::collector::{
    logs::v1::{ExportLogsServiceRequest, ExportLogsServiceResponse},
    metrics::v1::{ExportMetricsServiceRequest, ExportMetricsServiceResponse},
    profiles::v1development::{ExportProfilesServiceRequest, ExportProfilesServiceResponse},
    trace::v1::{ExportTraceServiceRequest, ExportTraceServiceResponse},
};
use prost::Message;
use sequins_storage::config::ColdTierConfig;
use sequins_storage::{Storage, StorageConfig};
use sequins_types::models::Duration;
use sequins_types::OtlpIngest;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use crate::{IngestFormat, SignalType};

pub async fn execute(
    file: PathBuf,
    signal: Option<SignalType>,
    format: Option<IngestFormat>,
    target: String,
) -> Result<()> {
    // Determine if target is a local path or remote URL
    let is_remote = target.starts_with("http://") || target.starts_with("https://");

    if is_remote {
        anyhow::bail!(
            "Remote OTLP ingestion not yet implemented - need to send HTTP POST to OTLP endpoint"
        );
    }

    // Read file contents
    let bytes =
        fs::read(&file).with_context(|| format!("Failed to read file: {}", file.display()))?;

    if bytes.is_empty() {
        anyhow::bail!("File is empty: {}", file.display());
    }

    // Auto-detect format if not specified
    let format = match format {
        Some(f) => f,
        None => detect_format(&file, &bytes)?,
    };

    // Auto-detect signal type if not specified
    let signal = match signal {
        Some(s) => s,
        None => {
            anyhow::bail!(
                "Signal type not specified. Please specify --signal (traces|logs|metrics|profiles)"
            );
        }
    };

    // Local ingestion
    let mut config = StorageConfig {
        cold_tier: ColdTierConfig {
            uri: target.clone(),
            ..Default::default()
        },
        ..Default::default()
    };
    // Set flush_interval to 0 so run_maintenance_internal() will flush all data immediately
    config.lifecycle.flush_interval = Duration::from_secs(0);
    let storage = Arc::new(
        Storage::new(config)
            .await
            .context("Failed to open database")?,
    );

    // Decode and ingest based on signal type
    match signal {
        SignalType::Traces => {
            let request = decode_traces(&bytes, &format)?;
            let response = storage.ingest_traces(request).await?;
            println!("✅ Ingested traces to {}", target);
            print_trace_response(&response);
        }
        SignalType::Logs => {
            let request = decode_logs(&bytes, &format)?;
            let response = storage.ingest_logs(request).await?;
            println!("✅ Ingested logs to {}", target);
            print_log_response(&response);
        }
        SignalType::Metrics => {
            let request = decode_metrics(&bytes, &format)?;
            let response = storage.ingest_metrics(request).await?;
            println!("✅ Ingested metrics to {}", target);
            print_metrics_response(&response);
        }
        SignalType::Profiles => {
            let request = decode_profiles(&bytes, &format)?;
            let response = storage.ingest_profiles(request).await?;
            println!("✅ Ingested profiles to {}", target);
            print_profiles_response(&response);
        }
    }

    // Add a small delay to ensure timestamps are in the past
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Flush data from hot tier to cold tier before exiting
    println!("💾 Flushing data to disk...");
    let stats = storage.run_maintenance_internal().await?;
    println!(
        "✅ Flushed {} batches, evicted {} entries",
        stats.batches_flushed, stats.entries_evicted
    );

    Ok(())
}

fn detect_format(file: &Path, bytes: &[u8]) -> Result<IngestFormat> {
    // Try file extension first
    if let Some(ext) = file.extension().and_then(|e| e.to_str()) {
        match ext.to_lowercase().as_str() {
            "pb" | "protobuf" | "bin" => return Ok(IngestFormat::Protobuf),
            "json" => return Ok(IngestFormat::Json),
            _ => {}
        }
    }

    // Try parsing as protobuf - if it starts with valid protobuf field tags
    // Protobuf messages typically start with field number + wire type
    if !bytes.is_empty() && (bytes[0] & 0x07) <= 5 {
        return Ok(IngestFormat::Protobuf);
    }

    // Try parsing as JSON - look for opening brace/bracket
    let trimmed = bytes.iter().take(10).find(|&&b| !b.is_ascii_whitespace());
    if let Some(&first_char) = trimmed {
        if first_char == b'{' || first_char == b'[' {
            return Ok(IngestFormat::Json);
        }
    }

    anyhow::bail!("Could not auto-detect format. Please specify --format (protobuf|json)");
}

fn decode_traces(bytes: &[u8], format: &IngestFormat) -> Result<ExportTraceServiceRequest> {
    match format {
        IngestFormat::Protobuf => {
            ExportTraceServiceRequest::decode(bytes).context("Failed to decode protobuf trace data")
        }
        IngestFormat::Json => {
            serde_json::from_slice(bytes).context("Failed to decode JSON trace data")
        }
    }
}

fn decode_logs(bytes: &[u8], format: &IngestFormat) -> Result<ExportLogsServiceRequest> {
    match format {
        IngestFormat::Protobuf => {
            ExportLogsServiceRequest::decode(bytes).context("Failed to decode protobuf log data")
        }
        IngestFormat::Json => {
            serde_json::from_slice(bytes).context("Failed to decode JSON log data")
        }
    }
}

fn decode_metrics(bytes: &[u8], format: &IngestFormat) -> Result<ExportMetricsServiceRequest> {
    match format {
        IngestFormat::Protobuf => ExportMetricsServiceRequest::decode(bytes)
            .context("Failed to decode protobuf metrics data"),
        IngestFormat::Json => {
            serde_json::from_slice(bytes).context("Failed to decode JSON metrics data")
        }
    }
}

fn decode_profiles(bytes: &[u8], format: &IngestFormat) -> Result<ExportProfilesServiceRequest> {
    match format {
        IngestFormat::Protobuf => ExportProfilesServiceRequest::decode(bytes)
            .context("Failed to decode protobuf profile data"),
        IngestFormat::Json => {
            serde_json::from_slice(bytes).context("Failed to decode JSON profile data")
        }
    }
}

fn print_trace_response(response: &ExportTraceServiceResponse) {
    if let Some(partial_success) = &response.partial_success {
        let accepted = partial_success.rejected_spans;
        println!("  Rejected spans: {}", accepted);
        if !partial_success.error_message.is_empty() {
            println!("  Error: {}", partial_success.error_message);
        }
    }
}

fn print_log_response(response: &ExportLogsServiceResponse) {
    if let Some(partial_success) = &response.partial_success {
        let accepted = partial_success.rejected_log_records;
        println!("  Rejected log records: {}", accepted);
        if !partial_success.error_message.is_empty() {
            println!("  Error: {}", partial_success.error_message);
        }
    }
}

fn print_metrics_response(response: &ExportMetricsServiceResponse) {
    if let Some(partial_success) = &response.partial_success {
        let accepted = partial_success.rejected_data_points;
        println!("  Rejected data points: {}", accepted);
        if !partial_success.error_message.is_empty() {
            println!("  Error: {}", partial_success.error_message);
        }
    }
}

fn print_profiles_response(response: &ExportProfilesServiceResponse) {
    if let Some(partial_success) = &response.partial_success {
        let accepted = partial_success.rejected_profiles;
        println!("  Rejected profiles: {}", accepted);
        if !partial_success.error_message.is_empty() {
            println!("  Error: {}", partial_success.error_message);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_traces_json() {
        let json_data = r#"{
            "resourceSpans": [{
                "resource": {"attributes": []},
                "scopeSpans": [{
                    "spans": [{
                        "traceId": "00000000000000000000000000000001",
                        "spanId": "0000000000000001",
                        "name": "test-span",
                        "startTimeUnixNano": "1000000000",
                        "endTimeUnixNano": "2000000000"
                    }]
                }]
            }]
        }"#;

        let result = decode_traces(json_data.as_bytes(), &IngestFormat::Json);
        assert!(
            result.is_ok(),
            "Expected traces JSON to decode successfully"
        );
        let request = result.unwrap();
        assert_eq!(request.resource_spans.len(), 1, "Expected 1 resource span");
    }

    #[test]
    fn test_decode_logs_json() {
        let json_data = r#"{
            "resourceLogs": [{
                "resource": {"attributes": []},
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": "1000000000",
                        "severityNumber": 9,
                        "severityText": "INFO",
                        "body": {"stringValue": "test log message"}
                    }]
                }]
            }]
        }"#;

        let result = decode_logs(json_data.as_bytes(), &IngestFormat::Json);
        assert!(result.is_ok(), "Expected logs JSON to decode successfully");
        let request = result.unwrap();
        assert_eq!(request.resource_logs.len(), 1, "Expected 1 resource log");
    }

    #[test]
    fn test_decode_metrics_json() {
        let json_data = r#"{
            "resourceMetrics": [{
                "resource": {"attributes": []},
                "scopeMetrics": [{
                    "metrics": [{
                        "name": "test_metric",
                        "unit": "1",
                        "gauge": {
                            "dataPoints": [{
                                "timeUnixNano": "1000000000",
                                "asInt": "42"
                            }]
                        }
                    }]
                }]
            }]
        }"#;

        let result = decode_metrics(json_data.as_bytes(), &IngestFormat::Json);
        assert!(
            result.is_ok(),
            "Expected metrics JSON to decode successfully"
        );
        let request = result.unwrap();
        assert_eq!(
            request.resource_metrics.len(),
            1,
            "Expected 1 resource metric"
        );
    }

    #[test]
    fn test_decode_profiles_protobuf() {
        // Test that decode_profiles function exists and handles protobuf format
        // Using empty protobuf message (will decode to empty but valid structure)
        let empty_protobuf = b"";
        let result = decode_profiles(empty_protobuf, &IngestFormat::Protobuf);
        // Empty protobuf should decode successfully to an empty request
        assert!(
            result.is_ok(),
            "Expected empty protobuf to decode successfully"
        );
    }

    #[test]
    fn test_decode_invalid_json() {
        let invalid_json = b"not valid json";

        let result = decode_traces(invalid_json, &IngestFormat::Json);
        assert!(result.is_err(), "Expected error when decoding invalid JSON");

        let result = decode_logs(invalid_json, &IngestFormat::Json);
        assert!(result.is_err(), "Expected error when decoding invalid JSON");

        let result = decode_metrics(invalid_json, &IngestFormat::Json);
        assert!(result.is_err(), "Expected error when decoding invalid JSON");
    }

    #[test]
    fn test_detect_format_from_json_extension() {
        let path = Path::new("traces.json");
        let bytes = b"{}";
        let result = detect_format(path, bytes);
        assert!(result.is_ok(), "Expected format detection to succeed");
        assert!(matches!(result.unwrap(), IngestFormat::Json));
    }

    #[test]
    fn test_detect_format_from_protobuf_extension() {
        let path = Path::new("traces.pb");
        let bytes = b"\x0a\x00"; // Valid protobuf field tag
        let result = detect_format(path, bytes);
        assert!(result.is_ok(), "Expected format detection to succeed");
        assert!(matches!(result.unwrap(), IngestFormat::Protobuf));
    }

    #[test]
    fn test_detect_format_prioritizes_extension_over_content() {
        // When extension is present, it takes priority
        let path = Path::new("data.json");
        let bytes = b"\x0a\x00"; // Binary protobuf-like data
        let result = detect_format(path, bytes);
        assert!(result.is_ok(), "Expected format detection to succeed");
        assert!(matches!(result.unwrap(), IngestFormat::Json));
    }

    #[test]
    fn test_detect_format_protobuf_fallback() {
        // Without a recognized extension, protobuf is detected for binary-looking data
        let path = Path::new("unknown_file");
        let bytes = b"\x0a\x10\x08\x01\x12\x04test";
        let result = detect_format(path, bytes);
        assert!(result.is_ok(), "Expected format detection to succeed");
        assert!(matches!(result.unwrap(), IngestFormat::Protobuf));
    }
}
