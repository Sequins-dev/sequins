//! Query command implementation

use anyhow::{Context, Result};
use futures::StreamExt;
use sequins_client::RemoteClient;
use sequins_datafusion_backend::DataFusionBackend;
use sequins_flight::{decode_metadata, SeqlMetadata};
use sequins_flight::{ipc_to_batch, SchemaFrame, WarningFrame};
use sequins_traits::QueryApi;

use crate::storage::open_local_storage;
use crate::OutputFormat;

pub async fn execute(
    query_str: String,
    target: String,
    format: OutputFormat,
    live: bool,
) -> Result<()> {
    let is_remote = target.starts_with("http://") || target.starts_with("https://");

    // Live queries emit the initial snapshot and then stream deltas indefinitely;
    // one-shot queries run to completion. Both remote (Flight SQL) and local
    // (in-process backend) targets support each mode.
    let mut stream = if is_remote {
        let client = RemoteClient::new(&target)
            .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", target, e))?;
        if live {
            client
                .query_live(&query_str)
                .await
                .context("Failed to open remote live query")?
        } else {
            client
                .query(&query_str)
                .await
                .context("Failed to execute remote query")?
        }
    } else {
        let storage = open_local_storage(&target).await?;
        let backend = DataFusionBackend::new(storage);
        if live {
            backend
                .query_live(&query_str)
                .await
                .context("Failed to open local live query")?
        } else {
            backend
                .query(&query_str)
                .await
                .context("Failed to execute local query")?
        }
    };

    if live {
        // A live stream is unbounded — render each frame as it arrives rather
        // than collecting the whole result first (a pretty JSON array can never
        // close, so `json` degrades to line-delimited JSON here).
        return stream_live(&mut stream, format).await;
    }

    match format {
        OutputFormat::Table => print_stream_as_table(&mut stream).await,
        OutputFormat::Json => {
            let rows = collect_data_rows(&mut stream).await?;
            println!("{}", serde_json::to_string_pretty(&rows)?);
            Ok(())
        }
        OutputFormat::Jsonl => {
            let rows = collect_data_rows(&mut stream).await?;
            for row in &rows {
                println!("{}", serde_json::to_string(row)?);
            }
            Ok(())
        }
    }
}

/// Render a live query stream continuously: the initial snapshot followed by
/// Append/Update/Expire/Replace deltas, flushing after every frame so the output
/// appears in real time (and pipes cleanly to `jq`, `grep`, etc.). Runs until the
/// server ends the stream or the process is interrupted (Ctrl-C).
async fn stream_live(stream: &mut sequins_traits::SeqlStream, format: OutputFormat) -> Result<()> {
    use std::io::Write;
    let jsonl = matches!(format, OutputFormat::Json | OutputFormat::Jsonl);

    // Emit each row of a delta batch as line-delimited JSON (used for both the
    // `json` and `jsonl` formats in live mode).
    let emit_rows = |batch: &arrow::record_batch::RecordBatch| -> Result<()> {
        for row in seql_substrait::reducer::batch_to_rows(batch) {
            println!("{}", serde_json::to_string(&serde_json::Value::Array(row))?);
        }
        Ok(())
    };

    while let Some(result) = stream.next().await {
        let fd = result.context("Live query stream error")?;
        let Some(metadata) = decode_metadata(&fd.app_metadata) else {
            continue;
        };
        match metadata {
            SeqlMetadata::Schema {
                shape,
                columns,
                watermark_ns,
                ..
            } if !jsonl => {
                print_schema(&SchemaFrame {
                    shape,
                    columns,
                    initial_watermark_ns: watermark_ns,
                });
            }
            SeqlMetadata::Data { .. } => {
                if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                    if jsonl {
                        emit_rows(&batch)?;
                    } else {
                        print_batch(&batch)?;
                    }
                }
            }
            SeqlMetadata::Append { .. } => {
                if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                    if jsonl {
                        emit_rows(&batch)?;
                    } else {
                        println!("\n＋ appended {} row(s)", batch.num_rows());
                        print_batch(&batch)?;
                    }
                }
            }
            SeqlMetadata::Update { row_id, .. } => {
                if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                    if jsonl {
                        emit_rows(&batch)?;
                    } else {
                        println!("\n~ updated row {row_id}");
                        print_batch(&batch)?;
                    }
                }
            }
            SeqlMetadata::Replace { .. } => {
                if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                    if jsonl {
                        emit_rows(&batch)?;
                    } else {
                        println!("\n= result replaced");
                        print_batch(&batch)?;
                    }
                }
            }
            SeqlMetadata::Expire { row_id, .. } if !jsonl => {
                println!("\n－ expired row {row_id}");
            }
            SeqlMetadata::Warning { code, message } => {
                print_warning(&warning_from_parts(code, message));
            }
            SeqlMetadata::Complete { .. } => {
                if !jsonl {
                    println!("\n✓ Live query ended");
                }
                break;
            }
            // Heartbeat / Expire-in-jsonl / Schema-in-jsonl carry nothing to render.
            _ => {}
        }
        std::io::stdout().flush().ok();
    }
    Ok(())
}

async fn print_stream_as_table(stream: &mut sequins_traits::SeqlStream) -> Result<()> {
    while let Some(result) = stream.next().await {
        let fd = result.context("Query stream error")?;
        let Some(metadata) = decode_metadata(&fd.app_metadata) else {
            continue;
        };
        match metadata {
            SeqlMetadata::Schema {
                shape,
                columns,
                watermark_ns,
                ..
            } => {
                print_schema(&SchemaFrame {
                    shape,
                    columns,
                    initial_watermark_ns: watermark_ns,
                });
            }
            SeqlMetadata::Data { .. } => {
                if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                    print_batch(&batch)?;
                }
            }
            SeqlMetadata::Warning { code, message } => {
                print_warning(&warning_from_parts(code, message));
            }
            SeqlMetadata::Complete { .. } => {
                println!("\n✓ Query complete");
            }
            SeqlMetadata::Heartbeat { .. } => {}
            _ => {
                println!("\n⚠ Delta frames not yet supported in table output");
            }
        }
    }
    Ok(())
}

/// For JSON output — collect all data rows as serde_json::Value objects.
async fn collect_data_rows(
    stream: &mut sequins_traits::SeqlStream,
) -> Result<Vec<serde_json::Value>> {
    let mut rows = Vec::new();
    while let Some(result) = stream.next().await {
        let fd = result.context("Query stream error")?;
        let Some(metadata) = decode_metadata(&fd.app_metadata) else {
            continue;
        };
        if matches!(metadata, SeqlMetadata::Data { .. }) {
            if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                rows.extend(
                    seql_substrait::reducer::batch_to_rows(&batch)
                        .into_iter()
                        .map(serde_json::Value::Array),
                );
            }
        }
    }
    Ok(rows)
}

fn warning_from_parts(code: u32, message: String) -> WarningFrame {
    use sequins_traits::WarningCode;
    let wc = match code {
        1 => WarningCode::SlowQuery,
        2 => WarningCode::ApproximateResult,
        3 => WarningCode::SchemaResolutionFallback,
        _ => WarningCode::ResultTruncated,
    };
    WarningFrame { code: wc, message }
}

fn print_schema(schema: &SchemaFrame) {
    println!("\n=== Schema ===");
    println!("Shape: {:?}", schema.shape);
    println!("Columns:");
    for col in &schema.columns {
        println!(
            "  {} ({:?}) - role: {:?}",
            col.name, col.data_type, col.role
        );
    }
}

fn print_batch(batch: &arrow::record_batch::RecordBatch) -> Result<()> {
    let row_count = batch.num_rows();
    println!("\n=== Data ({} rows) ===", row_count);

    if row_count == 0 {
        return Ok(());
    }

    let schema = batch.schema();
    let col_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    println!("{}", col_names.join(" | "));
    println!("{}", "-".repeat(col_names.len() * 20));

    let rows = seql_substrait::reducer::batch_to_rows(batch);
    for row in &rows {
        let values: Vec<String> = row
            .iter()
            .map(|v| match v {
                serde_json::Value::Null => "NULL".to_string(),
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        if f.fract() == 0.0 && n.as_i64().is_some() {
                            n.to_string()
                        } else {
                            format!("{:.2}", f)
                        }
                    } else {
                        n.to_string()
                    }
                }
                other => other.to_string().trim_matches('"').to_string(),
            })
            .collect();
        println!("{}", values.join(" | "));
    }

    Ok(())
}

fn print_warning(warning: &WarningFrame) {
    eprintln!("\n⚠️  Warning: {}", warning.message);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use seql_ast::schema::{ColumnDef, ColumnRole, DataType, ResponseShape};
    use sequins_traits::WarningCode;
    use std::sync::Arc;

    fn make_string_batch(values: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            ArrowDataType::Utf8,
            true,
        )]));
        let array = Arc::new(StringArray::from(values));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    fn make_int64_batch(values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            ArrowDataType::Int64,
            true,
        )]));
        let array = Arc::new(Int64Array::from(values));
        RecordBatch::try_new(schema, vec![array]).unwrap()
    }

    #[test]
    fn test_print_batch_empty() {
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::new_empty(schema);
        let result = print_batch(&batch);
        assert!(
            result.is_ok(),
            "Expected print_batch to succeed with empty batch"
        );
    }

    #[test]
    fn test_print_batch_strings() {
        let result = print_batch(&make_string_batch(vec!["hello", "world"]));
        assert!(result.is_ok());
    }

    #[test]
    fn test_print_batch_int64s() {
        let result = print_batch(&make_int64_batch(vec![42, -100, 0]));
        assert!(result.is_ok());
    }

    #[test]
    fn test_print_schema() {
        let schema = SchemaFrame {
            shape: ResponseShape::Table,
            columns: vec![
                ColumnDef {
                    name: "trace_id".to_string(),
                    data_type: DataType::String,
                    role: ColumnRole::Field,
                },
                ColumnDef {
                    name: "duration".to_string(),
                    data_type: DataType::Duration,
                    role: ColumnRole::Aggregation,
                },
            ],
            initial_watermark_ns: 1_700_000_000_000_000_000,
        };
        // Just verify it doesn't panic
        print_schema(&schema);
    }

    #[test]
    fn test_print_warning() {
        let warning = WarningFrame {
            code: WarningCode::ResultTruncated,
            message: "Test warning message".to_string(),
        };
        // Just verify it doesn't panic
        print_warning(&warning);
    }

    #[test]
    fn test_flight_data_schema_dispatches() {
        use seql_ast::schema::ResponseShape;
        use sequins_flight::schema_flight_data;
        let schema_ref = arrow::datatypes::SchemaRef::new(arrow::datatypes::Schema::empty());
        let fd = schema_flight_data(None, schema_ref, ResponseShape::Table, vec![], 0);
        let metadata = decode_metadata(&fd.app_metadata);
        assert!(matches!(metadata, Some(SeqlMetadata::Schema { .. })));
    }

    #[test]
    fn test_flight_data_complete_dispatches() {
        use sequins_flight::complete_flight_data;
        use sequins_flight::QueryStats;
        let fd = complete_flight_data(QueryStats::zero());
        let metadata = decode_metadata(&fd.app_metadata);
        assert!(matches!(metadata, Some(SeqlMetadata::Complete { .. })));
    }
}
