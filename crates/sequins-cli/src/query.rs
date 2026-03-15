//! Query command implementation

use anyhow::{Context, Result};
use futures::StreamExt;
use sequins_client::RemoteClient;
use sequins_query::flight::{decode_metadata, SeqlMetadata};
use sequins_query::frame::{ipc_to_batch, SchemaFrame, WarningFrame};
use sequins_query::QueryApi;
use sequins_storage::config::ColdTierConfig;
use sequins_storage::{DataFusionBackend, Storage, StorageConfig};
use std::sync::Arc;

use crate::OutputFormat;

pub async fn execute(query_str: String, target: String, format: OutputFormat) -> Result<()> {
    let is_remote = target.starts_with("http://") || target.starts_with("https://");

    let mut stream = if is_remote {
        let client = RemoteClient::new(&target)
            .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", target, e))?;
        client
            .query(&query_str)
            .await
            .context("Failed to execute remote query")?
    } else {
        let config = StorageConfig {
            cold_tier: ColdTierConfig {
                uri: target.clone(),
                ..Default::default()
            },
            ..Default::default()
        };
        let storage = Arc::new(
            Storage::new(config)
                .await
                .context("Failed to open database")?,
        );
        let backend = DataFusionBackend::new(storage);
        backend
            .query(&query_str)
            .await
            .context("Failed to execute local query")?
    };

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

async fn print_stream_as_table(stream: &mut sequins_query::SeqlStream) -> Result<()> {
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
    stream: &mut sequins_query::SeqlStream,
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
                    sequins_query::reducer::batch_to_rows(&batch)
                        .into_iter()
                        .map(serde_json::Value::Array),
                );
            }
        }
    }
    Ok(rows)
}

fn warning_from_parts(code: u32, message: String) -> WarningFrame {
    use sequins_query::error::WarningCode;
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

    let rows = sequins_query::reducer::batch_to_rows(batch);
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
    use sequins_query::error::WarningCode;
    use sequins_query::schema::{ColumnDef, ColumnRole, DataType, ResponseShape};
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
        use sequins_query::flight::schema_flight_data;
        use sequins_query::schema::ResponseShape;
        let schema_ref = arrow::datatypes::SchemaRef::new(arrow::datatypes::Schema::empty());
        let fd = schema_flight_data(None, schema_ref, ResponseShape::Table, vec![], 0);
        let metadata = decode_metadata(&fd.app_metadata);
        assert!(matches!(metadata, Some(SeqlMetadata::Schema { .. })));
    }

    #[test]
    fn test_flight_data_complete_dispatches() {
        use sequins_query::flight::complete_flight_data;
        use sequins_query::frame::QueryStats;
        let fd = complete_flight_data(QueryStats::zero());
        let metadata = decode_metadata(&fd.app_metadata);
        assert!(matches!(metadata, Some(SeqlMetadata::Complete { .. })));
    }
}
