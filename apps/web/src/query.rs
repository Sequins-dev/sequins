use arrow::record_batch::RecordBatch;
use futures::StreamExt;
use sequins_query::{frame::ipc_to_batch, reducer::batch_to_rows, QueryError};
use serde_json::{Map, Value};

/// Convert an Arrow RecordBatch into a vec of JSON objects keyed by column name.
pub fn batch_to_objects(batch: &RecordBatch) -> Vec<Map<String, Value>> {
    let schema = batch.schema();
    let rows = batch_to_rows(batch);
    rows.into_iter()
        .map(|row| {
            let mut obj = Map::new();
            for (i, field) in schema.fields().iter().enumerate() {
                obj.insert(
                    field.name().clone(),
                    row.get(i).cloned().unwrap_or(Value::Null),
                );
            }
            obj
        })
        .collect()
}

/// Decode Arrow IPC bytes into a RecordBatch, then into JSON objects.
pub fn ipc_to_objects(ipc: &[u8]) -> Result<Vec<Map<String, Value>>, QueryError> {
    let batch = ipc_to_batch(ipc).map_err(|e| QueryError::Execution {
        message: format!("IPC decode error: {e}"),
    })?;
    Ok(batch_to_objects(&batch))
}

/// Run a snapshot SeQL query and collect all rows as JSON objects.
pub async fn snapshot_objects(
    backend: &sequins_client::RemoteClient,
    seql: &str,
) -> Result<Vec<Map<String, Value>>, QueryError> {
    use sequins_query::QueryApi;
    let mut stream = backend.query(seql).await?;
    let mut all_objects = Vec::new();
    while let Some(item) = stream.next().await {
        let fd = item?;
        if fd.data_body.is_empty() {
            continue;
        }
        // Parse the metadata to see if this is a data frame
        if let Some(meta) = sequins_query::flight::decode_metadata(&fd.app_metadata) {
            use sequins_query::flight::SeqlMetadata;
            match meta {
                SeqlMetadata::Data { .. }
                | SeqlMetadata::Append { .. }
                | SeqlMetadata::Replace { .. } => {
                    if let Ok(batch) = ipc_to_batch(&fd.data_body) {
                        all_objects.extend(batch_to_objects(&batch));
                    }
                }
                SeqlMetadata::Complete { .. } => break,
                _ => {}
            }
        }
    }
    Ok(all_objects)
}
