//! Flight protocol helpers for Sequins SeQL queries
//!
//! All query results are delivered as Arrow Flight `FlightData` streams.
//! Each message carries `app_metadata: Bytes` containing a bincode-serialized
//! `SeqlMetadata` that tells the consumer what the message means.
//!
//! Multiple flat tables are multiplexed in a single stream using the `table`
//! field on each variant:
//! - `table: None`      → primary result table
//! - `table: Some("l")` → auxiliary table from `<- logs as l`

use crate::frame::{batch_to_ipc, QueryStats};
use crate::schema::{ColumnDef, ResponseShape};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_flight::FlightData;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

// ── SeqlMetadata ──────────────────────────────────────────────────────────────

/// Per-message metadata carried in `FlightData::app_metadata`.
///
/// Multiplexes multiple flat result tables in a single stream. For data-carrying
/// variants, `table: None` is the primary table; `table: Some("alias")` is an
/// auxiliary table from a merge stage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SeqlMetadata {
    /// Schema announcement — one per table, always before any Data frames for that table.
    Schema {
        /// Which table this schema describes (None = primary)
        table: Option<String>,
        /// Rendering hint
        shape: ResponseShape,
        /// Column definitions in result order
        columns: Vec<ColumnDef>,
        /// WAL watermark at query start (nanoseconds)
        watermark_ns: u64,
    },
    /// A batch of snapshot rows
    Data {
        /// Which table this batch belongs to
        table: Option<String>,
    },
    /// Live: new rows appended
    Append {
        table: Option<String>,
        /// Row ID of the first row in the appended batch
        start_row_id: u64,
        /// Current WAL watermark
        watermark_ns: u64,
    },
    /// Live: an existing row was partially updated (carries single-row batch)
    Update {
        table: Option<String>,
        /// Row being updated
        row_id: u64,
        /// Current WAL watermark
        watermark_ns: u64,
    },
    /// Live: a row expired from the time window (no IPC body)
    Expire {
        table: Option<String>,
        /// Row that expired
        row_id: u64,
        /// Current WAL watermark
        watermark_ns: u64,
    },
    /// Live: full result set replaced (carries the new complete result)
    Replace {
        table: Option<String>,
        /// Current WAL watermark
        watermark_ns: u64,
    },
    /// Keepalive for long-running live queries (no IPC body)
    Heartbeat {
        /// Current WAL watermark
        watermark_ns: u64,
    },
    /// Query finished successfully (no IPC body)
    Complete { stats: QueryStats },
    /// Non-fatal warning (no IPC body)
    Warning { code: u32, message: String },
}

/// Encode `SeqlMetadata` to bytes for `FlightData::app_metadata`.
pub fn encode_metadata(meta: &SeqlMetadata) -> Bytes {
    Bytes::from(bincode::serialize(meta).expect("SeqlMetadata serialization never fails"))
}

/// Decode `SeqlMetadata` from `FlightData::app_metadata`.
///
/// Returns `None` if the bytes are empty or cannot be decoded (e.g. messages
/// from non-Sequins Flight servers have no app_metadata).
pub fn decode_metadata(bytes: &Bytes) -> Option<SeqlMetadata> {
    if bytes.is_empty() {
        return None;
    }
    bincode::deserialize(bytes).ok()
}

// ── FlightData builders ───────────────────────────────────────────────────────

/// Build a Schema `FlightData` for a table.
///
/// The IPC body is empty (schema-only messages carry no rows).
pub fn schema_flight_data(
    table: Option<&str>,
    _schema: SchemaRef,
    shape: ResponseShape,
    columns: Vec<ColumnDef>,
    watermark_ns: u64,
) -> FlightData {
    let meta = SeqlMetadata::Schema {
        table: table.map(str::to_string),
        shape,
        columns,
        watermark_ns,
    };
    FlightData {
        data_header: Bytes::new(),
        data_body: Bytes::new(),
        app_metadata: encode_metadata(&meta),
        flight_descriptor: None,
    }
}

/// Build a Data `FlightData` carrying a `RecordBatch` as IPC bytes.
pub fn data_flight_data(table: Option<&str>, batch: &RecordBatch) -> FlightData {
    let meta = SeqlMetadata::Data {
        table: table.map(str::to_string),
    };
    FlightData {
        data_header: Bytes::new(),
        data_body: Bytes::from(batch_to_ipc(batch)),
        app_metadata: encode_metadata(&meta),
        flight_descriptor: None,
    }
}

/// Build an Append `FlightData` for live queries.
pub fn append_flight_data(
    table: Option<&str>,
    batch: &RecordBatch,
    start_row_id: u64,
    watermark_ns: u64,
) -> FlightData {
    let meta = SeqlMetadata::Append {
        table: table.map(str::to_string),
        start_row_id,
        watermark_ns,
    };
    FlightData {
        data_header: Bytes::new(),
        data_body: Bytes::from(batch_to_ipc(batch)),
        app_metadata: encode_metadata(&meta),
        flight_descriptor: None,
    }
}

/// Build an Update `FlightData` (single-row batch with changed columns).
pub fn update_flight_data(
    table: Option<&str>,
    batch: &RecordBatch,
    row_id: u64,
    watermark_ns: u64,
) -> FlightData {
    let meta = SeqlMetadata::Update {
        table: table.map(str::to_string),
        row_id,
        watermark_ns,
    };
    FlightData {
        data_header: Bytes::new(),
        data_body: Bytes::from(batch_to_ipc(batch)),
        app_metadata: encode_metadata(&meta),
        flight_descriptor: None,
    }
}

/// Build an Expire `FlightData` (no IPC body — the row is gone).
pub fn expire_flight_data(table: Option<&str>, row_id: u64, watermark_ns: u64) -> FlightData {
    let meta = SeqlMetadata::Expire {
        table: table.map(str::to_string),
        row_id,
        watermark_ns,
    };
    FlightData {
        data_header: Bytes::new(),
        data_body: Bytes::new(),
        app_metadata: encode_metadata(&meta),
        flight_descriptor: None,
    }
}

/// Build a Replace `FlightData` carrying the complete new result set.
pub fn replace_flight_data(
    table: Option<&str>,
    batch: &RecordBatch,
    watermark_ns: u64,
) -> FlightData {
    let meta = SeqlMetadata::Replace {
        table: table.map(str::to_string),
        watermark_ns,
    };
    FlightData {
        data_header: Bytes::new(),
        data_body: Bytes::from(batch_to_ipc(batch)),
        app_metadata: encode_metadata(&meta),
        flight_descriptor: None,
    }
}

/// Build a Heartbeat `FlightData` (no IPC body).
pub fn heartbeat_flight_data(watermark_ns: u64) -> FlightData {
    let meta = SeqlMetadata::Heartbeat { watermark_ns };
    FlightData {
        data_header: Bytes::new(),
        data_body: Bytes::new(),
        app_metadata: encode_metadata(&meta),
        flight_descriptor: None,
    }
}

/// Build a Complete `FlightData` (no IPC body).
pub fn complete_flight_data(stats: QueryStats) -> FlightData {
    let meta = SeqlMetadata::Complete { stats };
    FlightData {
        data_header: Bytes::new(),
        data_body: Bytes::new(),
        app_metadata: encode_metadata(&meta),
        flight_descriptor: None,
    }
}

/// Build a Warning `FlightData` (no IPC body).
pub fn warning_flight_data(code: u32, message: String) -> FlightData {
    let meta = SeqlMetadata::Warning { code, message };
    FlightData {
        data_header: Bytes::new(),
        data_body: Bytes::new(),
        app_metadata: encode_metadata(&meta),
        flight_descriptor: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::ipc_to_batch;
    use crate::schema::{ColumnDef, ColumnRole, DataType};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", ArrowDataType::Int64, false),
            Field::new("name", ArrowDataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
            ],
        )
        .unwrap()
    }

    // ── encode / decode round-trips ──

    #[test]
    fn seql_metadata_schema_round_trip() {
        let meta = SeqlMetadata::Schema {
            table: None,
            shape: ResponseShape::Table,
            columns: vec![ColumnDef {
                name: "span_id".into(),
                data_type: DataType::String,
                role: ColumnRole::Field,
            }],
            watermark_ns: 12345,
        };
        let encoded = encode_metadata(&meta);
        let decoded = decode_metadata(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn seql_metadata_data_round_trip() {
        let meta = SeqlMetadata::Data {
            table: Some("logs".to_string()),
        };
        let encoded = encode_metadata(&meta);
        let decoded = decode_metadata(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn seql_metadata_append_round_trip() {
        let meta = SeqlMetadata::Append {
            table: None,
            start_row_id: 42,
            watermark_ns: 999,
        };
        let encoded = encode_metadata(&meta);
        let decoded = decode_metadata(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn seql_metadata_expire_round_trip() {
        let meta = SeqlMetadata::Expire {
            table: Some("stacks".to_string()),
            row_id: 7,
            watermark_ns: 111,
        };
        let encoded = encode_metadata(&meta);
        let decoded = decode_metadata(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn seql_metadata_heartbeat_round_trip() {
        let meta = SeqlMetadata::Heartbeat { watermark_ns: 77 };
        let encoded = encode_metadata(&meta);
        let decoded = decode_metadata(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn seql_metadata_complete_round_trip() {
        let meta = SeqlMetadata::Complete {
            stats: QueryStats {
                execution_time_us: 500,
                rows_scanned: 1000,
                bytes_read: 4096,
                rows_returned: 10,
                warning_count: 0,
            },
        };
        let encoded = encode_metadata(&meta);
        let decoded = decode_metadata(&encoded).unwrap();
        assert_eq!(decoded, meta);
    }

    #[test]
    fn decode_empty_bytes_returns_none() {
        assert!(decode_metadata(&Bytes::new()).is_none());
    }

    #[test]
    fn decode_garbage_bytes_returns_none() {
        assert!(decode_metadata(&Bytes::from_static(b"not metadata")).is_none());
    }

    // ── FlightData builders ──

    #[test]
    fn data_flight_data_carries_ipc_batch() {
        let batch = make_batch();
        let fd = data_flight_data(None, &batch);

        // app_metadata decodes to Data frame
        let meta = decode_metadata(&fd.app_metadata).unwrap();
        assert!(matches!(meta, SeqlMetadata::Data { table: None }));

        // data_body decodes back to same RecordBatch
        let recovered = ipc_to_batch(&fd.data_body).unwrap();
        assert_eq!(recovered.num_rows(), 2);
        assert_eq!(recovered.num_columns(), 2);
    }

    #[test]
    fn data_flight_data_with_table_alias() {
        let batch = make_batch();
        let fd = data_flight_data(Some("logs"), &batch);

        let meta = decode_metadata(&fd.app_metadata).unwrap();
        assert!(matches!(
            meta,
            SeqlMetadata::Data {
                table: Some(ref t)
            } if t == "logs"
        ));
    }

    #[test]
    fn append_flight_data_has_row_id_and_watermark() {
        let batch = make_batch();
        let fd = append_flight_data(None, &batch, 100, 9999);

        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Append {
                table,
                start_row_id,
                watermark_ns,
            } => {
                assert_eq!(table, None);
                assert_eq!(start_row_id, 100);
                assert_eq!(watermark_ns, 9999);
            }
            other => panic!("Expected Append, got {:?}", other),
        }
        // Body contains the batch
        assert!(!fd.data_body.is_empty());
    }

    #[test]
    fn expire_flight_data_has_empty_body() {
        let fd = expire_flight_data(None, 42, 55);
        assert!(fd.data_body.is_empty());
        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Expire {
                table,
                row_id,
                watermark_ns,
            } => {
                assert_eq!(table, None);
                assert_eq!(row_id, 42);
                assert_eq!(watermark_ns, 55);
            }
            other => panic!("Expected Expire, got {:?}", other),
        }
    }

    #[test]
    fn heartbeat_has_empty_body() {
        let fd = heartbeat_flight_data(7777);
        assert!(fd.data_body.is_empty());
        let meta = decode_metadata(&fd.app_metadata).unwrap();
        assert_eq!(meta, SeqlMetadata::Heartbeat { watermark_ns: 7777 });
    }

    #[test]
    fn complete_has_empty_body() {
        let fd = complete_flight_data(QueryStats::zero());
        assert!(fd.data_body.is_empty());
        let meta = decode_metadata(&fd.app_metadata).unwrap();
        assert!(matches!(meta, SeqlMetadata::Complete { .. }));
    }

    #[test]
    fn warning_has_empty_body() {
        let fd = warning_flight_data(1, "truncated".to_string());
        assert!(fd.data_body.is_empty());
        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Warning { code, message } => {
                assert_eq!(code, 1);
                assert_eq!(message, "truncated");
            }
            other => panic!("Expected Warning, got {:?}", other),
        }
    }

    #[test]
    fn table_multiplexing_distinct_tables() {
        // Simulate a multi-table merge response stream
        let batch = make_batch();
        let primary = data_flight_data(None, &batch);
        let aux_logs = data_flight_data(Some("logs"), &batch);
        let aux_frames = data_flight_data(Some("frames"), &batch);

        // Each should decode to the correct table
        let m1 = decode_metadata(&primary.app_metadata).unwrap();
        let m2 = decode_metadata(&aux_logs.app_metadata).unwrap();
        let m3 = decode_metadata(&aux_frames.app_metadata).unwrap();

        assert!(matches!(m1, SeqlMetadata::Data { table: None }));
        assert!(matches!(m2, SeqlMetadata::Data { table: Some(ref t) } if t == "logs"));
        assert!(matches!(m3, SeqlMetadata::Data { table: Some(ref t) } if t == "frames"));
    }

    #[test]
    fn update_flight_data_round_trip() {
        let batch = make_batch();
        let fd = update_flight_data(None, &batch, 7, 1234);

        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Update {
                table,
                row_id,
                watermark_ns,
            } => {
                assert_eq!(table, None);
                assert_eq!(row_id, 7);
                assert_eq!(watermark_ns, 1234);
            }
            other => panic!("Expected Update, got {:?}", other),
        }
        assert!(!fd.data_body.is_empty());
    }

    #[test]
    fn replace_flight_data_carries_batch() {
        let batch = make_batch();
        let fd = replace_flight_data(Some("frames"), &batch, 42);
        let meta = decode_metadata(&fd.app_metadata).unwrap();
        match meta {
            SeqlMetadata::Replace {
                table,
                watermark_ns,
            } => {
                assert_eq!(table, Some("frames".to_string()));
                assert_eq!(watermark_ns, 42);
            }
            other => panic!("Expected Replace, got {:?}", other),
        }
        let recovered = ipc_to_batch(&fd.data_body).unwrap();
        assert_eq!(recovered.num_rows(), 2);
    }
}
