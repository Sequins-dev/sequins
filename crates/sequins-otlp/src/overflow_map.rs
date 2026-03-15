//! CBOR-encoded overflow attribute map.
//!
//! Attributes that are not promoted to first-class columns in the
//! `SchemaCatalog` are stored as a `Map<Utf8, LargeBinary>` Arrow column
//! called `_overflow_attrs`.  Each binary value is a CBOR-encoded
//! `ciborium::Value` so the original type information (string / int64 /
//! float64 / bool / array) is preserved at the byte level.
//!
//! # Write path
//!
//! Call `OverflowMapBuilder::new(n_rows)`, append entries with
//! `push_entry` / `finish_row`, then call `finish()` to get the
//! `Arc<ArrayRef>` to add to your `RecordBatch`.
//!
//! # Read path (DataFusion UDFs)
//!
//! `register_overflow_udfs(ctx)` registers the following scalar UDFs on a
//! `SessionContext` (requires `datafusion` feature):
//!
//! - `overflow_get_str(map_col, key)`  → `Utf8` (null if key missing / not a string)
//! - `overflow_get_i64(map_col, key)`  → `Int64`
//! - `overflow_get_f64(map_col, key)`  → `Float64`
//! - `overflow_get_bool(map_col, key)` → `Boolean`

use arrow::array::{Array, LargeBinaryBuilder, MapBuilder, MapFieldNames, StringBuilder};
use ciborium::Value as CborValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use sequins_types::models::AttributeValue;
use std::io::Cursor;
use std::sync::Arc;

// ---------------------------------------------------------------------------
// CBOR encode / decode helpers
// ---------------------------------------------------------------------------

/// CBOR-encode a single OTLP `AnyValue`.
///
/// Returns the encoded bytes.  If encoding fails, returns empty bytes.
pub fn cbor_encode_otlp_value(v: &OtlpValue) -> Vec<u8> {
    let cbor_val = otlp_value_to_cbor(v);
    let mut buf = Vec::new();
    if ciborium::into_writer(&cbor_val, &mut buf).is_ok() {
        buf
    } else {
        Vec::new()
    }
}

fn otlp_value_to_cbor(v: &OtlpValue) -> CborValue {
    match v {
        OtlpValue::StringValue(s) => CborValue::Text(s.clone()),
        OtlpValue::IntValue(i) => CborValue::Integer((*i).into()),
        OtlpValue::DoubleValue(f) => CborValue::Float(*f),
        OtlpValue::BoolValue(b) => CborValue::Bool(*b),
        OtlpValue::ArrayValue(arr) => {
            let items: Vec<CborValue> = arr
                .values
                .iter()
                .filter_map(|av| av.value.as_ref().map(otlp_value_to_cbor))
                .collect();
            CborValue::Array(items)
        }
        OtlpValue::KvlistValue(kv) => {
            let pairs: Vec<(CborValue, CborValue)> = kv
                .values
                .iter()
                .map(|kv| {
                    let k = CborValue::Text(kv.key.clone());
                    let v = kv
                        .value
                        .as_ref()
                        .and_then(|av| av.value.as_ref().map(otlp_value_to_cbor))
                        .unwrap_or(CborValue::Null);
                    (k, v)
                })
                .collect();
            CborValue::Map(pairs)
        }
        OtlpValue::BytesValue(b) => CborValue::Bytes(b.clone()),
    }
}

/// CBOR-encode a single domain `AttributeValue`.
///
/// Returns the encoded bytes.  If encoding fails, returns empty bytes.
pub fn cbor_encode_attribute_value(v: &AttributeValue) -> Vec<u8> {
    let cbor_val = attribute_value_to_cbor(v);
    let mut buf = Vec::new();
    if ciborium::into_writer(&cbor_val, &mut buf).is_ok() {
        buf
    } else {
        Vec::new()
    }
}

fn attribute_value_to_cbor(v: &AttributeValue) -> CborValue {
    match v {
        AttributeValue::String(s) => CborValue::Text(s.clone()),
        AttributeValue::Bool(b) => CborValue::Bool(*b),
        AttributeValue::Int(i) => CborValue::Integer((*i).into()),
        AttributeValue::Double(f) => CborValue::Float(*f),
        AttributeValue::Bytes(b) => CborValue::Bytes(b.clone()),
        AttributeValue::KvList(kvs) => {
            let pairs: Vec<(CborValue, CborValue)> = kvs
                .iter()
                .map(|(k, v)| (CborValue::Text(k.clone()), attribute_value_to_cbor(v)))
                .collect();
            CborValue::Map(pairs)
        }
        AttributeValue::Array(arr) => {
            CborValue::Array(arr.iter().map(attribute_value_to_cbor).collect())
        }
        AttributeValue::StringArray(arr) => {
            CborValue::Array(arr.iter().map(|s| CborValue::Text(s.clone())).collect())
        }
        AttributeValue::BoolArray(arr) => {
            CborValue::Array(arr.iter().map(|b| CborValue::Bool(*b)).collect())
        }
        AttributeValue::IntArray(arr) => CborValue::Array(
            arr.iter()
                .map(|i| CborValue::Integer((*i).into()))
                .collect(),
        ),
        AttributeValue::DoubleArray(arr) => {
            CborValue::Array(arr.iter().map(|f| CborValue::Float(*f)).collect())
        }
    }
}

/// Build the `_overflow_attrs` column from domain-type attribute rows.
///
/// `rows` contains one `Vec<(&str, &AttributeValue)>` per row. Each entry is a
/// (key, value) pair that was NOT promoted to a first-class column.
///
/// Returns `Arc<dyn Array>` suitable for inclusion in a `RecordBatch`.
pub fn build_overflow_column_domain(rows: &[Vec<(&str, &AttributeValue)>]) -> Arc<dyn Array> {
    let mut builder = OverflowMapBuilder::new();
    for row in rows {
        for (key, val) in row {
            let bytes = cbor_encode_attribute_value(val);
            builder.push_entry(key, bytes);
        }
        let _ = builder.finish_row(true);
    }
    builder.finish()
}

/// Decode a CBOR-encoded value back to a string representation, if possible.
pub fn cbor_decode_as_str(bytes: &[u8]) -> Option<String> {
    let mut cursor = Cursor::new(bytes);
    let val: Result<CborValue, _> = ciborium::from_reader(&mut cursor);
    match val {
        Ok(CborValue::Text(s)) => Some(s),
        Ok(other) => Some(format!("{other:?}")),
        Err(_) => None,
    }
}

/// Decode a CBOR-encoded value back to i64, if possible.
pub fn cbor_decode_as_i64(bytes: &[u8]) -> Option<i64> {
    let mut cursor = Cursor::new(bytes);
    let val: Result<CborValue, _> = ciborium::from_reader(&mut cursor);
    match val {
        Ok(CborValue::Integer(i)) => i128::from(i).try_into().ok(),
        Ok(CborValue::Float(f)) => Some(f as i64),
        _ => None,
    }
}

/// Decode a CBOR-encoded value back to f64, if possible.
pub fn cbor_decode_as_f64(bytes: &[u8]) -> Option<f64> {
    let mut cursor = Cursor::new(bytes);
    let val: Result<CborValue, _> = ciborium::from_reader(&mut cursor);
    match val {
        Ok(CborValue::Float(f)) => Some(f),
        Ok(CborValue::Integer(i)) => Some(i128::from(i) as f64),
        _ => None,
    }
}

/// Decode a CBOR-encoded value back to bool, if possible.
pub fn cbor_decode_as_bool(bytes: &[u8]) -> Option<bool> {
    let mut cursor = Cursor::new(bytes);
    let val: Result<CborValue, _> = ciborium::from_reader(&mut cursor);
    match val {
        Ok(CborValue::Bool(b)) => Some(b),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// OverflowMapBuilder
// ---------------------------------------------------------------------------

/// Builds the `_overflow_attrs` Map<Utf8, LargeBinary> column row-by-row.
///
/// # Usage
///
/// ```ignore
/// let mut builder = OverflowMapBuilder::new();
/// for kv in overflow_attrs {
///     builder.push_entry(&kv.key, cbor_encode_otlp_value(&kv.value));
/// }
/// builder.finish_row(true)?;   // true = row is not null
/// let array: Arc<dyn Array> = builder.finish();
/// ```
pub struct OverflowMapBuilder {
    inner: MapBuilder<StringBuilder, LargeBinaryBuilder>,
}

impl OverflowMapBuilder {
    pub fn new() -> Self {
        // Use "key"/"value" to match overflow_attrs_field() schema definition
        let field_names = MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        };
        OverflowMapBuilder {
            inner: MapBuilder::new(
                Some(field_names),
                StringBuilder::new(),
                LargeBinaryBuilder::new(),
            ),
        }
    }

    /// Append a single key-value entry for the current row.
    pub fn push_entry(&mut self, key: &str, value: Vec<u8>) {
        self.inner.keys().append_value(key);
        self.inner.values().append_value(&value);
    }

    /// Finalise the current row.
    ///
    /// Call once per row after all entries are pushed.
    /// `is_valid = false` writes a null map for the row.
    pub fn finish_row(&mut self, is_valid: bool) -> Result<(), arrow::error::ArrowError> {
        self.inner.append(is_valid)
    }

    /// Consume the builder and return the finished `MapArray`.
    pub fn finish(mut self) -> Arc<dyn Array> {
        Arc::new(self.inner.finish()) as Arc<dyn Array>
    }
}

impl Default for OverflowMapBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Convenience: build overflow column from a slice of per-row overflow lists
// ---------------------------------------------------------------------------

/// Build the `_overflow_attrs` column for a batch of rows.
///
/// `rows_overflow` contains one `Vec<&KeyValue>` per row.  Each entry is a
/// list of OTLP `KeyValue`s that were NOT promoted to first-class columns.
///
/// Returns `Arc<dyn Array>` suitable for inclusion in a `RecordBatch`.
pub fn build_overflow_column(rows_overflow: &[Vec<&KeyValue>]) -> Arc<dyn Array> {
    let n = rows_overflow.len();
    let mut builder = OverflowMapBuilder::new();
    for row in rows_overflow {
        for kv in row {
            if let Some(av) = &kv.value {
                if let Some(v) = &av.value {
                    let bytes = cbor_encode_otlp_value(v);
                    builder.push_entry(&kv.key, bytes);
                }
            }
        }
        // null map when row has no overflow entries
        let _ = builder.finish_row(true);
    }
    let result = builder.finish();
    debug_assert_eq!(
        result.len(),
        n,
        "build_overflow_column output length {} != input row count {}",
        result.len(),
        n
    );
    result
}

// ---------------------------------------------------------------------------
// DataFusion UDF registration (requires "datafusion" feature)
// ---------------------------------------------------------------------------

/// Extract a string value from the `_overflow_attrs` map.
///
/// `overflow_get_str(map_col, 'key')` → `Utf8` (null if missing or not a string).
#[cfg(feature = "datafusion")]
pub fn make_overflow_get_str_udf() -> datafusion::logical_expr::ScalarUDF {
    use arrow::array::{MapArray, StringArray};
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    };
    use once_cell::sync::Lazy;
    use std::hash::Hash;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct OverflowGetStr;

    static SIG_STR: Lazy<Signature> = Lazy::new(|| Signature::any(2, Volatility::Immutable));

    impl ScalarUDFImpl for OverflowGetStr {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "overflow_get_str"
        }
        fn signature(&self) -> &Signature {
            &SIG_STR
        }
        fn return_type(
            &self,
            _arg_types: &[DataType],
        ) -> Result<DataType, datafusion::error::DataFusionError> {
            Ok(DataType::Utf8)
        }
        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> Result<ColumnarValue, datafusion::error::DataFusionError> {
            let n = args.number_rows;
            let map_array = args.args[0].clone().into_array(n)?;
            let map_array = map_array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "overflow_get_str: first argument must be a MapArray".to_string(),
                    )
                })?;

            let key_str = match &args.args[1] {
                ColumnarValue::Scalar(s) => {
                    if let datafusion::scalar::ScalarValue::Utf8(Some(k)) = s {
                        k.clone()
                    } else {
                        return Ok(ColumnarValue::Array(Arc::new(StringArray::from(
                            vec![None::<&str>; map_array.len()],
                        ))));
                    }
                }
                ColumnarValue::Array(arr) => {
                    let arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                        datafusion::error::DataFusionError::Execution(
                            "overflow_get_str: key must be a string".to_string(),
                        )
                    })?;
                    if arr.is_empty() || arr.is_null(0) {
                        return Ok(ColumnarValue::Array(Arc::new(StringArray::from(
                            vec![None::<&str>; map_array.len()],
                        ))));
                    }
                    arr.value(0).to_string()
                }
            };

            let mut results: Vec<Option<String>> = Vec::with_capacity(map_array.len());
            for row in 0..map_array.len() {
                if map_array.is_null(row) {
                    results.push(None);
                    continue;
                }
                let entries = map_array.value(row);
                let keys = entries.column(0).as_any().downcast_ref::<StringArray>();
                let vals = entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::LargeBinaryArray>();
                let mut found = None;
                if let (Some(k_arr), Some(v_arr)) = (keys, vals) {
                    for i in 0..k_arr.len() {
                        if !k_arr.is_null(i) && k_arr.value(i) == key_str {
                            if !v_arr.is_null(i) {
                                found = cbor_decode_as_str(v_arr.value(i));
                            }
                            break;
                        }
                    }
                }
                results.push(found);
            }
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(results))))
        }
    }

    datafusion::logical_expr::ScalarUDF::new_from_impl(OverflowGetStr)
}

/// Extract an i64 value from the `_overflow_attrs` map.
#[cfg(feature = "datafusion")]
pub fn make_overflow_get_i64_udf() -> datafusion::logical_expr::ScalarUDF {
    use arrow::array::{Int64Builder, MapArray, StringArray};
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    };
    use once_cell::sync::Lazy;
    use std::hash::Hash;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct OverflowGetI64;

    static SIG_I64: Lazy<Signature> = Lazy::new(|| Signature::any(2, Volatility::Immutable));

    impl ScalarUDFImpl for OverflowGetI64 {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "overflow_get_i64"
        }
        fn signature(&self) -> &Signature {
            &SIG_I64
        }
        fn return_type(
            &self,
            _arg_types: &[DataType],
        ) -> Result<DataType, datafusion::error::DataFusionError> {
            Ok(DataType::Int64)
        }
        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> Result<ColumnarValue, datafusion::error::DataFusionError> {
            let n = args.number_rows;
            let map_array = args.args[0].clone().into_array(n)?;
            let map_array = map_array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "overflow_get_i64: first argument must be a MapArray".to_string(),
                    )
                })?;

            let key_str = match &args.args[1] {
                ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(k))) => k.clone(),
                _ => return Ok(ColumnarValue::Array(Arc::new(Int64Builder::new().finish()))),
            };

            let mut builder = Int64Builder::new();
            for row in 0..map_array.len() {
                if map_array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let entries = map_array.value(row);
                let keys = entries.column(0).as_any().downcast_ref::<StringArray>();
                let vals = entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::LargeBinaryArray>();
                let mut found = None;
                if let (Some(k_arr), Some(v_arr)) = (keys, vals) {
                    for i in 0..k_arr.len() {
                        if !k_arr.is_null(i) && k_arr.value(i) == key_str {
                            if !v_arr.is_null(i) {
                                found = cbor_decode_as_i64(v_arr.value(i));
                            }
                            break;
                        }
                    }
                }
                match found {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
    }

    datafusion::logical_expr::ScalarUDF::new_from_impl(OverflowGetI64)
}

/// Extract an f64 value from the `_overflow_attrs` map.
#[cfg(feature = "datafusion")]
pub fn make_overflow_get_f64_udf() -> datafusion::logical_expr::ScalarUDF {
    use arrow::array::{Float64Builder, MapArray, StringArray};
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    };
    use once_cell::sync::Lazy;
    use std::hash::Hash;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct OverflowGetF64;

    static SIG_F64: Lazy<Signature> = Lazy::new(|| Signature::any(2, Volatility::Immutable));

    impl ScalarUDFImpl for OverflowGetF64 {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "overflow_get_f64"
        }
        fn signature(&self) -> &Signature {
            &SIG_F64
        }
        fn return_type(
            &self,
            _arg_types: &[DataType],
        ) -> Result<DataType, datafusion::error::DataFusionError> {
            Ok(DataType::Float64)
        }
        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> Result<ColumnarValue, datafusion::error::DataFusionError> {
            let n = args.number_rows;
            let map_array = args.args[0].clone().into_array(n)?;
            let map_array = map_array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "overflow_get_f64: first argument must be a MapArray".to_string(),
                    )
                })?;

            let key_str = match &args.args[1] {
                ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(k))) => k.clone(),
                _ => {
                    return Ok(ColumnarValue::Array(Arc::new(
                        Float64Builder::new().finish(),
                    )))
                }
            };

            let mut builder = Float64Builder::new();
            for row in 0..map_array.len() {
                if map_array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let entries = map_array.value(row);
                let keys = entries.column(0).as_any().downcast_ref::<StringArray>();
                let vals = entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::LargeBinaryArray>();
                let mut found = None;
                if let (Some(k_arr), Some(v_arr)) = (keys, vals) {
                    for i in 0..k_arr.len() {
                        if !k_arr.is_null(i) && k_arr.value(i) == key_str {
                            if !v_arr.is_null(i) {
                                found = cbor_decode_as_f64(v_arr.value(i));
                            }
                            break;
                        }
                    }
                }
                match found {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
    }

    datafusion::logical_expr::ScalarUDF::new_from_impl(OverflowGetF64)
}

/// Extract a bool value from the `_overflow_attrs` map.
#[cfg(feature = "datafusion")]
pub fn make_overflow_get_bool_udf() -> datafusion::logical_expr::ScalarUDF {
    use arrow::array::{BooleanBuilder, MapArray, StringArray};
    use arrow::datatypes::DataType;
    use datafusion::logical_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    };
    use once_cell::sync::Lazy;
    use std::hash::Hash;

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct OverflowGetBool;

    static SIG_BOOL: Lazy<Signature> = Lazy::new(|| Signature::any(2, Volatility::Immutable));

    impl ScalarUDFImpl for OverflowGetBool {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn name(&self) -> &str {
            "overflow_get_bool"
        }
        fn signature(&self) -> &Signature {
            &SIG_BOOL
        }
        fn return_type(
            &self,
            _arg_types: &[DataType],
        ) -> Result<DataType, datafusion::error::DataFusionError> {
            Ok(DataType::Boolean)
        }
        fn invoke_with_args(
            &self,
            args: ScalarFunctionArgs,
        ) -> Result<ColumnarValue, datafusion::error::DataFusionError> {
            let n = args.number_rows;
            let map_array = args.args[0].clone().into_array(n)?;
            let map_array = map_array
                .as_any()
                .downcast_ref::<MapArray>()
                .ok_or_else(|| {
                    datafusion::error::DataFusionError::Execution(
                        "overflow_get_bool: first argument must be a MapArray".to_string(),
                    )
                })?;

            let key_str = match &args.args[1] {
                ColumnarValue::Scalar(datafusion::scalar::ScalarValue::Utf8(Some(k))) => k.clone(),
                _ => {
                    return Ok(ColumnarValue::Array(Arc::new(
                        BooleanBuilder::new().finish(),
                    )))
                }
            };

            let mut builder = BooleanBuilder::new();
            for row in 0..map_array.len() {
                if map_array.is_null(row) {
                    builder.append_null();
                    continue;
                }
                let entries = map_array.value(row);
                let keys = entries.column(0).as_any().downcast_ref::<StringArray>();
                let vals = entries
                    .column(1)
                    .as_any()
                    .downcast_ref::<arrow::array::LargeBinaryArray>();
                let mut found = None;
                if let (Some(k_arr), Some(v_arr)) = (keys, vals) {
                    for i in 0..k_arr.len() {
                        if !k_arr.is_null(i) && k_arr.value(i) == key_str {
                            if !v_arr.is_null(i) {
                                found = cbor_decode_as_bool(v_arr.value(i));
                            }
                            break;
                        }
                    }
                }
                match found {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(ColumnarValue::Array(Arc::new(builder.finish())))
        }
    }

    datafusion::logical_expr::ScalarUDF::new_from_impl(OverflowGetBool)
}

/// Register all overflow extraction UDFs on the given `SessionContext`.
#[cfg(feature = "datafusion")]
pub fn register_overflow_udfs(ctx: &datafusion::prelude::SessionContext) {
    ctx.register_udf(make_overflow_get_str_udf());
    ctx.register_udf(make_overflow_get_i64_udf());
    ctx.register_udf(make_overflow_get_f64_udf());
    ctx.register_udf(make_overflow_get_bool_udf());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::MapArray;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};

    fn make_kv(key: &str, val: OtlpValue) -> KeyValue {
        KeyValue {
            key: key.to_string(),
            value: Some(AnyValue { value: Some(val) }),
        }
    }

    #[test]
    fn test_cbor_encode_string() {
        let v = OtlpValue::StringValue("hello".to_string());
        let bytes = cbor_encode_otlp_value(&v);
        assert!(!bytes.is_empty());
        assert_eq!(cbor_decode_as_str(&bytes).unwrap(), "hello");
    }

    #[test]
    fn test_cbor_encode_int() {
        let v = OtlpValue::IntValue(42);
        let bytes = cbor_encode_otlp_value(&v);
        assert_eq!(cbor_decode_as_i64(&bytes).unwrap(), 42);
    }

    #[test]
    fn test_cbor_encode_float() {
        let v = OtlpValue::DoubleValue(1.5);
        let bytes = cbor_encode_otlp_value(&v);
        let decoded = cbor_decode_as_f64(&bytes).unwrap();
        assert!((decoded - 1.5).abs() < 1e-10);
    }

    #[test]
    fn test_cbor_encode_bool() {
        let v = OtlpValue::BoolValue(true);
        let bytes = cbor_encode_otlp_value(&v);
        assert!(cbor_decode_as_bool(&bytes).unwrap());
    }

    #[test]
    fn test_overflow_map_builder_empty_row() {
        let mut builder = OverflowMapBuilder::new();
        builder.finish_row(true).unwrap();
        let arr = builder.finish();
        assert_eq!(arr.len(), 1);
    }

    #[test]
    fn test_overflow_map_builder_with_entries() {
        let mut builder = OverflowMapBuilder::new();
        let bytes = cbor_encode_otlp_value(&OtlpValue::StringValue("world".to_string()));
        builder.push_entry("hello", bytes);
        builder.finish_row(true).unwrap();
        let arr = builder.finish();
        assert_eq!(arr.len(), 1);

        let map_arr = arr.as_any().downcast_ref::<MapArray>().unwrap();
        let entries = map_arr.value(0);
        assert_eq!(entries.len(), 1);

        let keys = entries
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(keys.value(0), "hello");
    }

    #[test]
    fn test_build_overflow_column_multiple_rows() {
        let kv1 = make_kv("custom.key", OtlpValue::StringValue("val1".to_string()));
        let kv2 = make_kv("other.key", OtlpValue::IntValue(99));

        let rows: Vec<Vec<&KeyValue>> = vec![vec![&kv1], vec![&kv2], vec![]];

        let arr = build_overflow_column(&rows);
        assert_eq!(arr.len(), 3);

        let map_arr = arr.as_any().downcast_ref::<MapArray>().unwrap();
        // Row 0 has 1 entry
        assert_eq!(map_arr.value(0).len(), 1);
        // Row 1 has 1 entry
        assert_eq!(map_arr.value(1).len(), 1);
        // Row 2 has 0 entries
        assert_eq!(map_arr.value(2).len(), 0);
    }
}
