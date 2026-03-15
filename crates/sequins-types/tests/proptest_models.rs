//! Property-based tests for sequins-types models
//!
//! Tests core invariants using proptest to verify behavior across random inputs.

use proptest::prelude::*;
use sequins_types::models::{AttributeValue, Duration, SpanId, Timestamp, TraceId};

// Test 1: Timestamp::from_nanos doesn't panic for any i64 value
proptest! {
    #[test]
    fn prop_timestamp_from_nanos_doesnt_panic(nanos: i64) {
        let _ts = Timestamp::from_nanos(nanos);
        // Reaching here means no panic occurred
    }
}

// Test 2: Duration::from_nanos doesn't panic for any i64 value
proptest! {
    #[test]
    fn prop_duration_from_nanos_doesnt_panic(nanos: i64) {
        let _duration = Duration::from_nanos(nanos);
        // Reaching here means no panic occurred
    }
}

// Test 3: TraceId hex round trip - random bytes -> TraceId -> hex -> from_hex matches
proptest! {
    #[test]
    fn prop_trace_id_hex_round_trip(bytes: [u8; 16]) {
        let trace_id = TraceId::from_bytes(bytes);
        let hex = trace_id.to_hex();
        let parsed = TraceId::from_hex(&hex).expect("Valid hex should parse");

        prop_assert_eq!(trace_id, parsed);
        prop_assert_eq!(trace_id.to_bytes(), bytes);
    }
}

// Test 4: SpanId hex round trip - random bytes -> SpanId -> hex -> from_hex matches
proptest! {
    #[test]
    fn prop_span_id_hex_round_trip(bytes: [u8; 8]) {
        let span_id = SpanId::from_bytes(bytes);
        let hex = span_id.to_hex();
        let parsed = SpanId::from_hex(&hex).expect("Valid hex should parse");

        prop_assert_eq!(span_id, parsed);
        prop_assert_eq!(span_id.to_bytes(), bytes);
    }
}

// Test 5: AttributeValue::String round trip through JSON
proptest! {
    #[test]
    fn prop_attribute_value_string_round_trip(s in "[a-zA-Z0-9 ]{0,100}") {
        let attr = AttributeValue::String(s.clone());

        // Serialize to JSON
        let json = serde_json::to_string(&attr).expect("Should serialize");

        // Deserialize back
        let parsed: AttributeValue = serde_json::from_str(&json).expect("Should deserialize");

        prop_assert_eq!(attr, parsed);
    }
}

// Test 6: AttributeValue::Int round trip through JSON
proptest! {
    #[test]
    fn prop_attribute_value_int_round_trip(value: i64) {
        let attr = AttributeValue::Int(value);

        // Serialize to JSON
        let json = serde_json::to_string(&attr).expect("Should serialize");

        // Deserialize back
        let parsed: AttributeValue = serde_json::from_str(&json).expect("Should deserialize");

        prop_assert_eq!(attr, parsed);
    }
}
