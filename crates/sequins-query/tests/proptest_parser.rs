//! Property-based tests for sequins-query parser
//!
//! Tests parser invariants using proptest to verify robustness across random inputs.

use proptest::prelude::*;
use sequins_query::parser::parse;

// Test 1: Parsing valid signal keywords doesn't panic
proptest! {
    #[test]
    fn prop_parse_valid_signals_doesnt_panic(
        signal in prop::sample::select(vec!["spans", "logs", "datapoints", "metrics", "samples", "traces"])
    ) {
        let query = format!("{} last 1h", signal);
        // Should not panic even if parse fails
        let _result = parse(&query);
        // Reaching here means no panic occurred
    }
}

// Test 2: Parsing with random time durations doesn't panic
proptest! {
    #[test]
    fn prop_parse_time_durations_doesnt_panic(duration in 1u64..1000000) {
        let query = format!("spans last {}s", duration);
        // Should not panic even if parse fails
        let _result = parse(&query);
        // Reaching here means no panic occurred
    }
}

// Test 3: Valid queries parse successfully
proptest! {
    #[test]
    fn prop_valid_queries_parse(
        signal in prop::sample::select(vec!["spans", "logs", "metrics"]),
        duration in 1u64..10000,
    ) {
        let query = format!("{} last {}s", signal, duration);
        let result = parse(&query);
        prop_assert!(result.is_ok(), "Valid query should parse successfully: {}", query);
    }
}

// Test 4: Parse with quoted strings in filters
proptest! {
    #[test]
    fn prop_parse_quoted_filter_doesnt_panic(s in "[a-zA-Z0-9]{1,20}") {
        let query = format!("spans last 1h | where name == \"{}\"", s);
        // Should not panic
        let _result = parse(&query);
        // Reaching here means no panic occurred
    }
}
