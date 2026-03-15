/// Custom assertions for testing Sequins data types
use sequins::models::{
    logs::LogEntry,
    metrics::{HistogramDataPoint, Metric, MetricDataPoint},
    profiles::Profile,
    traces::Span,
};
use std::collections::HashMap;

/// Assert that two spans are equal (with better error messages)
#[allow(dead_code)]
pub fn assert_span_eq(left: &Span, right: &Span) {
    assert_eq!(
        left.trace_id, right.trace_id,
        "Trace IDs don't match:\n  left: {:?}\n  right: {:?}",
        left.trace_id, right.trace_id
    );
    assert_eq!(
        left.span_id, right.span_id,
        "Span IDs don't match:\n  left: {:?}\n  right: {:?}",
        left.span_id, right.span_id
    );
    assert_eq!(
        left.parent_span_id, right.parent_span_id,
        "Parent span IDs don't match"
    );
    assert_eq!(
        left.service_name, right.service_name,
        "Service names don't match"
    );
    assert_eq!(
        left.operation_name, right.operation_name,
        "Operation names don't match"
    );
    assert_eq!(left.start_time, right.start_time, "Start times don't match");
    assert_eq!(left.end_time, right.end_time, "End times don't match");
    assert_eq!(left.status, right.status, "Statuses don't match");
    assert_eq!(left.span_kind, right.span_kind, "Span kinds don't match");

    // Compare attributes
    assert_eq!(
        left.attributes.len(),
        right.attributes.len(),
        "Attribute counts don't match"
    );
    for (key, left_value) in &left.attributes {
        let right_value = right
            .attributes
            .get(key)
            .unwrap_or_else(|| panic!("Attribute '{}' missing in right span", key));
        assert_eq!(
            left_value, right_value,
            "Attribute '{}' values don't match:\n  left: {:?}\n  right: {:?}",
            key, left_value, right_value
        );
    }
}

/// Assert that two logs are equal (with better error messages)
#[allow(dead_code)]
pub fn assert_log_eq(left: &LogEntry, right: &LogEntry) {
    assert_eq!(left.id, right.id, "Log IDs don't match");
    assert_eq!(left.timestamp, right.timestamp, "Timestamps don't match");
    assert_eq!(
        left.service_name, right.service_name,
        "Service names don't match"
    );
    assert_eq!(left.severity, right.severity, "Severities don't match");
    assert_eq!(left.body, right.body, "Bodies don't match");
    assert_eq!(left.trace_id, right.trace_id, "Trace IDs don't match");
    assert_eq!(left.span_id, right.span_id, "Span IDs don't match");

    // Compare attributes
    assert_eq!(
        left.attributes.len(),
        right.attributes.len(),
        "Attribute counts don't match"
    );
    for (key, left_value) in &left.attributes {
        let right_value = right
            .attributes
            .get(key)
            .unwrap_or_else(|| panic!("Attribute '{}' missing in right log", key));
        assert_eq!(
            left_value, right_value,
            "Attribute '{}' values don't match",
            key
        );
    }
}

/// Assert that two metrics are equal (with better error messages)
#[allow(dead_code)]
pub fn assert_metric_eq(left: &Metric, right: &Metric) {
    assert_eq!(left.id, right.id, "Metric IDs don't match");
    assert_eq!(left.name, right.name, "Metric names don't match");
    assert_eq!(
        left.description, right.description,
        "Descriptions don't match"
    );
    assert_eq!(left.unit, right.unit, "Units don't match");
    assert_eq!(
        left.metric_type, right.metric_type,
        "Metric types don't match"
    );
    assert_eq!(
        left.service_name, right.service_name,
        "Service names don't match"
    );
}

/// Assert that two metric data points are equal
#[allow(dead_code)]
pub fn assert_metric_data_point_eq(left: &MetricDataPoint, right: &MetricDataPoint) {
    assert_eq!(left.metric_id, right.metric_id, "Metric IDs don't match");
    assert_eq!(left.timestamp, right.timestamp, "Timestamps don't match");
    assert!(
        (left.value - right.value).abs() < 0.0001,
        "Values don't match: {} vs {}",
        left.value,
        right.value
    );
}

/// Assert that two histogram data points are equal
#[allow(dead_code)]
pub fn assert_histogram_data_point_eq(left: &HistogramDataPoint, right: &HistogramDataPoint) {
    assert_eq!(left.metric_id, right.metric_id, "Metric IDs don't match");
    assert_eq!(left.timestamp, right.timestamp, "Timestamps don't match");
    assert_eq!(left.count, right.count, "Counts don't match");
    assert!((left.sum - right.sum).abs() < 0.0001, "Sums don't match");
    assert_eq!(
        left.bucket_counts.len(),
        right.bucket_counts.len(),
        "Bucket count lengths don't match"
    );
    assert_eq!(
        left.explicit_bounds.len(),
        right.explicit_bounds.len(),
        "Explicit bounds lengths don't match"
    );
}

/// Assert that two profiles are equal (with better error messages)
#[allow(dead_code)]
pub fn assert_profile_eq(left: &Profile, right: &Profile) {
    assert_eq!(left.id, right.id, "Profile IDs don't match");
    assert_eq!(left.timestamp, right.timestamp, "Timestamps don't match");
    assert_eq!(
        left.service_name, right.service_name,
        "Service names don't match"
    );
    assert_eq!(
        left.profile_type, right.profile_type,
        "Profile types don't match"
    );
    assert_eq!(
        left.sample_type, right.sample_type,
        "Sample types don't match"
    );
    assert_eq!(
        left.data.len(),
        right.data.len(),
        "Data lengths don't match"
    );
}

/// Assert that JSONB roundtrip serialization works correctly
#[allow(dead_code)]
pub fn assert_jsonb_roundtrip(
    attributes: &HashMap<String, sequins::models::traces::AttributeValue>,
) {
    // Serialize to JSON
    let json_str =
        serde_json::to_string(attributes).expect("Failed to serialize attributes to JSON");

    // Deserialize back
    let roundtrip: HashMap<String, sequins::models::traces::AttributeValue> =
        serde_json::from_str(&json_str).expect("Failed to deserialize attributes from JSON");

    // Assert equality
    assert_eq!(
        attributes.len(),
        roundtrip.len(),
        "Attribute count changed after roundtrip"
    );

    for (key, original_value) in attributes {
        let roundtrip_value = roundtrip
            .get(key)
            .unwrap_or_else(|| panic!("Attribute '{}' lost during roundtrip", key));
        assert_eq!(
            original_value, roundtrip_value,
            "Attribute '{}' value changed during roundtrip:\n  original: {:?}\n  roundtrip: {:?}",
            key, original_value, roundtrip_value
        );
    }
}

/// Assert that a string is safe from SQL injection
#[allow(dead_code)]
pub fn assert_no_sql_injection(input: &str, query_result: &str) {
    // Common SQL injection patterns that should NOT appear in executed queries
    let dangerous_patterns = vec![
        "DROP TABLE",
        "DELETE FROM",
        "TRUNCATE",
        "'; --",
        "' OR '1'='1",
        "' OR 1=1",
        "UNION SELECT",
    ];

    for pattern in dangerous_patterns {
        assert!(
            !query_result
                .to_uppercase()
                .contains(&pattern.to_uppercase()),
            "Query contains dangerous SQL pattern '{}': {}",
            pattern,
            query_result
        );
    }

    // Verify that dangerous input was properly escaped or parameterized
    if input.contains("';") || input.contains("--") {
        assert!(
            !query_result.contains("';") || query_result.contains("\\';"),
            "Dangerous input not properly escaped: {}",
            query_result
        );
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::OtlpFixtures;
    use super::*;

    #[test]
    fn test_assert_span_eq_identical() {
        let span1 = OtlpFixtures::valid_span();
        let span2 = span1.clone();
        assert_span_eq(&span1, &span2);
    }

    #[test]
    #[should_panic(expected = "Service names don't match")]
    fn test_assert_span_eq_different_service() {
        let span1 = OtlpFixtures::valid_span();
        let mut span2 = span1.clone();
        span2.service_name = "different-service".to_string();
        assert_span_eq(&span1, &span2);
    }

    #[test]
    fn test_assert_log_eq_identical() {
        let log1 = OtlpFixtures::valid_log();
        let log2 = log1.clone();
        assert_log_eq(&log1, &log2);
    }

    #[test]
    fn test_assert_metric_eq_identical() {
        let (metric1, _) = OtlpFixtures::valid_gauge();
        let metric2 = metric1.clone();
        assert_metric_eq(&metric1, &metric2);
    }

    #[test]
    fn test_assert_profile_eq_identical() {
        let profile1 = OtlpFixtures::valid_cpu_profile();
        let profile2 = profile1.clone();
        assert_profile_eq(&profile1, &profile2);
    }

    #[test]
    fn test_assert_jsonb_roundtrip() {
        use sequins::models::traces::AttributeValue;
        let mut attributes = HashMap::new();
        attributes.insert(
            "string".to_string(),
            AttributeValue::String("test".to_string()),
        );
        attributes.insert("int".to_string(), AttributeValue::Int(42));
        attributes.insert("bool".to_string(), AttributeValue::Bool(true));
        attributes.insert("float".to_string(), AttributeValue::Double(3.14));

        assert_jsonb_roundtrip(&attributes);
    }

    #[test]
    fn test_assert_no_sql_injection_safe() {
        let input = "normal input";
        let query = "SELECT * FROM users WHERE name = 'normal input'";
        assert_no_sql_injection(input, query);
    }

    #[test]
    #[should_panic(expected = "Query contains dangerous SQL pattern")]
    fn test_assert_no_sql_injection_unsafe() {
        let input = "'; DROP TABLE users; --";
        let query = "SELECT * FROM users WHERE name = ''; DROP TABLE users; --'";
        assert_no_sql_injection(input, query);
    }
}
