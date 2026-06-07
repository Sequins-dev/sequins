//! Correlation key tables for SeQL navigate/merge operations
//!
//! Defines which signal pairs can be correlated and what column to join on.
//! These tables have zero storage dependencies — they reference only the `Signal`
//! enum and static strings.

use crate::ast::Signal;

/// Centralized definition of all valid merge paths.
/// Format: (from_signal, to_signal, join_key_column)
///
/// All joins are scalar equi-joins. The stacks→frames path joins on `frame_id`
/// (scalar) after the stacks schema was normalized to a junction table.
static MERGE_PATHS: &[(Signal, Signal, &str)] = &[
    // ═══ TRACE RELATIONSHIPS (trace_id) ═══
    (Signal::Spans, Signal::Logs, "trace_id"),
    (Signal::Logs, Signal::Spans, "trace_id"),
    (Signal::Spans, Signal::Traces, "trace_id"),
    (Signal::Traces, Signal::Spans, "trace_id"),
    (Signal::Traces, Signal::Logs, "trace_id"),
    (Signal::Logs, Signal::Traces, "trace_id"),
    // ═══ METRIC RELATIONSHIPS (metric_id) ═══
    (Signal::Metrics, Signal::Datapoints, "metric_id"),
    (Signal::Datapoints, Signal::Metrics, "metric_id"),
    (Signal::Metrics, Signal::Histograms, "metric_id"),
    (Signal::Histograms, Signal::Metrics, "metric_id"),
    // ═══ PROFILE HIERARCHY ═══
    // profile_id: profiles ↔ samples
    (Signal::Profiles, Signal::Samples, "profile_id"),
    (Signal::Samples, Signal::Profiles, "profile_id"),
    // stack_id: samples ↔ stacks
    (Signal::Samples, Signal::Stacks, "stack_id"),
    (Signal::Stacks, Signal::Samples, "stack_id"),
    // frame_id: stacks ↔ frames (scalar join — stacks is now a junction table)
    (Signal::Stacks, Signal::Frames, "frame_id"),
    (Signal::Frames, Signal::Stacks, "frame_id"),
    // mapping_id: frames ↔ mappings
    (Signal::Frames, Signal::Mappings, "mapping_id"),
    (Signal::Mappings, Signal::Frames, "mapping_id"),
    // ═══ RESOURCE RELATIONSHIPS (resource_id) ═══
    (Signal::Spans, Signal::Resources, "resource_id"),
    (Signal::Logs, Signal::Resources, "resource_id"),
    (Signal::Metrics, Signal::Resources, "resource_id"),
    (Signal::Datapoints, Signal::Resources, "resource_id"),
    (Signal::Histograms, Signal::Resources, "resource_id"),
    (Signal::Profiles, Signal::Resources, "resource_id"),
    (Signal::Samples, Signal::Resources, "resource_id"),
    (Signal::Resources, Signal::Spans, "resource_id"),
    (Signal::Resources, Signal::Logs, "resource_id"),
    (Signal::Resources, Signal::Metrics, "resource_id"),
    (Signal::Resources, Signal::Datapoints, "resource_id"),
    (Signal::Resources, Signal::Histograms, "resource_id"),
    (Signal::Resources, Signal::Profiles, "resource_id"),
    (Signal::Resources, Signal::Samples, "resource_id"),
    // ═══ SCOPE RELATIONSHIPS (scope_id) ═══
    (Signal::Spans, Signal::Scopes, "scope_id"),
    (Signal::Logs, Signal::Scopes, "scope_id"),
    (Signal::Metrics, Signal::Scopes, "scope_id"),
    (Signal::Datapoints, Signal::Scopes, "scope_id"),
    (Signal::Histograms, Signal::Scopes, "scope_id"),
    (Signal::Profiles, Signal::Scopes, "scope_id"),
    (Signal::Samples, Signal::Scopes, "scope_id"),
    (Signal::Scopes, Signal::Spans, "scope_id"),
    (Signal::Scopes, Signal::Logs, "scope_id"),
    (Signal::Scopes, Signal::Metrics, "scope_id"),
    (Signal::Scopes, Signal::Datapoints, "scope_id"),
    (Signal::Scopes, Signal::Histograms, "scope_id"),
    (Signal::Scopes, Signal::Profiles, "scope_id"),
    (Signal::Scopes, Signal::Samples, "scope_id"),
];

/// Get the join key for navigate operations.
///
/// Navigate replaces the current row set with rows from a different signal type,
/// resolved by a shared correlation key.
pub fn navigate_join_key(from: &Signal, to: &Signal) -> Option<&'static str> {
    match (from, to) {
        (Signal::Logs, Signal::Spans) => Some("trace_id"),
        (Signal::Logs, Signal::Traces) => Some("trace_id"),
        (Signal::Spans, Signal::Traces) => Some("trace_id"),
        (Signal::Datapoints, Signal::Metrics) => Some("metric_id"),
        _ => None,
    }
}

/// Get the join key for merge operations.
///
/// Merge augments each row with correlated rows from another signal as a
/// flat auxiliary table in the response stream.
pub fn merge_join_key(from: &Signal, to: &Signal) -> Option<&'static str> {
    MERGE_PATHS
        .iter()
        .find(|(f, t, _)| f == from && t == to)
        .map(|(_, _, key)| *key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn navigate_spans_to_traces() {
        assert_eq!(
            navigate_join_key(&Signal::Spans, &Signal::Traces),
            Some("trace_id")
        );
    }

    #[test]
    fn navigate_logs_to_spans() {
        assert_eq!(
            navigate_join_key(&Signal::Logs, &Signal::Spans),
            Some("trace_id")
        );
    }

    #[test]
    fn navigate_datapoints_to_metrics() {
        assert_eq!(
            navigate_join_key(&Signal::Datapoints, &Signal::Metrics),
            Some("metric_id")
        );
    }

    #[test]
    fn navigate_invalid_path_returns_none() {
        assert_eq!(navigate_join_key(&Signal::Spans, &Signal::Datapoints), None);
        assert_eq!(navigate_join_key(&Signal::Logs, &Signal::Profiles), None);
    }

    #[test]
    fn merge_spans_to_logs() {
        assert_eq!(
            merge_join_key(&Signal::Spans, &Signal::Logs),
            Some("trace_id")
        );
    }

    #[test]
    fn merge_metrics_to_datapoints() {
        assert_eq!(
            merge_join_key(&Signal::Metrics, &Signal::Datapoints),
            Some("metric_id")
        );
    }

    #[test]
    fn merge_samples_to_stacks() {
        assert_eq!(
            merge_join_key(&Signal::Samples, &Signal::Stacks),
            Some("stack_id")
        );
    }

    #[test]
    fn merge_stacks_to_frames_is_scalar() {
        // After stacks normalization, this is a scalar join on frame_id (not a list join)
        assert_eq!(
            merge_join_key(&Signal::Stacks, &Signal::Frames),
            Some("frame_id")
        );
    }

    #[test]
    fn merge_invalid_path_returns_none() {
        assert_eq!(merge_join_key(&Signal::Spans, &Signal::Samples), None);
        assert_eq!(merge_join_key(&Signal::Logs, &Signal::Datapoints), None);
    }

    #[test]
    fn merge_paths_are_symmetric_for_navigable_types() {
        // Traces ↔ Spans both directions
        assert!(merge_join_key(&Signal::Spans, &Signal::Traces).is_some());
        assert!(merge_join_key(&Signal::Traces, &Signal::Spans).is_some());
    }
}
