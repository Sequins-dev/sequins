//! Signal type enum for hot tier providers

use arrow::datatypes::SchemaRef;

use crate::arrow_schema;

/// Signal types supported by hot tier storage
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalType {
    Spans,
    Logs,
    Metrics,
    MetricsMetadata,
    Histograms,
    ExpHistograms,
    ProfilesMetadata,
    ProfileFrames,
    ProfileStacks,
    ProfileSamples,
    ProfileMappings,
    Resources,
    Scopes,
    SpanLinks,
    SpanEvents,
}

impl SignalType {
    /// Get the Arrow schema for this signal type
    pub fn schema(&self) -> SchemaRef {
        match self {
            Self::Spans => arrow_schema::span_schema(),
            Self::Logs => arrow_schema::log_schema(),
            Self::Metrics => arrow_schema::series_data_point_schema(),
            Self::MetricsMetadata => arrow_schema::metric_schema(),
            Self::Histograms => arrow_schema::histogram_series_data_point_schema(),
            Self::ExpHistograms => arrow_schema::exp_histogram_data_point_schema(),
            Self::ProfilesMetadata => arrow_schema::profile_schema(),
            Self::ProfileFrames => arrow_schema::profile_frames_schema(),
            Self::ProfileStacks => arrow_schema::profile_stacks_schema(),
            Self::ProfileSamples => arrow_schema::profile_samples_schema(),
            Self::ProfileMappings => arrow_schema::profile_mappings_schema(),
            Self::Resources => arrow_schema::resource_schema(),
            Self::Scopes => arrow_schema::scope_schema(),
            Self::SpanLinks => arrow_schema::span_links_schema(),
            Self::SpanEvents => arrow_schema::span_events_schema(),
        }
    }

    /// Get a display name for this signal type
    pub fn name(&self) -> &'static str {
        match self {
            Self::Spans => "spans",
            Self::Logs => "logs",
            Self::Metrics => "metrics",
            Self::MetricsMetadata => "metrics_metadata",
            Self::Histograms => "histograms",
            Self::ExpHistograms => "exp_histograms",
            Self::ProfilesMetadata => "profiles_metadata",
            Self::ProfileFrames => "profile_frames",
            Self::ProfileStacks => "profile_stacks",
            Self::ProfileSamples => "profile_samples",
            Self::ProfileMappings => "profile_mappings",
            Self::Resources => "resources",
            Self::Scopes => "scopes",
            Self::SpanLinks => "span_links",
            Self::SpanEvents => "span_events",
        }
    }

    /// Get the time column name for signals that support time range filtering
    pub fn time_column(&self) -> Option<&'static str> {
        match self {
            Self::Spans => Some("start_time_unix_nano"),
            Self::Logs | Self::Metrics | Self::Histograms | Self::ExpHistograms => {
                Some("time_unix_nano")
            }
            _ => None,
        }
    }

    /// All variants in a fixed order, useful for iteration in tests.
    pub fn all() -> &'static [SignalType] {
        &[
            SignalType::Spans,
            SignalType::Logs,
            SignalType::Metrics,
            SignalType::MetricsMetadata,
            SignalType::Histograms,
            SignalType::ExpHistograms,
            SignalType::ProfilesMetadata,
            SignalType::ProfileFrames,
            SignalType::ProfileStacks,
            SignalType::ProfileSamples,
            SignalType::ProfileMappings,
            SignalType::Resources,
            SignalType::Scopes,
            SignalType::SpanLinks,
            SignalType::SpanEvents,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_all_variants_have_schema() {
        for signal in SignalType::all() {
            let schema = signal.schema();
            assert!(
                !schema.fields().is_empty(),
                "{:?} schema has no fields",
                signal
            );
        }
    }

    #[test]
    fn test_all_variants_have_unique_name() {
        let names: Vec<&str> = SignalType::all().iter().map(|s| s.name()).collect();
        let mut unique = names.clone();
        unique.sort_unstable();
        unique.dedup();
        assert_eq!(unique.len(), names.len(), "signal names are not unique");
        for name in &names {
            assert!(!name.is_empty(), "signal name must not be empty");
        }
    }

    #[test]
    fn test_time_column_present_for_time_bearing_signals() {
        let time_bearing = [
            SignalType::Spans,
            SignalType::Logs,
            SignalType::Metrics,
            SignalType::Histograms,
            SignalType::ExpHistograms,
        ];
        for signal in &time_bearing {
            let col = signal.time_column();
            assert!(col.is_some(), "{:?} should have a time column", signal);
            let schema = signal.schema();
            assert!(
                schema.column_with_name(col.unwrap()).is_some(),
                "{:?} time column '{}' not found in schema",
                signal,
                col.unwrap()
            );
        }
    }

    #[test]
    fn test_time_column_none_for_non_time_signals() {
        let non_time = [
            SignalType::Resources,
            SignalType::Scopes,
            SignalType::ProfilesMetadata,
            SignalType::ProfileFrames,
            SignalType::ProfileStacks,
            SignalType::ProfileMappings,
        ];
        for signal in &non_time {
            assert!(
                signal.time_column().is_none(),
                "{:?} should not have a time column",
                signal
            );
        }
    }
}
