//! Helper utilities for cold tier storage

use sequins_types::models::Timestamp;

pub(crate) fn generate_partition_path(telemetry_type: &str, timestamp: &Timestamp) -> String {
    // Format: {type}/year=YYYY/month=MM/day=DD/{timestamp}.vortex

    // Use chrono to properly convert timestamp to date components
    use chrono::{Datelike, TimeZone, Utc};

    let secs = timestamp.as_secs();
    let nanos = (timestamp.as_nanos() % 1_000_000_000) as u32;
    let dt = Utc.timestamp_opt(secs, nanos).unwrap();

    let year = dt.year();
    let month = dt.month();
    let day = dt.day();

    format!(
        "{}/year={}/month={:02}/day={:02}/{}.vortex",
        telemetry_type,
        year,
        month,
        day,
        timestamp.as_nanos()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cold_tier::ColdTier;
    use sequins_types::models::Timestamp;

    #[test]
    fn test_generate_partition_path_format() {
        let timestamp = Timestamp::from_secs(1705328400); // 2024-01-15
        let path = generate_partition_path("traces", &timestamp);

        // Should follow format: {type}/year=YYYY/month=MM/day=DD/{timestamp}.vortex
        assert!(path.starts_with("traces/year=2024/month=01/day=15/"));
        assert!(path.ends_with(".vortex"));
    }

    #[test]
    fn test_generate_metric_partition_path_hour_buckets() {
        // Test different hour buckets using ColdTier methods
        let ts_hour_0 = Timestamp::from_secs(1705276800); // 2024-01-15 00:00:00
        let ts_hour_3 = Timestamp::from_secs(1705287600); // 2024-01-15 03:00:00
        let ts_hour_14 = Timestamp::from_secs(1705328400); // 2024-01-15 14:00:00

        let path_0 = ColdTier::generate_metric_partition_path("metric", &ts_hour_0);
        let path_3 = ColdTier::generate_metric_partition_path("metric", &ts_hour_3);
        let path_14 = ColdTier::generate_metric_partition_path("metric", &ts_hour_14);

        // Hour 0 and 3 should both be in bucket 0 and 2 respectively
        assert!(path_0.contains("/00/"));
        assert!(path_3.contains("/02/"));
        assert!(path_14.contains("/14/"));
    }
}
