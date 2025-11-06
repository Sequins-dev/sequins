use crate::config::HotTierConfig;
use crate::error::Result;
use papaya::HashMap as PapayaHashMap;
use sequins_core::models::{
    LogEntry, LogId, LogQuery, Metric, MetricId, MetricQuery, Profile, ProfileId, ProfileQuery,
    QueryTrace, Service, Span, SpanId, Timestamp, TraceId, TraceQuery,
};
use std::collections::HashMap;

/// Hot tier (in-memory) storage using Papaya lock-free HashMap
pub struct HotTier {
    config: HotTierConfig,
    spans: PapayaHashMap<SpanId, TimestampedEntry<Span>>,
    logs: PapayaHashMap<LogId, TimestampedEntry<LogEntry>>,
    metrics: PapayaHashMap<MetricId, TimestampedEntry<Metric>>,
    profiles: PapayaHashMap<ProfileId, TimestampedEntry<Profile>>,
}

/// Entry with timestamp for age-based eviction
#[derive(Debug, Clone)]
struct TimestampedEntry<T> {
    data: T,
    inserted_at: Timestamp,
}

impl HotTier {
    /// Create new hot tier storage
    pub fn new(config: HotTierConfig) -> Self {
        Self {
            config,
            spans: PapayaHashMap::new(),
            logs: PapayaHashMap::new(),
            metrics: PapayaHashMap::new(),
            profiles: PapayaHashMap::new(),
        }
    }

    /// Insert a span
    pub fn insert_span(&self, span: Span) -> Result<()> {
        let inserted_at = Timestamp::now().map_err(|e| {
            crate::error::Error::Storage(format!("Failed to get current time: {}", e))
        })?;

        let entry = TimestampedEntry {
            data: span.clone(),
            inserted_at,
        };

        let pin = self.spans.pin();
        pin.insert(span.span_id, entry);
        Ok(())
    }

    /// Get a span by trace ID and span ID
    ///
    /// Span IDs are only unique within a trace, so both IDs are required
    pub fn get_span(&self, trace_id: &TraceId, span_id: &SpanId) -> Option<Span> {
        let pin = self.spans.pin();
        pin.get(span_id).and_then(|entry| {
            // Verify the span belongs to the requested trace
            if &entry.data.trace_id == trace_id {
                Some(entry.data.clone())
            } else {
                None
            }
        })
    }

    /// Get all spans for a trace
    pub fn get_trace_spans(&self, trace_id: &TraceId) -> Vec<Span> {
        let pin = self.spans.pin();
        let mut spans = Vec::new();

        for entry in pin.iter() {
            if &entry.1.data.trace_id == trace_id {
                spans.push(entry.1.data.clone());
            }
        }

        spans
    }

    /// Insert a log entry
    pub fn insert_log(&self, log: LogEntry) -> Result<()> {
        let inserted_at = Timestamp::now().map_err(|e| {
            crate::error::Error::Storage(format!("Failed to get current time: {}", e))
        })?;

        let entry = TimestampedEntry {
            data: log.clone(),
            inserted_at,
        };

        let pin = self.logs.pin();
        pin.insert(log.id, entry);
        Ok(())
    }

    /// Get a log entry by ID
    pub fn get_log(&self, log_id: &LogId) -> Option<LogEntry> {
        let pin = self.logs.pin();
        pin.get(log_id).map(|entry| entry.data.clone())
    }

    /// Query logs by time range and optional trace context
    pub fn query_logs(
        &self,
        start: Timestamp,
        end: Timestamp,
        trace_id: Option<&TraceId>,
    ) -> Vec<LogEntry> {
        let pin = self.logs.pin();
        let mut logs = Vec::new();

        for entry in pin.iter() {
            let log = &entry.1.data;

            // Check time range
            if log.timestamp < start || log.timestamp > end {
                continue;
            }

            // Check trace filter if provided
            if let Some(tid) = trace_id {
                if log.trace_id.as_ref() != Some(tid) {
                    continue;
                }
            }

            logs.push(log.clone());
        }

        logs
    }

    /// Insert a metric
    pub fn insert_metric(&self, metric: Metric) -> Result<()> {
        let inserted_at = Timestamp::now().map_err(|e| {
            crate::error::Error::Storage(format!("Failed to get current time: {}", e))
        })?;

        let entry = TimestampedEntry {
            data: metric.clone(),
            inserted_at,
        };

        let pin = self.metrics.pin();
        pin.insert(metric.id, entry);
        Ok(())
    }

    /// Get a metric by ID
    pub fn get_metric(&self, metric_id: &MetricId) -> Option<Metric> {
        let pin = self.metrics.pin();
        pin.get(metric_id).map(|entry| entry.data.clone())
    }

    /// Query metrics by name
    pub fn query_metrics(&self, name: Option<&str>) -> Vec<Metric> {
        let pin = self.metrics.pin();
        let mut metrics = Vec::new();

        for entry in pin.iter() {
            let metric = &entry.1.data;

            // Check name filter if provided
            if let Some(n) = name {
                if metric.name != n {
                    continue;
                }
            }

            metrics.push(metric.clone());
        }

        metrics
    }

    /// Insert a profile
    pub fn insert_profile(&self, profile: Profile) -> Result<()> {
        let inserted_at = Timestamp::now().map_err(|e| {
            crate::error::Error::Storage(format!("Failed to get current time: {}", e))
        })?;

        let entry = TimestampedEntry {
            data: profile.clone(),
            inserted_at,
        };

        let pin = self.profiles.pin();
        pin.insert(profile.id, entry);
        Ok(())
    }

    /// Get a profile by ID
    pub fn get_profile(&self, profile_id: &ProfileId) -> Option<Profile> {
        let pin = self.profiles.pin();
        pin.get(profile_id).map(|entry| entry.data.clone())
    }

    /// Evict old entries based on age and count limits
    pub fn evict_old_entries(&self) -> Result<EvictionStats> {
        let now = Timestamp::now().map_err(|e| {
            crate::error::Error::Storage(format!("Failed to get current time: {}", e))
        })?;

        let max_age = self.config.max_age;
        let cutoff_time = now - max_age;

        let stats = EvictionStats {
            spans_evicted: self.evict_old_entries_from_map(&self.spans, cutoff_time),
            logs_evicted: self.evict_old_entries_from_map(&self.logs, cutoff_time),
            metrics_evicted: self.evict_old_entries_from_map(&self.metrics, cutoff_time),
            profiles_evicted: self.evict_old_entries_from_map(&self.profiles, cutoff_time),
        };

        Ok(stats)
    }

    /// Evict old entries from a specific map
    fn evict_old_entries_from_map<K, V>(
        &self,
        map: &PapayaHashMap<K, TimestampedEntry<V>>,
        cutoff_time: Timestamp,
    ) -> usize
    where
        K: Clone + Eq + std::hash::Hash,
    {
        let pin = map.pin();
        let mut to_remove = Vec::new();

        for entry in pin.iter() {
            if entry.1.inserted_at < cutoff_time {
                to_remove.push(entry.0.clone());
            }
        }

        let count = to_remove.len();
        for key in to_remove {
            pin.remove(&key);
        }

        count
    }

    /// Get current storage statistics
    pub fn stats(&self) -> StorageStats {
        let spans_pin = self.spans.pin();
        let logs_pin = self.logs.pin();
        let metrics_pin = self.metrics.pin();
        let profiles_pin = self.profiles.pin();

        StorageStats {
            span_count: spans_pin.len(),
            log_count: logs_pin.len(),
            metric_count: metrics_pin.len(),
            profile_count: profiles_pin.len(),
        }
    }

    /// Clear all data
    pub fn clear(&self) {
        let spans_pin = self.spans.pin();
        spans_pin.clear();

        let logs_pin = self.logs.pin();
        logs_pin.clear();

        let metrics_pin = self.metrics.pin();
        metrics_pin.clear();

        let profiles_pin = self.profiles.pin();
        profiles_pin.clear();
    }

    /// Get trace IDs for spans where data is older than the given threshold
    pub fn get_old_trace_ids(&self, cutoff_time: Timestamp) -> Vec<TraceId> {
        let pin = self.spans.pin();
        let mut trace_ids = std::collections::HashSet::new();

        for entry in pin.iter() {
            let span = &entry.1.data;
            // Use the span's end_time to determine age
            if span.end_time < cutoff_time {
                trace_ids.insert(span.trace_id);
            }
        }

        trace_ids.into_iter().collect()
    }

    /// Get logs older than the given timestamp
    pub fn get_old_logs(&self, cutoff_time: Timestamp) -> Vec<LogEntry> {
        let pin = self.logs.pin();
        let mut logs = Vec::new();

        for entry in pin.iter() {
            let log = &entry.1.data;
            if log.timestamp < cutoff_time {
                logs.push(log.clone());
            }
        }

        logs
    }

    /// Get metric names for metrics inserted before the given time
    pub fn get_old_metric_names(&self, cutoff_time: Timestamp) -> Vec<String> {
        let pin = self.metrics.pin();
        let mut names = std::collections::HashSet::new();

        for entry in pin.iter() {
            // Use insertion time since metrics don't have their own timestamp
            if entry.1.inserted_at < cutoff_time {
                names.insert(entry.1.data.name.clone());
            }
        }

        names.into_iter().collect()
    }

    /// Get profiles older than the given timestamp
    pub fn get_old_profiles(&self, cutoff_time: Timestamp) -> Vec<Profile> {
        let pin = self.profiles.pin();
        let mut profiles = Vec::new();

        for entry in pin.iter() {
            let profile = &entry.1.data;
            if profile.timestamp < cutoff_time {
                profiles.push(profile.clone());
            }
        }

        profiles
    }

    /// Get list of services with counts
    pub fn get_services(&self) -> Vec<Service> {
        let mut service_map: HashMap<String, (usize, usize)> = HashMap::new();

        // Count spans per service
        let spans_pin = self.spans.pin();
        for entry in spans_pin.iter() {
            let span = &entry.1.data;
            let (span_count, _) = service_map
                .entry(span.service_name.clone())
                .or_insert((0, 0));
            *span_count += 1;
        }

        // Count logs per service
        let logs_pin = self.logs.pin();
        for entry in logs_pin.iter() {
            let log = &entry.1.data;
            let (_, log_count) = service_map
                .entry(log.service_name.clone())
                .or_insert((0, 0));
            *log_count += 1;
        }

        // Convert to Service structs
        service_map
            .into_iter()
            .map(|(name, (span_count, log_count))| Service {
                name,
                span_count,
                log_count,
            })
            .collect()
    }

    /// Query traces with filtering
    pub fn query_traces(&self, query: &TraceQuery) -> Vec<QueryTrace> {
        let pin = self.spans.pin();
        let mut trace_map: HashMap<TraceId, Vec<Span>> = HashMap::new();

        // Group spans by trace_id
        for entry in pin.iter() {
            let span = &entry.1.data;

            // Apply service filter
            if let Some(ref service) = query.service {
                if &span.service_name != service {
                    continue;
                }
            }

            // Apply time range filter
            if span.start_time < query.start_time || span.end_time > query.end_time {
                continue;
            }

            trace_map
                .entry(span.trace_id)
                .or_default()
                .push(span.clone());
        }

        // Convert to QueryTrace and apply additional filters
        let mut traces: Vec<QueryTrace> = trace_map
            .into_iter()
            .filter_map(|(trace_id, spans)| {
                // Find root span
                let root_span = spans.iter().find(|s| s.parent_span_id.is_none())?;

                // Calculate total duration
                let duration = root_span.duration.as_nanos();

                // Apply duration filters
                if let Some(min) = query.min_duration {
                    if duration < min {
                        return None;
                    }
                }
                if let Some(max) = query.max_duration {
                    if duration > max {
                        return None;
                    }
                }

                // Check for errors
                let has_error = spans.iter().any(|s| s.has_error());

                // Apply error filter
                if let Some(filter_error) = query.has_error {
                    if has_error != filter_error {
                        return None;
                    }
                }

                Some(QueryTrace {
                    trace_id,
                    root_span_id: root_span.span_id,
                    spans,
                    duration,
                    has_error,
                })
            })
            .collect();

        // Apply limit
        if let Some(limit) = query.limit {
            traces.truncate(limit);
        }

        traces
    }

    /// Query logs with filtering
    pub fn query_logs_filtered(&self, query: &LogQuery) -> Vec<LogEntry> {
        let pin = self.logs.pin();
        let mut logs = Vec::new();

        for entry in pin.iter() {
            let log = &entry.1.data;

            // Apply service filter
            if let Some(ref service) = query.service {
                if &log.service_name != service {
                    continue;
                }
            }

            // Apply time range filter
            if log.timestamp < query.start_time || log.timestamp > query.end_time {
                continue;
            }

            // Apply severity filter
            if let Some(ref severity) = query.severity {
                if log.severity.as_str() != severity {
                    continue;
                }
            }

            // Apply search filter
            if let Some(ref search) = query.search {
                if !log.body.contains(search) {
                    continue;
                }
            }

            // Apply trace_id filter
            if let Some(ref tid) = query.trace_id {
                if log.trace_id.as_ref() != Some(tid) {
                    continue;
                }
            }

            logs.push(log.clone());
        }

        // Apply limit
        if let Some(limit) = query.limit {
            logs.truncate(limit);
        }

        logs
    }

    /// Query metrics with filtering
    pub fn query_metrics_filtered(&self, query: &MetricQuery) -> Vec<Metric> {
        let pin = self.metrics.pin();
        let mut metrics = Vec::new();

        for entry in pin.iter() {
            let metric = &entry.1.data;

            // Apply name filter
            if let Some(ref name) = query.name {
                if &metric.name != name {
                    continue;
                }
            }

            // Apply service filter
            if let Some(ref service) = query.service {
                if &metric.service_name != service {
                    continue;
                }
            }

            // Note: Metrics don't have their own timestamp in the Metric struct
            // We'd need to use inserted_at if we want time filtering, but the query
            // struct expects start_time/end_time. For now, we'll skip time filtering
            // on metrics since they don't have a timestamp field.

            metrics.push(metric.clone());
        }

        // Apply limit
        if let Some(limit) = query.limit {
            metrics.truncate(limit);
        }

        metrics
    }

    /// Query profiles with filtering
    pub fn query_profiles(&self, query: &ProfileQuery) -> Vec<Profile> {
        let pin = self.profiles.pin();
        let mut profiles = Vec::new();

        for entry in pin.iter() {
            let profile = &entry.1.data;

            // Apply service filter
            if let Some(ref service) = query.service {
                if &profile.service_name != service {
                    continue;
                }
            }

            // Apply profile_type filter
            if let Some(ref ptype) = query.profile_type {
                if profile.profile_type.as_str() != ptype {
                    continue;
                }
            }

            // Apply time range filter
            if profile.timestamp < query.start_time || profile.timestamp > query.end_time {
                continue;
            }

            // Apply trace_id filter
            if let Some(ref tid) = query.trace_id {
                if profile.trace_id.as_ref() != Some(tid) {
                    continue;
                }
            }

            profiles.push(profile.clone());
        }

        // Apply limit
        if let Some(limit) = query.limit {
            profiles.truncate(limit);
        }

        profiles
    }

    /// Get all spans (prototype helper for MemTable testing)
    ///
    /// **WARNING**: This is a prototype method and will be removed in Phase 2.
    /// This is only used for Phase 1 prototype and will be removed
    /// once the custom TableProvider is implemented. Do not use in production.
    pub fn get_all_spans(&self) -> Vec<Span> {
        let pin = self.spans.pin();
        pin.iter().map(|entry| entry.1.data.clone()).collect()
    }
}

/// Statistics about evicted entries
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EvictionStats {
    pub spans_evicted: usize,
    pub logs_evicted: usize,
    pub metrics_evicted: usize,
    pub profiles_evicted: usize,
}

impl EvictionStats {
    pub fn total(&self) -> usize {
        self.spans_evicted + self.logs_evicted + self.metrics_evicted + self.profiles_evicted
    }
}

/// Current storage statistics
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageStats {
    pub span_count: usize,
    pub log_count: usize,
    pub metric_count: usize,
    pub profile_count: usize,
}

impl StorageStats {
    pub fn total(&self) -> usize {
        self.span_count + self.log_count + self.metric_count + self.profile_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sequins_core::models::{
        Duration, LogSeverity, MetricType, ProfileType, SpanKind, SpanStatus, TraceId,
    };
    use std::collections::HashMap;

    fn create_test_config() -> HotTierConfig {
        HotTierConfig {
            max_age: Duration::from_minutes(5),
            max_entries: 1000,
        }
    }

    fn create_test_span(trace_id: TraceId, span_id: SpanId) -> Span {
        let start = Timestamp::now().unwrap();
        let end = start + Duration::from_secs(1);
        Span {
            trace_id,
            span_id,
            parent_span_id: None,
            service_name: "test-service".to_string(),
            operation_name: "test-op".to_string(),
            start_time: start,
            end_time: end,
            duration: Duration::from_secs(1),
            attributes: HashMap::new(),
            events: Vec::new(),
            span_kind: SpanKind::Internal,
            status: SpanStatus::Ok,
        }
    }

    fn create_test_log() -> LogEntry {
        LogEntry {
            id: LogId::new(),
            timestamp: Timestamp::now().unwrap(),
            observed_timestamp: Timestamp::now().unwrap(),
            service_name: "test-service".to_string(),
            severity: LogSeverity::Info,
            body: "Test log".to_string(),
            attributes: HashMap::new(),
            trace_id: None,
            span_id: None,
            resource: HashMap::new(),
        }
    }

    fn create_test_metric() -> Metric {
        Metric {
            id: MetricId::new(),
            name: "test.metric".to_string(),
            description: "Test metric".to_string(),
            unit: "ms".to_string(),
            service_name: "test-service".to_string(),
            metric_type: MetricType::Gauge,
        }
    }

    fn create_test_profile() -> Profile {
        Profile {
            id: ProfileId::new(),
            timestamp: Timestamp::now().unwrap(),
            service_name: "test-service".to_string(),
            profile_type: ProfileType::Cpu,
            sample_type: "samples".to_string(),
            sample_unit: "count".to_string(),
            data: vec![1, 2, 3],
            trace_id: None,
        }
    }

    #[test]
    fn test_hot_tier_new() {
        let config = create_test_config();
        let hot_tier = HotTier::new(config);

        let stats = hot_tier.stats();
        assert_eq!(stats.total(), 0);
    }

    #[test]
    fn test_insert_and_get_span() {
        let hot_tier = HotTier::new(create_test_config());
        let trace_id = TraceId::from_bytes([1; 16]);
        let span_id = SpanId::from_bytes([1; 8]);
        let span = create_test_span(trace_id, span_id);

        hot_tier.insert_span(span.clone()).unwrap();

        let retrieved = hot_tier.get_span(&trace_id, &span_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().span_id, span_id);
    }

    #[test]
    fn test_get_trace_spans() {
        let hot_tier = HotTier::new(create_test_config());
        let trace_id = TraceId::from_bytes([1; 16]);

        // Insert multiple spans with the same trace ID
        for i in 0..3 {
            let span_id = SpanId::from_bytes([i; 8]);
            let span = create_test_span(trace_id, span_id);
            hot_tier.insert_span(span).unwrap();
        }

        let spans = hot_tier.get_trace_spans(&trace_id);
        assert_eq!(spans.len(), 3);
    }

    #[test]
    fn test_insert_and_get_log() {
        let hot_tier = HotTier::new(create_test_config());
        let log = create_test_log();
        let log_id = log.id;

        hot_tier.insert_log(log).unwrap();

        let retrieved = hot_tier.get_log(&log_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, log_id);
    }

    #[test]
    fn test_query_logs() {
        let hot_tier = HotTier::new(create_test_config());
        let now = Timestamp::now().unwrap();
        let start = now - Duration::from_minutes(1);
        let end = now + Duration::from_minutes(1);

        // Insert a log within the time range
        let log = create_test_log();
        hot_tier.insert_log(log).unwrap();

        let logs = hot_tier.query_logs(start, end, None);
        assert_eq!(logs.len(), 1);
    }

    #[test]
    fn test_insert_and_get_metric() {
        let hot_tier = HotTier::new(create_test_config());
        let metric = create_test_metric();
        let metric_id = metric.id;

        hot_tier.insert_metric(metric).unwrap();

        let retrieved = hot_tier.get_metric(&metric_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, metric_id);
    }

    #[test]
    fn test_query_metrics() {
        let hot_tier = HotTier::new(create_test_config());

        let metric = create_test_metric();
        hot_tier.insert_metric(metric.clone()).unwrap();

        // Query all metrics
        let metrics = hot_tier.query_metrics(None);
        assert_eq!(metrics.len(), 1);

        // Query by name
        let metrics = hot_tier.query_metrics(Some("test.metric"));
        assert_eq!(metrics.len(), 1);

        // Query by non-existent name
        let metrics = hot_tier.query_metrics(Some("nonexistent"));
        assert_eq!(metrics.len(), 0);
    }

    #[test]
    fn test_insert_and_get_profile() {
        let hot_tier = HotTier::new(create_test_config());
        let profile = create_test_profile();
        let profile_id = profile.id;

        hot_tier.insert_profile(profile).unwrap();

        let retrieved = hot_tier.get_profile(&profile_id);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, profile_id);
    }

    #[test]
    fn test_stats() {
        let hot_tier = HotTier::new(create_test_config());

        let trace_id = TraceId::from_bytes([1; 16]);
        let span_id = SpanId::from_bytes([1; 8]);
        hot_tier
            .insert_span(create_test_span(trace_id, span_id))
            .unwrap();
        hot_tier.insert_log(create_test_log()).unwrap();
        hot_tier.insert_metric(create_test_metric()).unwrap();
        hot_tier.insert_profile(create_test_profile()).unwrap();

        let stats = hot_tier.stats();
        assert_eq!(stats.span_count, 1);
        assert_eq!(stats.log_count, 1);
        assert_eq!(stats.metric_count, 1);
        assert_eq!(stats.profile_count, 1);
        assert_eq!(stats.total(), 4);
    }

    #[test]
    fn test_clear() {
        let hot_tier = HotTier::new(create_test_config());

        let trace_id = TraceId::from_bytes([1; 16]);
        let span_id = SpanId::from_bytes([1; 8]);
        hot_tier
            .insert_span(create_test_span(trace_id, span_id))
            .unwrap();
        hot_tier.insert_log(create_test_log()).unwrap();

        hot_tier.clear();

        let stats = hot_tier.stats();
        assert_eq!(stats.total(), 0);
    }

    #[test]
    fn test_eviction_stats() {
        let stats = EvictionStats {
            spans_evicted: 10,
            logs_evicted: 20,
            metrics_evicted: 5,
            profiles_evicted: 3,
        };

        assert_eq!(stats.total(), 38);
    }
}
