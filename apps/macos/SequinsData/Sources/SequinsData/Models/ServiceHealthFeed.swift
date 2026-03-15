//
//  ServiceHealthFeed.swift
//  SequinsData
//
//  An @Observable live feed for service health, driven by two aggregation queries
//  (span stats + error log count). Accepts RecordBatch payloads from the app's
//  live stream callbacks and owns all Arrow parsing, metric extraction, and
//  HealthAnalyzer invocation.
//
//  Column layout expected from the span aggregation query:
//    group by {} {
//      count() where status == 2 as error_count,                     // 0
//      count() as total,                                              // 1
//      p50(duration_ns) as p50,                                       // 2
//      p95(duration_ns) as p95,                                       // 3
//      p99(duration_ns) as p99,                                       // 4
//      count() where attr.http_status_code >= 500 as http_5xx,       // 5
//      count() where attr.http_status_code > 0 as http_total         // 6
//    }
//
//  Column layout expected from the log aggregation query:
//    group by {} { count() as error_logs }                            // 0
//

import Foundation

/// An `@Observable` service health feed populated by two live aggregation queries.
///
/// Created fresh by `HealthViewModel` whenever the selected service or time range changes.
/// The view observes this feed directly; each property update triggers granular re-renders
/// of only the views that read that property.
@Observable
public final class ServiceHealthFeed {
    // ── Public state ──────────────────────────────────────────────────────────

    /// Computed metric values keyed by `HealthMetricNames` constants.
    public private(set) var healthMetricValues: [String: Double] = [:]

    /// Full health analysis computed once both streams have delivered data.
    /// Nil until both span and log results have arrived.
    public private(set) var healthAnalysis: HealthAnalysis?

    /// True until both streams have delivered their first result.
    public private(set) var isLoading: Bool = true

    /// Metric values from the comparison period. Nil until a comparison query has completed.
    public private(set) var previousMetricValues: [String: Double]? = nil

    // ── Private state ─────────────────────────────────────────────────────────

    private var spanMetrics: [String: Double] = [:]
    private var logMetrics: [String: Double] = [:]
    private var spanDataReady = false
    private var logDataReady = false

    private let analyzer: HealthAnalyzer
    private let serviceName: String
    private let timeWindowMinutes: Double

    // MARK: - Initialisation

    public init(
        serviceName: String,
        timeWindowMinutes: Double,
        config: HealthThresholdConfig = .default
    ) {
        self.serviceName = serviceName
        self.timeWindowMinutes = timeWindowMinutes
        self.analyzer = HealthAnalyzer(config: config)
    }

    // MARK: - Error handling

    /// Call this when the live query failed to start — clears the loading spinner.
    public func markFailed() {
        isLoading = false
    }

    // MARK: - Comparison batch ingestion

    /// Apply a span batch from the comparison period query.
    /// Expected schema: error_count, total, p50, p95, p99, http_5xx, http_total (all 1 row).
    public func applyComparisonSpanBatch(_ batch: RecordBatch, timeWindowMinutes: Double) {
        let rows = batch.toRows()
        guard let row = rows.first else { return }

        let errorCount = asDouble(row[safe: 0]) ?? 0
        let total      = asDouble(row[safe: 1]) ?? 0

        var metrics: [String: Double] = [:]
        if total > 0 {
            metrics[HealthMetricNames.spanErrorRate] = errorCount / total
            metrics[HealthMetricNames.latencyP50]    = asDouble(row[safe: 2]) ?? 0
            metrics[HealthMetricNames.latencyP95]    = asDouble(row[safe: 3]) ?? 0
            metrics[HealthMetricNames.latencyP99]    = asDouble(row[safe: 4]) ?? 0
            metrics[HealthMetricNames.throughput]    = total / timeWindowMinutes

            let http5xx   = asDouble(row[safe: 5]) ?? 0
            let httpTotal = asDouble(row[safe: 6]) ?? 0
            if httpTotal > 0 {
                metrics[HealthMetricNames.httpErrorRate] = http5xx / httpTotal
            }
        }

        var combined = previousMetricValues ?? [:]
        for (k, v) in metrics { combined[k] = v }
        previousMetricValues = combined
    }

    /// Apply a log batch from the comparison period query.
    /// Expected schema: error_logs (1 row).
    public func applyComparisonLogBatch(_ batch: RecordBatch, timeWindowMinutes: Double) {
        let rows = batch.toRows()
        guard let row = rows.first else { return }

        let errorLogs = asDouble(row[safe: 0]) ?? 0
        var combined = previousMetricValues ?? [:]
        combined[HealthMetricNames.errorLogRate] = errorLogs / timeWindowMinutes
        previousMetricValues = combined
    }

    /// Clear any comparison period data.
    public func clearComparison() {
        previousMetricValues = nil
    }

    // MARK: - Batch ingestion

    /// Apply a batch from the span aggregation stream (snapshot or Replace delta).
    /// Expected schema: error_count, total, p50, p95, p99, http_5xx, http_total (all 1 row).
    public func applySpanBatch(_ batch: RecordBatch) {
        let rows = batch.toRows()
        guard let row = rows.first else { return }

        let errorCount = asDouble(row[safe: 0]) ?? 0
        let total      = asDouble(row[safe: 1]) ?? 0

        var metrics: [String: Double] = [:]
        if total > 0 {
            metrics[HealthMetricNames.spanErrorRate] = errorCount / total
            metrics[HealthMetricNames.latencyP50]    = asDouble(row[safe: 2]) ?? 0
            metrics[HealthMetricNames.latencyP95]    = asDouble(row[safe: 3]) ?? 0
            metrics[HealthMetricNames.latencyP99]    = asDouble(row[safe: 4]) ?? 0
            metrics[HealthMetricNames.throughput]    = total / timeWindowMinutes

            let http5xx   = asDouble(row[safe: 5]) ?? 0
            let httpTotal = asDouble(row[safe: 6]) ?? 0
            if httpTotal > 0 {
                metrics[HealthMetricNames.httpErrorRate] = http5xx / httpTotal
            }
        }

        spanMetrics = metrics
        spanDataReady = true
        computeIfReady()
    }

    /// Apply a batch from the log aggregation stream (snapshot or Replace delta).
    /// Expected schema: error_logs (1 row).
    public func applyLogBatch(_ batch: RecordBatch) {
        let rows = batch.toRows()
        guard let row = rows.first else { return }

        let errorLogs = asDouble(row[safe: 0]) ?? 0
        logMetrics = [HealthMetricNames.errorLogRate: errorLogs / timeWindowMinutes]
        logDataReady = true
        computeIfReady()
    }

    // MARK: - Private

    private func computeIfReady() {
        guard spanDataReady && logDataReady else { return }

        var combined = spanMetrics
        for (key, value) in logMetrics { combined[key] = value }

        healthMetricValues = combined
        healthAnalysis = analyzer.analyze(metrics: combined, serviceName: serviceName)
        isLoading = false
    }

    private func asDouble(_ value: Any?) -> Double? {
        switch value {
        case let d as Double: return d
        case let f as Float:  return Double(f)
        case let n as NSNumber: return n.doubleValue
        default: return nil
        }
    }
}

// MARK: - Array safe subscript (file-private)

private extension Array {
    subscript(safe index: Int) -> Element? {
        indices.contains(index) ? self[index] : nil
    }
}
