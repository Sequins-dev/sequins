//
//  HealthViewModel.swift
//  Sequins
//
//  Thin query orchestrator for the health tab. Builds and starts two live
//  aggregation queries, wires their callbacks to ServiceHealthFeed, and
//  exposes the feed for the view to render.
//
//  All Arrow decoding, metric extraction, and health analysis live in
//  SequinsData.ServiceHealthFeed — this class owns only query lifecycle.
//

import AppKit
import Foundation
import SwiftUI
import SequinsData

// MARK: - ComparisonPeriod

enum ComparisonPeriod: String, CaseIterable, Identifiable {
    case none = "None"
    case previousPeriod = "Previous Period"
    case yesterday = "Yesterday"

    var id: String { rawValue }
}

@MainActor
@Observable
final class HealthViewModel {
    // MARK: - Observable state

    /// The live health data feed. Replaced on each loadHealth call.
    private(set) var feed: ServiceHealthFeed?

    /// Query-level error (set if executeView throws).
    private(set) var error: String?

    /// Controls whether the HealthRulesSettingsView sheet is presented.
    var showingHealthRulesSheet: Bool = false

    /// Active comparison period selection.
    var comparisonPeriod: ComparisonPeriod = .none

    // MARK: - Private

    private var spanViewHandle: ViewHandle?
    private var logViewHandle: ViewHandle?
    private weak var dataSource: DataSource?
    private var healthConfig: HealthThresholdConfig = .default

    /// Cached time window so comparison queries can use the same duration.
    private var lastDuration: TimeInterval = 3600
    private var lastResourceFilter: String = ""

    // MARK: - Initialization

    init() {}

    // MARK: - Configuration

    func configure(dataSource: DataSource?) {
        cancel()
        self.dataSource = dataSource
        if let ds = dataSource {
            loadHealthConfig(from: ds)
        }
    }

    func reloadConfig(dataSource: DataSource?) {
        if let ds = dataSource {
            loadHealthConfig(from: ds)
        }
    }

    private func loadHealthConfig(from dataSource: DataSource) {
        do {
            healthConfig = try dataSource.getHealthThresholdConfig()
            print("🏥 Loaded health config with \(healthConfig.rules.count) rules")
        } catch {
            print("🏥 Failed to load health config, using defaults: \(error)")
            healthConfig = .default
        }
    }

    // MARK: - Query lifecycle

    func loadHealth(
        dataSource: DataSource?,
        selectedService: Service?,
        timeRange: TimeRange
    ) async {
        guard let dataSource else {
            print("🏥 [HealthViewModel] No data source")
            return
        }

        self.dataSource = dataSource

        let duration = timeRange.bounds.end.timeIntervalSince(timeRange.bounds.start)
        lastDuration = duration
        let timeWindowMinutes = max(1.0, duration / 60.0)
        let durationStr = formatDuration(duration)
        let resourceFilter = buildResourceIdFilter(selectedService)
        lastResourceFilter = resourceFilter
        let serviceName = selectedService?.name ?? "unknown"

        let spanQuery =
            "spans last \(durationStr)\(resourceFilter)"
            + " | group by {} {"
            + " count() where status == 2 as error_count,"
            + " count() as total,"
            + " p50(duration_ns) as p50,"
            + " p95(duration_ns) as p95,"
            + " p99(duration_ns) as p99,"
            + " count() where attr.http_status_code >= 500 as http_5xx,"
            + " count() where attr.http_status_code > 0 as http_total"
            + " }"

        let logQuery =
            "logs last \(durationStr) | where severity_number >= 9\(resourceFilter)"
            + " | group by {} { count() as error_logs }"

        print("🏥 [HealthViewModel] Span query: \(spanQuery)")
        print("🏥 [HealthViewModel] Log query: \(logQuery)")

        cancel()
        error = nil

        let newFeed = ServiceHealthFeed(
            serviceName: serviceName,
            timeWindowMinutes: timeWindowMinutes,
            config: healthConfig
        )
        feed = newFeed

        do {
            spanViewHandle = try dataSource.executeView(spanQuery, strategy: .aggregate) { [weak newFeed] deltas in
                for d in deltas where d.type == .tableReplaced || d.type == .rowsAppended {
                    if let batch = d.data.first {
                        newFeed?.applySpanBatch(batch)
                    }
                }
            }

            logViewHandle = try dataSource.executeView(logQuery, strategy: .aggregate) { [weak newFeed] deltas in
                for d in deltas where d.type == .tableReplaced || d.type == .rowsAppended {
                    if let batch = d.data.first {
                        newFeed?.applyLogBatch(batch)
                    }
                }
            }
        } catch {
            print("🏥 [HealthViewModel] Query failed: \(error)")
            self.error = error.localizedDescription
            newFeed.markFailed()
        }

        if comparisonPeriod != .none {
            await loadComparison(dataSource: dataSource)
        }
    }

    func cancel() {
        spanViewHandle?.cancel()
        spanViewHandle = nil
        logViewHandle?.cancel()
        logViewHandle = nil
    }

    // MARK: - Comparison queries

    /// Run snapshot queries for the comparison period and populate `feed.previousMetricValues`.
    func loadComparison(dataSource: DataSource?) async {
        guard let dataSource, let currentFeed = feed else { return }

        guard comparisonPeriod != .none else {
            currentFeed.clearComparison()
            return
        }

        let offsetSeconds: TimeInterval
        switch comparisonPeriod {
        case .none: return
        case .previousPeriod: offsetSeconds = lastDuration
        case .yesterday: offsetSeconds = 86400
        }

        let comparisonWindowMinutes = max(1.0, lastDuration / 60.0)
        let durationStr = formatDuration(lastDuration)
        let cutoffNs = UInt64((Date().timeIntervalSince1970 - offsetSeconds) * 1_000_000_000)

        let spanQuery =
            "spans last \(durationStr)\(lastResourceFilter)"
            + " | where start_time_unix_nano < \(cutoffNs)"
            + " | group by {} {"
            + " count() where status == 2 as error_count,"
            + " count() as total,"
            + " p50(duration_ns) as p50,"
            + " p95(duration_ns) as p95,"
            + " p99(duration_ns) as p99,"
            + " count() where attr.http_status_code >= 500 as http_5xx,"
            + " count() where attr.http_status_code > 0 as http_total"
            + " }"

        let logQuery =
            "logs last \(durationStr) | where severity_number >= 9\(lastResourceFilter)"
            + " | where start_time_unix_nano < \(cutoffNs)"
            + " | group by {} { count() as error_logs }"

        currentFeed.clearComparison()

        final class SnapshotSink: SeQLSink {
            let onBatch: (RecordBatch) -> Void
            init(onBatch: @escaping (RecordBatch) -> Void) { self.onBatch = onBatch }
            nonisolated func onSchema(_ schema: SeQLSchema) {}
            nonisolated func onBatch(_ batch: RecordBatch, table: String?) { if table == nil { onBatch(batch) } }
            nonisolated func onComplete(_ stats: SeQLStats) {}
            nonisolated func onWarning(code: UInt32, message: String) {}
            nonisolated func onError(code: UInt32, message: String) {}
        }

        do {
            let spanSink = SnapshotSink { [weak currentFeed] batch in
                DispatchQueue.main.async {
                    currentFeed?.applyComparisonSpanBatch(batch, timeWindowMinutes: comparisonWindowMinutes)
                }
            }
            let _ = try dataSource.executeSeQL(spanQuery, sink: spanSink)
        } catch {
            print("🏥 [HealthViewModel] Comparison span query failed: \(error)")
        }

        do {
            let logSink = SnapshotSink { [weak currentFeed] batch in
                DispatchQueue.main.async {
                    currentFeed?.applyComparisonLogBatch(batch, timeWindowMinutes: comparisonWindowMinutes)
                }
            }
            let _ = try dataSource.executeSeQL(logQuery, sink: logSink)
        } catch {
            print("🏥 [HealthViewModel] Comparison log query failed: \(error)")
        }
    }

    // MARK: - Export

    func exportHealthSnapshot() {
        guard let feed else { return }

        var dict: [String: Any] = [:]
        dict["service"] = feed.healthAnalysis?.serviceName ?? "unknown"
        dict["timestamp"] = ISO8601DateFormatter().string(from: Date())

        if let analysis = feed.healthAnalysis {
            dict["status"] = analysis.status.rawValue
            dict["score"] = analysis.overallScore
            dict["factors"] = analysis.factors.map { factor -> [String: Any] in
                [
                    "metric": factor.metricName,
                    "display_name": factor.displayName,
                    "value": factor.rawValue as Any,
                    "formatted_value": factor.formattedValue
                ]
            }
        }

        dict["metric_values"] = feed.healthMetricValues
        if let previous = feed.previousMetricValues {
            dict["previous_metric_values"] = previous
        }

        let panel = NSSavePanel()
        panel.title = "Export Health Snapshot"
        panel.nameFieldStringValue = "health-snapshot.json"
        panel.allowedContentTypes = [.json]
        panel.canCreateDirectories = true
        guard panel.runModal() == .OK, let url = panel.url else { return }

        do {
            let data = try JSONSerialization.data(withJSONObject: dict, options: [.prettyPrinted, .sortedKeys])
            try data.write(to: url)
        } catch {
            NSLog("HealthViewModel: export failed: \(error)")
        }
    }

    // MARK: - Helpers

    private func buildResourceIdFilter(_ selectedService: Service?) -> String {
        guard let service = selectedService, !service.resourceIds.isEmpty else { return "" }
        if service.resourceIds.count == 1 {
            return " | where resource_id = \(service.resourceIds[0])"
        }
        let ids = service.resourceIds.map { String($0) }.joined(separator: ", ")
        return " | where resource_id in [\(ids)]"
    }

    private func formatDuration(_ duration: TimeInterval) -> String {
        let hours = Int(duration / 3600)
        if hours > 0 && hours <= 24 {
            return "\(hours)h"
        }
        let minutes = Int(duration / 60)
        return "\(max(minutes, 1))m"
    }
}
