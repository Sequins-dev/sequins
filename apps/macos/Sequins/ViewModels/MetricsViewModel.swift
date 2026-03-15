//
//  MetricsViewModel.swift
//  Sequins
//
//  View model for the Metrics tab.
//
//  Uses the reactive view API (executeView) with TableStrategy.
//  The `table` field on ViewDelta replaces the first-column-type discrimination
//  used in the old SeQLSink path.
//

import AppKit
import Foundation
import SwiftUI
import SequinsData
import UniformTypeIdentifiers

@MainActor
@Observable
final class MetricsViewModel {
    // MARK: - Observable State

    /// Gauge/counter metric lines keyed by metric_id.
    private(set) var metricLines: [String: MetricLine] = [:]

    /// Histogram metric lines keyed by metric_id.
    private(set) var histogramLines: [String: HistogramLine] = [:] {
        didSet { sortedHistograms = histogramLines.values.sorted { $0.name < $1.name } }
    }

    /// Histogram lines sorted by name — cached so MetricsContentView avoids re-sorting on every access.
    private(set) var sortedHistograms: [HistogramLine] = []

    /// Running sum/count per (metricId, bucketTimestamp) for live gauge aggregation.
    private var gaugeBinAccumulators: [String: [TimeInterval: (sum: Double, count: Int)]] = [:]

    /// Loading state
    private(set) var isLoading: Bool = false

    /// Error from query
    private(set) var error: String?

    // MARK: - Private State

    /// View handle for the gauge/counter query.
    private var gaugeViewHandle: ViewHandle?

    /// View handle for the histogram query.
    private var histogramViewHandle: ViewHandle?

    /// Data source reference
    private weak var dataSource: DataSource?

    // MARK: - Properties

    var selectedWorkerThread: String = "all"
    var timeRange: TimeRange = .last(hours: 1)

    var availableThreads: [String] = []

    /// All gauge/counter metrics sorted by name.
    var metrics: [SequinsData.Metric] {
        metricLines.values
            .map { line in
                SequinsData.Metric(
                    id: line.id,
                    name: line.name,
                    description: line.description,
                    unit: line.unit,
                    metricType: line.metricType,
                    serviceName: line.serviceName
                )
            }
            .sorted { $0.name < $1.name }
    }

    /// Detected metric groups (computed from gauge/counter metrics).
    var metricGroups: [SequinsData.MetricGroup] {
        var groups: [SequinsData.MetricGroup] = []
        let metricsByService = Dictionary(grouping: metrics) { $0.serviceName }
        for (serviceName, serviceMetrics) in metricsByService {
            let statGroups = detectStatisticalGroups(serviceMetrics, serviceName: serviceName)
            groups.append(contentsOf: statGroups)
        }
        return groups
    }

    /// Bin size in seconds that produces ~100 data points for the current time range.
    var computedBinSeconds: TimeInterval {
        binSeconds(for: timeRange.duration)
    }

    /// Human-readable bin size string for display in the filter bar.
    var binSizeStringForDisplay: String {
        binSizeString(for: timeRange.duration)
    }

    // MARK: - Public Methods

    func configure(dataSource: DataSource?) {
        cancel()
        self.dataSource = dataSource
        metricLines = [:]
        histogramLines = [:]
        gaugeBinAccumulators = [:]
        isLoading = false
        error = nil
    }

    func loadMetrics(dataSource: DataSource?, selectedService: Service?, timeRange: TimeRange) async {
        guard let dataSource = dataSource else {
            print("📊 [MetricsViewModel] No data source available")
            return
        }

        self.dataSource = dataSource
        self.timeRange = timeRange

        let duration = timeRange.bounds.end.timeIntervalSince(timeRange.bounds.start)
        let hours = Int(duration / 3600)
        let binSize = binSizeString(for: duration)

        let timeStr: String
        if hours > 0 && hours <= 24 {
            timeStr = "last \(hours)h"
        } else {
            let minutes = Int(duration / 60)
            timeStr = "last \(max(minutes, 1))m"
        }

        let resourceFilter = buildResourceIdFilter(selectedService) ?? ""

        let gaugeQuery = "metrics \(timeStr)\(resourceFilter) | where metric_type != 'histogram' <- (datapoints | group by { ts() bin \(binSize) as bucket, metric_id } { avg(value) as val }) as datapoints"
        let histQuery = "metrics \(timeStr)\(resourceFilter) | where metric_type = 'histogram' <- (histograms) as histograms"

        print("📊 [MetricsViewModel] Gauge query: \(gaugeQuery)")
        print("📊 [MetricsViewModel] Histogram query: \(histQuery)")

        cancel()
        metricLines = [:]
        histogramLines = [:]
        gaugeBinAccumulators = [:]
        isLoading = true
        error = nil

        do {
            gaugeViewHandle = try dataSource.executeView(gaugeQuery, strategy: .table) { [weak self] deltas in
                self?.processGaugeDeltas(deltas)
            }

            histogramViewHandle = try dataSource.executeView(histQuery, strategy: .table) { [weak self] deltas in
                self?.processHistogramDeltas(deltas)
            }

            try? await Task.sleep(nanoseconds: 200_000_000)
            let totalPoints = metricLines.values.reduce(0) { $0 + $1.dataPoints.count }
            print("📊 [MetricsViewModel] Loaded \(totalPoints) data points across \(metricLines.count) gauge metrics, \(histogramLines.count) histogram metrics")
            isLoading = false
        } catch {
            print("📊 [MetricsViewModel] Query failed: \(error)")
            isLoading = false
            self.error = "Query failed: \(error.localizedDescription)"
        }
    }

    func refresh(dataSource: DataSource?, selectedService: Service?, timeRange: TimeRange) {
        Task {
            await loadMetrics(dataSource: dataSource, selectedService: selectedService, timeRange: timeRange)
        }
    }

    func clearModelCache() {
        metricLines.removeAll()
        histogramLines.removeAll()
        gaugeBinAccumulators.removeAll()
    }

    func updateTimeRangeOnModels(_ timeRange: TimeRange) {
        self.timeRange = timeRange
    }

    var groupedMetricNames: Set<String> {
        Set(metricGroups.flatMap { $0.metricNames })
    }

    func getDataPoints(forMetricId metricId: String) -> [SequinsData.MetricDataPoint] {
        metricLines[metricId]?.dataPoints ?? []
    }

    func getDataPoints(forMetricName name: String) -> [SequinsData.MetricDataPoint] {
        metricLines.values
            .filter { $0.name == name }
            .flatMap { $0.dataPoints }
    }

    func getEffectiveBucketDuration(forMetricId _: String) -> TimeInterval? {
        computedBinSeconds
    }

    /// Start live mode with two view streams: gauge/counter and histogram.
    func startLiveStream(dataSource: DataSource?, selectedService: Service?, timeRange: TimeRange) {
        guard let dataSource = dataSource else { return }

        cancel()

        self.timeRange = timeRange
        let duration = timeRange.bounds.end.timeIntervalSince(timeRange.bounds.start)
        let hours = Int(duration / 3600)
        let liveBinSize = binSizeString(for: duration)
        let timeStr: String
        if hours > 0 && hours <= 24 {
            timeStr = "last \(hours)h"
        } else {
            let minutes = Int(duration / 60)
            timeStr = "last \(max(minutes, 1))m"
        }
        let resourceFilter = buildResourceIdFilter(selectedService) ?? ""

        do {
            let gaugeQuery = "metrics \(timeStr)\(resourceFilter) | where metric_type != 'histogram' <- (datapoints | group by { ts() bin \(liveBinSize) as bucket, metric_id } { avg(value) as val }) as datapoints"
            gaugeViewHandle = try dataSource.executeView(gaugeQuery, strategy: .table) { [weak self] deltas in
                self?.processGaugeDeltas(deltas)
            }

            let histQuery = "metrics \(timeStr)\(resourceFilter) | where metric_type = 'histogram' <- (histograms) as histograms"
            histogramViewHandle = try dataSource.executeView(histQuery, strategy: .table) { [weak self] deltas in
                self?.processHistogramDeltas(deltas)
            }
        } catch {
            NSLog("📊 [MetricsViewModel] Live stream failed to start: \(error)")
            self.error = "Query failed: \(error.localizedDescription)"
        }
    }

    func stopLiveStream() {
        gaugeViewHandle?.cancel()
        gaugeViewHandle = nil
        histogramViewHandle?.cancel()
        histogramViewHandle = nil
    }

    func exportAsJSON() {
        let panel = NSSavePanel()
        panel.title = "Export Metrics"
        panel.nameFieldStringValue = "metrics-export.json"
        panel.allowedContentTypes = [.json]
        panel.canCreateDirectories = true

        guard panel.runModal() == .OK, let url = panel.url else { return }

        let jsonArray: [[String: Any]] = metricLines.values
            .sorted { $0.name < $1.name }
            .map { line in
                [
                    "id": line.id,
                    "name": line.name,
                    "description": line.description,
                    "unit": line.unit,
                    "type": String(describing: line.metricType),
                    "serviceName": line.serviceName,
                    "dataPoints": line.dataPoints.map { dp in
                        ["timestamp": dp.timestamp.timeIntervalSince1970, "value": dp.value] as [String: Any]
                    }
                ] as [String: Any]
            }

        do {
            let data = try JSONSerialization.data(withJSONObject: jsonArray, options: [.prettyPrinted, .sortedKeys])
            try data.write(to: url)
            NSLog("📊 Exported \(jsonArray.count) metrics to \(url.path)")
        } catch {
            NSLog("📊 Failed to export metrics JSON: \(error)")
        }
    }

    func exportAsCSV() {
        let panel = NSSavePanel()
        panel.title = "Export Metrics as CSV"
        panel.nameFieldStringValue = "metrics-export.csv"
        panel.allowedContentTypes = [UTType(filenameExtension: "csv") ?? .commaSeparatedText]
        panel.canCreateDirectories = true

        guard panel.runModal() == .OK, let url = panel.url else { return }

        var lines = ["metric_name,timestamp,value,unit,type"]
        for line in metricLines.values.sorted(by: { $0.name < $1.name }) {
            for dp in line.dataPoints {
                let ts = String(format: "%.3f", dp.timestamp.timeIntervalSince1970)
                let val = String(dp.value)
                lines.append("\(line.name),\(ts),\(val),\(line.unit),\(line.metricType)")
            }
        }

        do {
            try lines.joined(separator: "\n").write(to: url, atomically: true, encoding: .utf8)
            NSLog("📊 Exported metrics CSV to \(url.path)")
        } catch {
            NSLog("📊 Failed to export metrics CSV: \(error)")
        }
    }

    func cancel() {
        gaugeViewHandle?.cancel()
        gaugeViewHandle = nil
        histogramViewHandle?.cancel()
        histogramViewHandle = nil
        isLoading = false
    }

    // MARK: - Gauge delta processing

    private func processGaugeDeltas(_ deltas: [ViewDelta]) {
        // Pre-compute toRows() in background (avoids stalling the main thread for large batches).
        let binSecs = max(1.0, computedBinSeconds)
        let currentAccumulators = gaugeBinAccumulators
        let existingLineCount = metricLines.count

        Task.detached { [weak self] in
            var parsed: [(delta: ViewDelta, rows: [[Any?]])] = []
            for delta in deltas {
                if let batch = delta.data.first {
                    parsed.append((delta, batch.toRows()))
                }
            }
            await MainActor.run { [weak self] in
                self?.applyParsedGaugeDeltas(parsed, binSecs: binSecs,
                                             accumulators: currentAccumulators,
                                             existingLineCount: existingLineCount)
            }
        }
    }

    @MainActor
    private func applyParsedGaugeDeltas(
        _ parsed: [(delta: ViewDelta, rows: [[Any?]])],
        binSecs: TimeInterval,
        accumulators: [String: [TimeInterval: (sum: Double, count: Int)]],
        existingLineCount: Int
    ) {
        var localCount = existingLineCount
        for (delta, rows) in parsed {
            switch delta.type {
            case .tableReplaced:
                guard delta.table == nil else { continue }
                metricLines.removeAll()
                gaugeBinAccumulators.removeAll()
                localCount = 0
                for (rowIdx, row) in rows.enumerated() {
                    if let line = parseMetricLine(from: row, rowId: UInt64(rowIdx)) {
                        metricLines[line.id] = line
                        localCount += 1
                    }
                }

            case .rowsAppended:
                let table = delta.table
                if table == nil {
                    for (rowIdx, row) in rows.enumerated() {
                        let rowId = UInt64(localCount + rowIdx)
                        if let line = parseMetricLine(from: row, rowId: rowId) {
                            metricLines[line.id] = line
                        }
                    }
                    localCount += rows.count
                } else if table == "datapoints" {
                    let isBinned = rows.first.map { $0.count == 3 } ?? true
                    if isBinned {
                        for row in rows {
                            guard row.count >= 3,
                                  let metricId = row[1] as? String,
                                  let line = metricLines[metricId]
                            else { continue }
                            let bucketNs: Int64
                            if let d = row[0] as? Date {
                                bucketNs = Int64(d.timeIntervalSince1970 * 1_000_000_000)
                            } else if let n = asInt64(row[0]) {
                                bucketNs = n
                            } else { continue }
                            guard let val = asDouble(row[2]) else { continue }
                            let timestamp = Date(timeIntervalSince1970: Double(bucketNs) / 1_000_000_000)
                            let dp = SequinsData.MetricDataPoint(metricId: metricId, timestamp: timestamp, value: val, attributes: [:])
                            line.dataPoints.append(dp)
                        }
                    } else {
                        for row in rows {
                            guard row.count >= 4,
                                  let metricId = row[1] as? String,
                                  let line = metricLines[metricId]
                            else { continue }
                            let timestamp: Date
                            if let d = row[2] as? Date {
                                timestamp = d
                            } else if let ns = asInt64(row[2]) {
                                timestamp = Date(timeIntervalSince1970: Double(ns) / 1_000_000_000)
                            } else { continue }
                            guard let val = asDouble(row[3]) else { continue }

                            let bucketTs = (timestamp.timeIntervalSince1970 / binSecs).rounded(.down) * binSecs
                            var accum = gaugeBinAccumulators[metricId] ?? [:]
                            let (prevSum, prevCount) = accum[bucketTs] ?? (0.0, 0)
                            let newSum = prevSum + val
                            let newCount = prevCount + 1
                            accum[bucketTs] = (newSum, newCount)
                            gaugeBinAccumulators[metricId] = accum

                            let avgValue = newSum / Double(newCount)
                            let bucketDate = Date(timeIntervalSince1970: bucketTs)
                            let dp = SequinsData.MetricDataPoint(metricId: metricId, timestamp: bucketDate, value: avgValue, attributes: [:])

                            if let existingIdx = line.dataPoints.firstIndex(where: {
                                abs($0.timestamp.timeIntervalSince1970 - bucketTs) < 1.0
                            }) {
                                line.dataPoints[existingIdx] = dp
                            } else {
                                let cutoff = Date().addingTimeInterval(-3600)
                                line.appendAndPrune([dp], olderThan: cutoff)
                            }
                        }
                    }
                }

            default:
                break
            }
        }
    }

    // MARK: - Histogram delta processing

    private func processHistogramDeltas(_ deltas: [ViewDelta]) {
        let existingLineCount = histogramLines.count

        Task.detached { [weak self] in
            var parsed: [(delta: ViewDelta, rows: [[Any?]])] = []
            for delta in deltas {
                if let batch = delta.data.first {
                    parsed.append((delta, batch.toRows()))
                }
            }
            await MainActor.run { [weak self] in
                self?.applyParsedHistogramDeltas(parsed, existingLineCount: existingLineCount)
            }
        }
    }

    @MainActor
    private func applyParsedHistogramDeltas(
        _ parsed: [(delta: ViewDelta, rows: [[Any?]])],
        existingLineCount: Int
    ) {
        var localCount = existingLineCount
        var hadReady = false

        for (delta, rows) in parsed {
            switch delta.type {
            case .tableReplaced:
                guard delta.table == nil else { continue }
                histogramLines.removeAll()
                localCount = 0
                for (rowIdx, row) in rows.enumerated() {
                    if let line = parseHistogramLine(from: row, rowId: UInt64(rowIdx)) {
                        histogramLines[line.id] = line
                        localCount += 1
                    }
                }

            case .rowsAppended:
                let table = delta.table
                if table == nil {
                    for (rowIdx, row) in rows.enumerated() {
                        let rowId = UInt64(localCount + rowIdx)
                        if let line = parseHistogramLine(from: row, rowId: rowId) {
                            histogramLines[line.id] = line
                        }
                    }
                    localCount += rows.count
                } else if table == "histograms" {
                    var pending: [String: [HistogramSnapshot]] = [:]
                    for row in rows {
                        guard row.count >= 7,
                              let metricId = row[1] as? String,
                              let snap = parseHistogramSnapshotRow(row)
                        else { continue }
                        pending[metricId, default: []].append(snap)
                    }
                    for (metricId, snaps) in pending {
                        guard let line = histogramLines[metricId] else { continue }
                        line.snapshots.append(contentsOf: snaps)
                    }
                } else {
                    var pending: [String: [HistogramSnapshot]] = [:]
                    for row in rows {
                        guard row.count >= 7,
                              let metricId = row[1] as? String,
                              let snap = parseHistogramSnapshotRow(row)
                        else { continue }
                        pending[metricId, default: []].append(snap)
                    }
                    let cutoff = Date().addingTimeInterval(-3600)
                    for (metricId, snaps) in pending {
                        histogramLines[metricId]?.appendCumulativeAndPrune(snaps, olderThan: cutoff)
                    }
                }

            case .ready:
                hadReady = true

            default:
                break
            }
        }

        if hadReady {
            for line in histogramLines.values {
                line.replaceWithCumulatives(line.snapshots)
            }
            isLoading = false
        }
    }

    // MARK: - Arrow column parsing

    /// Parse a MetricLine from a primary metrics query row.
    ///   0: metric_id, 1: name, 2: description, 3: unit, 4: metric_type,
    ///   5: service_name, 6: resource_id, 7: scope_id
    private func parseMetricLine(from row: [Any?], rowId: UInt64) -> MetricLine? {
        guard row.count >= 6 else { return nil }

        guard
            let metricId = row[0] as? String,
            let name = row[1] as? String,
            let metricTypeStr = row[4] as? String,
            let serviceName = row[5] as? String
        else { return nil }

        let description = row[2] as? String ?? ""
        let unit = row[3] as? String ?? ""
        let metricType = parseMetricType(metricTypeStr)

        return MetricLine(
            id: metricId,
            rowId: rowId,
            name: name,
            description: description,
            unit: unit,
            metricType: metricType,
            serviceName: serviceName,
            dataPoints: []
        )
    }

    /// Parse a HistogramLine from a primary histogram metrics query row.
    private func parseHistogramLine(from row: [Any?], rowId: UInt64) -> HistogramLine? {
        guard row.count >= 6 else { return nil }

        guard
            let metricId = row[0] as? String,
            let name = row[1] as? String,
            let serviceName = row[5] as? String
        else { return nil }

        let description = row[2] as? String ?? ""
        let unit = row[3] as? String ?? ""

        return HistogramLine(
            id: metricId,
            rowId: rowId,
            name: name,
            description: description,
            unit: unit,
            serviceName: serviceName,
            snapshots: []
        )
    }

    /// Parse one HistogramSnapshot from an auxiliary "histograms" table row.
    ///   0: series_id, 1: metric_id, 2: time_unix_nano, 3: count, 4: sum,
    ///   5: bucket_counts, 6: explicit_bounds
    private func parseHistogramSnapshotRow(_ row: [Any?]) -> HistogramSnapshot? {
        guard row.count >= 7 else { return nil }

        let timestamp: Date
        if let d = row[2] as? Date {
            timestamp = d
        } else if let ns = asInt64(row[2]) {
            timestamp = Date(timeIntervalSince1970: Double(ns) / 1_000_000_000)
        } else {
            return nil
        }

        guard let count = asUInt64(row[3]) else { return nil }
        let sum = asDouble(row[4]) ?? 0.0
        let bucketCounts = parseUInt64List(row[5])
        let explicitBounds = parseFloat64List(row[6])

        return HistogramSnapshot(
            timestamp: timestamp,
            count: count,
            sum: sum,
            bucketCounts: bucketCounts,
            explicitBounds: explicitBounds
        )
    }

    // MARK: - List parsing helpers

    private func parseUInt64List(_ value: Any?) -> [UInt64] {
        guard let list = value as? [Any?] else { return [] }
        return list.compactMap { asUInt64($0) }
    }

    private func parseFloat64List(_ value: Any?) -> [Double] {
        guard let list = value as? [Any?] else { return [] }
        return list.compactMap { asDouble($0) }
    }

    private func parseMetricType(_ s: String) -> MetricType {
        switch s.lowercased() {
        case "counter": return .counter
        case "histogram": return .histogram
        case "summary": return .summary
        default: return .gauge
        }
    }

    private func asDouble(_ v: Any?) -> Double? {
        switch v {
        case let d as Double: return d
        case let f as Float: return Double(f)
        case let n as NSNumber: return n.doubleValue
        default: return nil
        }
    }

    private func asInt64(_ v: Any?) -> Int64? {
        switch v {
        case let i as Int64: return i
        case let i as Int: return Int64(i)
        case let i as Int32: return Int64(i)
        case let n as NSNumber: return n.int64Value
        default: return nil
        }
    }

    private func asUInt64(_ v: Any?) -> UInt64? {
        switch v {
        case let u as UInt64: return u
        case let u as UInt32: return UInt64(u)
        case let i as Int64 where i >= 0: return UInt64(i)
        case let n as NSNumber: return n.uint64Value
        default: return nil
        }
    }

    // MARK: - Private helpers

    private func buildResourceIdFilter(_ selectedService: Service?) -> String? {
        guard let service = selectedService, !service.resourceIds.isEmpty else { return nil }
        if service.resourceIds.count == 1 {
            return " | where resource_id = \(service.resourceIds[0])"
        }
        let idList = service.resourceIds.map { String($0) }.joined(separator: ", ")
        return " | where resource_id in [\(idList)]"
    }

    private static let niceIntervals: [Int] = [
        1, 2, 3, 5, 10, 15, 20, 30, 60, 90, 120, 180, 240, 300, 600, 900, 1200, 1800, 3600
    ]

    private func binSeconds(for duration: TimeInterval) -> TimeInterval {
        let target = max(5.0, duration / 100.0)
        let best = Self.niceIntervals.min(by: { abs(Double($0) - target) < abs(Double($1) - target) }) ?? 5
        return TimeInterval(best)
    }

    private func binSizeString(for duration: TimeInterval) -> String {
        let secs = Int(binSeconds(for: duration))
        return secs < 60 ? "\(secs)s" : "\(secs / 60)m"
    }

    private func detectStatisticalGroups(_ metrics: [SequinsData.Metric], serviceName: String) -> [SequinsData.MetricGroup] {
        var groups: [SequinsData.MetricGroup] = []
        let suffixes = [".min", ".max", ".mean", ".p50", ".p90", ".p95", ".p99"]
        var candidateGroups: [String: [SequinsData.Metric]] = [:]

        for metric in metrics {
            if let suffix = suffixes.first(where: { metric.name.hasSuffix($0) }) {
                let baseName = String(metric.name.dropLast(suffix.count))
                candidateGroups[baseName, default: []].append(metric)
            }
        }

        for (baseName, groupMetrics) in candidateGroups where groupMetrics.count >= 2 {
            guard let firstMetric = groupMetrics.first else { continue }
            let allSameUnit = groupMetrics.allSatisfy { $0.unit == firstMetric.unit }
            let allSameType = groupMetrics.allSatisfy { $0.metricType == firstMetric.metricType }

            if allSameUnit && allSameType {
                let group = SequinsData.MetricGroup(
                    baseName: baseName,
                    metricNames: groupMetrics.map { $0.name }.sorted(),
                    pattern: .statisticalVariants,
                    serviceName: serviceName,
                    metricType: firstMetric.metricType,
                    unit: firstMetric.unit,
                    visualization: .multiLineChart
                )
                groups.append(group)
            }
        }

        return groups
    }
}
