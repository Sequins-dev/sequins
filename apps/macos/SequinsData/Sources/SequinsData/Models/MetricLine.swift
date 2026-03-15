//
//  MetricLine.swift
//  SequinsData
//
//  An @Observable metric line whose dataPoints are patched surgically by
//  DeltaOp::Update frames. Held by MetricsViewModel keyed by metric_id.
//
//  SwiftUI's Observation framework tracks which properties each view reads,
//  so a chart card that reads `metricLine.dataPoints` will only re-render
//  when THAT metric's data changes — not on every live update.
//

import Foundation

/// An observable metric line with live-patchable data points.
///
/// Created once per metric from the Phase 1 historical snapshot. In live mode,
/// `dataPoints` is updated in-place when a `DeltaOp::Update` arrives for this
/// metric's row_id, avoiding a full re-render of unaffected chart cards.
@Observable
public final class MetricLine: Identifiable {
    /// Metric ID (UUID hex string) — also used as the `id` for `Identifiable`
    public let id: String

    /// Row index in the live query outer result (used to match Update delta row_ids)
    public let rowId: UInt64

    // ── Metric metadata (immutable after creation) ────────────────────────────
    public let name: String
    public let description: String
    public let unit: String
    public let metricType: MetricType
    public let serviceName: String

    /// Data points — mutated in-place when a DeltaOp::Update arrives.
    public var dataPoints: [MetricDataPoint]

    /// Append new data points and discard any older than `cutoff` in one mutation,
    /// keeping `dataPoints` bounded to the current live time window.
    public func appendAndPrune(_ newDataPoints: [MetricDataPoint], olderThan cutoff: Date) {
        dataPoints.append(contentsOf: newDataPoints)
        dataPoints = dataPoints.filter { $0.timestamp >= cutoff }
    }

    public init(
        id: String,
        rowId: UInt64,
        name: String,
        description: String,
        unit: String,
        metricType: MetricType,
        serviceName: String,
        dataPoints: [MetricDataPoint] = []
    ) {
        self.id = id
        self.rowId = rowId
        self.name = name
        self.description = description
        self.unit = unit
        self.metricType = metricType
        self.serviceName = serviceName
        self.dataPoints = dataPoints
    }
}
