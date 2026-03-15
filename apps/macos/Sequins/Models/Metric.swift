//
//  Metric.swift
//  Sequins
//
//  Metric data models - ported from node.builders
//

import Foundation
import SwiftUI

/// A single metric data point for charting
struct MetricDataPoint: Identifiable {
    let id = UUID()
    let timestamp: Date
    let value: Double
    let containerId: String?

    init(timestamp: Date, value: Double, containerId: String? = nil) {
        self.timestamp = timestamp
        self.value = value
        self.containerId = containerId
    }
}

/// Granularity for metric aggregation
// Granularity enum moved to MetricsViewModel
