import SwiftUI
import Charts
import SequinsData

/// Grouped metric chart card with multi-line visualization
struct GroupedMetricChartCard: View {
    let group: SequinsData.MetricGroup
    let timeRange: SequinsData.TimeRange?
    let onTimeRangeSelected: ((Date, Date) -> Void)?

    @Environment(MetricsViewModel.self) private var viewModel
    @State private var hiddenSeries: Set<String> = []

    // Color palette for series
    private let colorPalette: [Color] = [
        .blue, .green, .orange, .purple, .red, .pink, .yellow, .cyan
    ]

    // Build chart series from group metrics
    private var chartSeries: [ChartSeries] {
        // Sort metric names by suffix (.min, .max, .mean, .p50, etc.)
        let sortedNames = group.metricNames.sorted { name1, name2 in
            let suffixes = [".min", ".max", ".mean", ".p50", ".p90", ".p95", ".p99"]
            let index1 = suffixes.firstIndex(where: { name1.hasSuffix($0) }) ?? suffixes.count
            let index2 = suffixes.firstIndex(where: { name2.hasSuffix($0) }) ?? suffixes.count
            return index1 < index2
        }

        return sortedNames.enumerated().compactMap { (index, metricName) in
            // Get data points for this metric
            let dataPoints = viewModel.getDataPoints(forMetricName: metricName)
            guard !dataPoints.isEmpty else { return nil }

            // Get metric ID for bucket duration
            let metricId = dataPoints.first?.metricId ?? ""
            let bucketDuration = viewModel.getEffectiveBucketDuration(forMetricId: metricId)

            // Apply unit scaling
            let (scaledPoints, _) = applyUnitScaling(dataPoints, unit: group.unit)

            // Convert to local MetricDataPoint
            let localPoints = scaledPoints.map { point in
                MetricDataPoint(
                    timestamp: point.timestamp,
                    value: point.value,
                    containerId: nil
                )
            }

            // Extract suffix for label
            let label = extractSuffix(from: metricName)
            let color = colorPalette[index % colorPalette.count]

            return ChartSeries(
                id: metricName,
                label: label,
                color: color,
                data: localPoints,
                bucketDuration: bucketDuration
            )
        }
    }

    // Scaled unit
    private var displayUnit: String {
        // Use first series to determine scaling
        if let firstMetricName = group.metricNames.first {
            let dataPoints = viewModel.getDataPoints(forMetricName: firstMetricName)
            let (_, scaledUnit) = applyUnitScaling(dataPoints, unit: group.unit)
            return scaledUnit
        }
        return group.unit
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            // Header
            HStack(alignment: .center, spacing: 8) {
                if let metricType = group.metricType {
                    MetricTypePill(metricType: metricType)
                }

                Text(group.baseName)
                    .font(.subheadline)
                    .fontWeight(.medium)
                    .foregroundColor(.primary)
                    .contentShape(Rectangle())  // Define hoverable region for tooltip
                    .help("\(group.metricNames.count) variants: \(group.metricNames.joined(separator: ", "))")

                Spacer()
            }

            // Multi-line chart
            if chartSeries.isEmpty {
                Rectangle()
                    .fill(Color.secondary.opacity(0.1))
                    .frame(height: 120)
                    .overlay(
                        Text("No data available")
                            .font(.caption2)
                            .foregroundColor(.secondary)
                    )
            } else {
                let visibleSeries = chartSeries.filter { !hiddenSeries.contains($0.id) }

                MultiLineChart(
                    series: visibleSeries,
                    unit: displayUnit,
                    timeRange: timeRange,
                    onSelection: { start, end in
                        onTimeRangeSelected?(start, end)
                    }
                )
                .frame(height: 120)

                // Legend chips — click to toggle series visibility
                HStack(spacing: 4) {
                    ForEach(chartSeries) { series in
                        let hidden = hiddenSeries.contains(series.id)
                        Button {
                            if hidden {
                                hiddenSeries.remove(series.id)
                            } else {
                                hiddenSeries.insert(series.id)
                            }
                        } label: {
                            HStack(spacing: 4) {
                                Circle()
                                    .fill(hidden ? Color.secondary.opacity(0.4) : series.color)
                                    .frame(width: 6, height: 6)
                                Text(series.label)
                                    .font(.caption2)
                                    .foregroundColor(hidden ? .secondary.opacity(0.5) : .secondary)
                            }
                            .padding(.horizontal, 6)
                            .padding(.vertical, 2)
                            .background(Color.secondary.opacity(hidden ? 0.05 : 0.1))
                            .cornerRadius(4)
                        }
                        .buttonStyle(.plain)
                    }
                }
            }
        }
        .padding(12)
        .background(Color(NSColor.controlBackgroundColor))
        .cornerRadius(6)
    }

    // MARK: - Helper Methods

    /// Extract the suffix from a metric name (e.g., ".p99" from "nodejs.eventloop.delay.p99")
    private func extractSuffix(from name: String) -> String {
        let suffixes = [".min", ".max", ".mean", ".p50", ".p90", ".p95", ".p99"]
        for suffix in suffixes {
            if name.hasSuffix(suffix) {
                return String(suffix.dropFirst()) // Remove leading dot
            }
        }
        return name
    }

    /// Apply unit scaling to data points (e.g., bytes → MB, nanoseconds → ms)
    private func applyUnitScaling(
        _ dataPoints: [SequinsData.MetricDataPoint],
        unit: String
    ) -> ([SequinsData.MetricDataPoint], String) {
        // Determine scaling factor and new unit
        let (scaleFactor, newUnit): (Double, String) = {
            switch unit.lowercased() {
            case "bytes", "byte":
                // Check if values are large enough to warrant MB scaling
                let maxValue = dataPoints.map { $0.value }.max() ?? 0
                if maxValue >= 1_000_000 {
                    return (1.0 / (1024 * 1024), "MB")
                } else if maxValue >= 1_000 {
                    return (1.0 / 1024, "KB")
                } else {
                    return (1.0, "bytes")
                }

            case "ns", "nanoseconds":
                return (1.0 / 1_000_000, "ms")

            case "us", "microseconds", "µs":
                return (1.0 / 1_000, "ms")

            default:
                return (1.0, unit)
            }
        }()

        // Apply scaling to data points
        let scaledPoints = dataPoints.map { point in
            SequinsData.MetricDataPoint(
                metricId: point.metricId,
                timestamp: point.timestamp,
                value: point.value * scaleFactor,
                attributes: point.attributes
            )
        }

        return (scaledPoints, newUnit)
    }
}
