import SwiftUI
import Charts
import SequinsData

/// Chart card for displaying metric data points with DraggableChart
struct StaticMetricChartCard: View {
    let metric: SequinsData.Metric
    let timeRange: SequinsData.TimeRange?
    let onTimeRangeSelected: ((Date, Date) -> Void)?

    @Environment(AppStateViewModel.self) private var appState
    @Environment(MetricsViewModel.self) private var viewModel

    // Data points for this metric (from SequinsData)
    private var dataPoints: [SequinsData.MetricDataPoint] {
        viewModel.getDataPoints(forMetricId: metric.id)
    }

    // Convert to local MetricDataPoint with unit scaling
    private var scaledDataPoints: [MetricDataPoint] {
        let (scaledPoints, _) = applyUnitScaling(dataPoints, unit: metric.unit)
        return scaledPoints.map { dataPoint in
            MetricDataPoint(
                timestamp: dataPoint.timestamp,
                value: dataPoint.value,
                containerId: nil
            )
        }
    }

    // Latest value (scaled)
    private var latestScaledValue: Double? {
        scaledDataPoints.max(by: { $0.timestamp < $1.timestamp })?.value
    }

    // Display title (service-qualified if not unique)
    private var displayTitle: String {
        return "\(metric.serviceName): \(metric.name)"
    }

    // Chart color based on metric name/type
    private var chartColor: Color {
        if metric.name.contains("cpu") || metric.name.contains("utilization") {
            return .blue
        } else if metric.name.contains("memory") || metric.name.contains("heap") {
            return .green
        } else if metric.name.contains("delay") || metric.name.contains("latency") {
            return .orange
        } else if metric.name.contains("error") {
            return .red
        } else {
            return .purple
        }
    }

    // Chart type based on metric type
    private var chartType: MetricChartCard.ChartType {
        switch metric.metricType {
        case .counter:
            return .area
        case .histogram, .summary:
            return .bar
        default:
            return .line
        }
    }

    // Bucket duration from viewModel
    private var bucketDuration: TimeInterval? {
        viewModel.getEffectiveBucketDuration(forMetricId: metric.id)
    }

    // Scaled unit
    private var displayUnit: String {
        let (_, scaledUnit) = applyUnitScaling(dataPoints, unit: metric.unit)
        return scaledUnit
    }

    // Metric type as string (lowercase)
    private var metricTypeString: String {
        switch metric.metricType {
        case .gauge: return "gauge"
        case .counter: return "counter"
        case .histogram: return "histogram"
        case .summary: return "summary"
        }
    }

    // Tooltip text with description and current value
    private var tooltipText: String {
        var parts: [String] = []

        // Add description if available
        if !metric.description.isEmpty {
            parts.append(metric.description)
        } else {
            // Fallback to showing the full name with type and service
            parts.append("\(metricTypeString): \(metric.name)")
            parts.append("Service: \(metric.serviceName)")
        }

        // Add current value
        if let value = latestScaledValue {
            parts.append("Current: \(formatValue(value, unit: displayUnit))")
        }

        return parts.joined(separator: "\n")
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            // Header with type pill and title
            HStack(alignment: .center, spacing: 8) {
                MetricTypePill(metricType: metric.metricType)

                Text(metric.name)
                    .font(.subheadline)
                    .fontWeight(.medium)
                    .foregroundColor(.primary)
                    .contentShape(Rectangle())  // Define hoverable region for tooltip
                    .help(tooltipText)

                Spacer()
            }

            // Chart with hover and drag-to-select
            if !scaledDataPoints.isEmpty {
                DraggableChart(
                    data: scaledDataPoints,
                    color: chartColor,
                    unit: displayUnit,
                    chartType: chartType,
                    bucketDuration: bucketDuration,
                    timeRange: timeRange,
                    onSelection: { start, end in
                        onTimeRangeSelected?(start, end)
                    }
                )
                .frame(height: 120)
            } else {
                Rectangle()
                    .fill(Color.gray.opacity(0.1))
                    .frame(height: 120)
                    .overlay(
                        Text("No data")
                            .foregroundColor(.secondary)
                            .font(.caption)
                    )
            }
        }
        .padding()
        .background(Color(NSColor.controlBackgroundColor))
        .cornerRadius(8)
    }

    // MARK: - Formatting

    private func formatValue(_ value: Double, unit: String) -> String {
        if unit == "MB" {
            return String(format: "%.0f %@", value, unit)
        } else if unit == "%" {
            return String(format: "%.1f%@", value, unit)
        } else {
            return String(format: "%.1f %@", value, unit)
        }
    }

    // MARK: - Unit Scaling

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
