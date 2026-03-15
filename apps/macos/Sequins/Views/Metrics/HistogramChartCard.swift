import SwiftUI
import Charts
import SequinsData

/// Chart card for displaying histogram metrics as a heat map.
struct HistogramChartCard: View {
    let line: HistogramLine
    let timeRange: SequinsData.TimeRange?
    let onTimeRangeSelected: ((Date, Date) -> Void)?

    @Environment(AppStateViewModel.self) private var appState

    private var latestSnapshot: HistogramSnapshot? {
        line.snapshots.max(by: { $0.timestamp < $1.timestamp })
    }

    private var observationRate: Double? {
        guard let snap = latestSnapshot, snap.count > 0 else { return nil }
        // Rate in obs/sec — use count from the latest snapshot as a rough proxy
        return Double(snap.count)
    }

    private var meanValue: Double? {
        latestSnapshot?.mean
    }

    private var tooltipText: String {
        var parts: [String] = []
        if !line.description.isEmpty {
            parts.append(line.description)
        } else {
            parts.append("histogram: \(line.name)")
            parts.append("Service: \(line.serviceName)")
        }
        if let mean = meanValue {
            parts.append("Mean: \(formatValue(mean))")
        }
        if let snap = latestSnapshot {
            parts.append("Count: \(snap.count)")
        }
        return parts.joined(separator: "\n")
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            // Header
            HStack(alignment: .center, spacing: 8) {
                MetricTypePill(metricType: .histogram)

                Text(line.name)
                    .font(.subheadline)
                    .fontWeight(.medium)
                    .foregroundColor(.primary)
                    .help(tooltipText)

                Spacer()

                if let mean = meanValue {
                    Text("μ \(formatValue(mean))\(line.unit.isEmpty ? "" : " \(line.unit)")")
                        .font(.caption)
                        .foregroundColor(.secondary)
                }
            }

            // Heat map chart
            HeatMapChart(
                snapshots: line.snapshots,
                maxActiveBucket: line.maxActiveBucket,
                unit: line.unit,
                timeRange: timeRange
            )
            .frame(height: 120)
        }
        .padding()
        .background(Color(NSColor.controlBackgroundColor))
        .cornerRadius(8)
    }

    private func formatValue(_ value: Double) -> String {
        if abs(value) >= 1_000_000 { return String(format: "%.2fM", value / 1_000_000) }
        if abs(value) >= 1_000 { return String(format: "%.1fk", value / 1_000) }
        if abs(value) < 0.01 && value != 0 { return String(format: "%.4f", value) }
        return String(format: "%.2f", value)
    }
}
