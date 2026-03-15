import SwiftUI
import Charts
import SequinsData

/// Simplified dynamic chart card
/// TODO: Implement full charting with SeQL data loading
struct DynamicMetricChartCard: View {
    let metric: SequinsData.Metric
    let dataSource: DataSource
    let timeRange: TimeRange
    let onTimeRangeSelected: ((Date, Date) -> Void)?

    init(
        metric: SequinsData.Metric,
        dataSource: DataSource,
        timeRange: TimeRange,
        onTimeRangeSelected: ((Date, Date) -> Void)? = nil
    ) {
        self.metric = metric
        self.dataSource = dataSource
        self.timeRange = timeRange
        self.onTimeRangeSelected = onTimeRangeSelected
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            // Header
            HStack {
                VStack(alignment: .leading, spacing: 2) {
                    Text(metric.name)
                        .font(.caption)
                        .foregroundColor(.secondary)
                    if !metric.description.isEmpty {
                        Text(metric.description)
                            .font(.caption2)
                            .foregroundColor(.secondary.opacity(0.7))
                            .lineLimit(1)
                    }
                }
                Spacer()
                Text("--")
                    .font(.caption)
                    .fontWeight(.medium)
            }

            // Placeholder chart area
            Rectangle()
                .fill(Color.secondary.opacity(0.1))
                .frame(height: 120)
                .overlay(
                    Text("Chart requires full SeQL integration")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                )
        }
        .padding(12)
        .background(Color(NSColor.controlBackgroundColor))
        .cornerRadius(6)
    }
}
