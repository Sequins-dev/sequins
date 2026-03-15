import SwiftUI
import Charts
import SequinsData

/// Card component displaying a metric chart with title and current value
struct MetricChartCard: View {
    let title: String
    let value: Double
    let unit: String
    let data: [MetricDataPoint]
    let color: Color
    let chartType: ChartType
    let bucketDuration: TimeInterval?
    let timeRange: SequinsData.TimeRange?
    let onTimeRangeSelected: ((Date, Date) -> Void)?

    init(
        title: String,
        value: Double,
        unit: String,
        data: [MetricDataPoint],
        color: Color,
        chartType: ChartType,
        bucketDuration: TimeInterval? = nil,
        timeRange: SequinsData.TimeRange? = nil,
        onTimeRangeSelected: ((Date, Date) -> Void)? = nil
    ) {
        self.title = title
        self.value = value
        self.unit = unit
        self.data = data
        self.color = color
        self.chartType = chartType
        self.bucketDuration = bucketDuration
        self.timeRange = timeRange
        self.onTimeRangeSelected = onTimeRangeSelected
    }

    enum ChartType {
        case line, area, bar
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            // Header
            HStack(alignment: .center, spacing: 8) {
                Text(title)
                    .font(.subheadline)
                    .fontWeight(.medium)
                    .foregroundColor(.primary)
                Spacer()
                Text(formattedValue)
                    .font(.caption)
                    .fontWeight(.medium)
                    .foregroundColor(.secondary)
            }

            // Chart with hover and drag-to-select
            if !data.isEmpty {
                DraggableChart(
                    data: data,
                    color: color,
                    unit: unit,
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

    private var formattedValue: String {
        if unit == "MB" {
            return String(format: "%.0f %@", value, unit)
        } else if unit == "%" {
            return String(format: "%.1f%@", value, unit)
        } else {
            return String(format: "%.1f %@", value, unit)
        }
    }
}

#Preview("MetricChartCard - CPU") {
    let data = (0..<20).map { i in
        MetricDataPoint(
            timestamp: Date().addingTimeInterval(Double(i) * 60),
            value: Double.random(in: 20...80)
        )
    }

    return MetricChartCard(
        title: "CPU Usage",
        value: 45.2,
        unit: "%",
        data: data,
        color: .blue,
        chartType: .line
    )
    .frame(width: 300)
    .padding()
}

#Preview("MetricChartCard - Memory") {
    let data = (0..<20).map { i in
        MetricDataPoint(
            timestamp: Date().addingTimeInterval(Double(i) * 60),
            value: Double.random(in: 100...256)
        )
    }

    return MetricChartCard(
        title: "Memory Usage",
        value: 178.5,
        unit: "MB",
        data: data,
        color: .green,
        chartType: .area
    )
    .frame(width: 300)
    .padding()
}

#Preview("MetricChartCard - No Data") {
    MetricChartCard(
        title: "Event Loop Delay",
        value: 0,
        unit: "ms",
        data: [],
        color: .orange,
        chartType: .line
    )
    .frame(width: 300)
    .padding()
}
