import SwiftUI
import Charts
import SequinsData

/// Interactive chart with hover and drag-to-select functionality
struct DraggableChart: View {
    let data: [MetricDataPoint]
    let color: Color
    let unit: String
    let chartType: MetricChartCard.ChartType
    let bucketDuration: TimeInterval?
    let timeRange: SequinsData.TimeRange?
    let onSelection: (Date, Date) -> Void

    /// Data points segmented by time gaps for gap-aware rendering.
    ///
    /// Uses the larger of the expected bin size and the observed data interval so that
    /// metrics reporting less frequently than the query bin size still draw connected lines.
    private var segmentedData: [SegmentedDataPoint] {
        let observed = ChartDataUtils.estimateBucketDuration(from: data)
        let effective = max(bucketDuration ?? 0, observed ?? 0)
        guard effective > 0 else {
            let isSingle = data.count == 1
            return data.map { SegmentedDataPoint(timestamp: $0.timestamp, value: $0.value, segmentId: 0, isSinglePointSegment: isSingle) }
        }
        return ChartDataUtils.segmentDataPoints(data, bucketDuration: effective)
    }

    @State private var isDragging = false
    @State private var dragStart: CGFloat = 0
    @State private var dragEnd: CGFloat = 0
    @State private var hoveredPoint: MetricDataPoint?
    @State private var hoverLocation: CGPoint = .zero
    @State private var plotAreaFrame: CGRect = .zero
    @State private var now = Date()

    private var yAxisDomain: ClosedRange<Double> {
        // Always start from 0 for honest visualization
        let minValue: Double = 0

        // For percentages, always show 0-100%
        if unit == "%" {
            return 0...100
        }

        // For other units, show from 0 to max value with some padding
        let values = data.map { $0.value }
        let maxValue = values.max() ?? 100
        let paddedMax = maxValue * 1.1 // Add 10% padding above max

        return minValue...paddedMax
    }

    /// X-axis domain based on the selected time range.
    /// For live (relative) ranges, reads `now` to create a SwiftUI dependency
    /// so the domain recomputes every second as the timer fires.
    private var xAxisDomain: ClosedRange<Date>? {
        guard let timeRange = timeRange else { return nil }
        if timeRange.isLive { _ = now }
        let bounds = timeRange.bounds
        return bounds.start...bounds.end
    }

    var body: some View {
        GeometryReader { geometry in
            ZStack {
                // Chart with axes
                Chart {
                    ForEach(segmentedData) { dataPoint in
                        // Single-point segments need PointMark since LineMark won't render
                        if dataPoint.isSinglePointSegment {
                            PointMark(
                                x: .value("Time", dataPoint.timestamp),
                                y: .value("Value", dataPoint.value)
                            )
                            .foregroundStyle(color)
                            .symbolSize(30) // Small dot, roughly the width of a 2pt line
                        } else {
                            switch chartType {
                            case .line:
                                LineMark(
                                    x: .value("Time", dataPoint.timestamp),
                                    y: .value("Value", dataPoint.value),
                                    series: .value("Segment", dataPoint.segmentId)
                                )
                                .foregroundStyle(color)
                                .lineStyle(StrokeStyle(lineWidth: 2))
                            case .area:
                                AreaMark(
                                    x: .value("Time", dataPoint.timestamp),
                                    y: .value("Value", dataPoint.value),
                                    series: .value("Segment", dataPoint.segmentId)
                                )
                                .foregroundStyle(color.opacity(0.3))

                                LineMark(
                                    x: .value("Time", dataPoint.timestamp),
                                    y: .value("Value", dataPoint.value),
                                    series: .value("Segment", dataPoint.segmentId)
                                )
                                .foregroundStyle(color)
                                .lineStyle(StrokeStyle(lineWidth: 2))
                            case .bar:
                                BarMark(
                                    x: .value("Time", dataPoint.timestamp),
                                    y: .value("Value", dataPoint.value)
                                )
                                .foregroundStyle(color)
                            }
                        }
                    }

                    // Hover indicator
                    if let hoveredPoint = hoveredPoint {
                        PointMark(
                            x: .value("Time", hoveredPoint.timestamp),
                            y: .value("Value", hoveredPoint.value)
                        )
                        .foregroundStyle(color)
                        .symbolSize(100)

                        RuleMark(x: .value("Time", hoveredPoint.timestamp))
                            .foregroundStyle(Color.secondary.opacity(0.3))
                    }
                }
                .chartXAxis {
                    let domain = xAxisDomain ?? (data.first?.timestamp ?? Date())...(data.last?.timestamp ?? Date())
                    let interval = ChartDataUtils.axisTickInterval(for: domain.upperBound.timeIntervalSince(domain.lowerBound))
                    // Suppress labels within 30% of a tick interval from the left edge so
                    // they don't slide behind the y-axis labels as the domain scrolls.
                    let labelSafeStart = domain.lowerBound.addingTimeInterval(interval * 0.3)
                    AxisMarks(values: ChartDataUtils.axisTicks(for: domain)) { value in
                        AxisGridLine()
                            .foregroundStyle(Color.secondary.opacity(0.2))
                        let date = value.as(Date.self)
                        let showLabel = date.map { $0 >= labelSafeStart } ?? false
                        if showLabel, let d = date {
                            AxisValueLabel {
                                Text(formatAxisDate(d))
                                    .font(.caption2)
                                    .foregroundColor(.secondary)
                            }
                        }
                    }
                }
                .chartYAxis {
                    AxisMarks(position: .leading) { value in
                        AxisGridLine()
                            .foregroundStyle(Color.secondary.opacity(0.2))
                        AxisValueLabel {
                            if let doubleValue = value.as(Double.self) {
                                Text(formatAxisValue(doubleValue))
                                    .font(.caption2)
                                    .foregroundColor(.secondary)
                            }
                        }
                    }
                }
                .animation(.easeInOut(duration: 0.3), value: data.count)
                .chartYScale(domain: yAxisDomain)
                .chartXScale(domain: xAxisDomain ?? (data.first?.timestamp ?? Date())...(data.last?.timestamp ?? Date()))
                .chartPlotStyle { plotArea in
                    plotArea
                        .padding(.trailing, 8)
                        .padding(.top, 4)
                        .padding(.bottom, 4)
                        .clipped()
                }
                .chartOverlay { proxy in
                    GeometryReader { overlayGeometry in
                        let frame = proxy.plotFrame.map { overlayGeometry[$0] } ?? .zero

                        Color.clear
                            .contentShape(Rectangle())
                            .onAppear {
                                plotAreaFrame = frame
                            }
                            .onChange(of: frame) { _, newFrame in
                                plotAreaFrame = newFrame
                            }
                            .onContinuousHover { phase in
                                switch phase {
                                case .active(let location):
                                    hoverLocation = location
                                    // Convert view coordinates to plot coordinates before querying proxy
                                    let plotX = location.x - frame.origin.x
                                    let plotY = location.y - frame.origin.y

                                    // Ensure we're within plot bounds
                                    guard plotX >= 0, plotX <= frame.width,
                                          plotY >= 0, plotY <= frame.height else {
                                        hoveredPoint = nil
                                        return
                                    }

                                    // Use ChartProxy to convert plot position to date value
                                    if let date: Date = proxy.value(atX: plotX) {
                                        hoveredPoint = data.min(by: {
                                            abs($0.timestamp.timeIntervalSince(date)) < abs($1.timestamp.timeIntervalSince(date))
                                        })
                                    } else {
                                        hoveredPoint = nil
                                    }
                                case .ended:
                                    hoveredPoint = nil
                                }
                            }
                            .gesture(
                                DragGesture()
                                    .onChanged { value in
                                        if !isDragging {
                                            isDragging = true
                                            dragStart = value.startLocation.x
                                        }
                                        dragEnd = value.location.x
                                    }
                                    .onEnded { _ in
                                        isDragging = false

                                        // Use ChartProxy to convert drag positions to dates
                                        if let startDate: Date = proxy.value(atX: min(dragStart, dragEnd)),
                                           let endDate: Date = proxy.value(atX: max(dragStart, dragEnd)) {
                                            // Only trigger if selection is meaningful (>5% of chart)
                                            if abs(dragEnd - dragStart) > overlayGeometry.size.width * 0.05 {
                                                onSelection(startDate, endDate)
                                            }
                                        }

                                        dragStart = 0
                                        dragEnd = 0
                                    }
                            )

                        // Selection overlay
                        if isDragging && dragStart != dragEnd {
                            Rectangle()
                                .fill(Color.blue.opacity(0.2))
                                .frame(width: abs(dragEnd - dragStart))
                                .position(
                                    x: min(dragStart, dragEnd) + abs(dragEnd - dragStart) / 2,
                                    y: overlayGeometry.size.height / 2
                                )
                        }

                        // Hover tooltip - position at the actual data point, not mouse position
                        if let hoveredPoint = hoveredPoint,
                           let pointX = proxy.position(forX: hoveredPoint.timestamp),
                           let pointY = proxy.position(forY: hoveredPoint.value) {
                            VStack(alignment: .leading, spacing: 2) {
                                Text(formatHoverDate(hoveredPoint.timestamp))
                                    .font(.caption2)
                                    .foregroundColor(.secondary)
                                Text("\(formatHoverValue(hoveredPoint.value)) \(unit)")
                                    .font(.caption)
                                    .fontWeight(.medium)
                            }
                            .padding(6)
                            .background(Color(NSColor.controlBackgroundColor))
                            .cornerRadius(4)
                            .overlay(
                                RoundedRectangle(cornerRadius: 4)
                                    .stroke(Color.secondary.opacity(0.3), lineWidth: 1)
                            )
                            .position(
                                x: plotAreaFrame.origin.x + pointX,
                                y: max(20, plotAreaFrame.origin.y + pointY - 30)
                            )
                            .allowsHitTesting(false)
                        }
                    }
                }
            }
        }
        .task(id: timeRange?.isLive) {
            // Advance `now` every second with a 1-second linear animation so the
            // x-axis domain tweens continuously instead of jumping once per second.
            guard timeRange?.isLive == true else { return }
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                withAnimation(.linear(duration: 1.0)) {
                    now = Date()
                }
            }
        }
    }

    private func formatAxisDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm"
        return formatter.string(from: date)
    }

    private func formatAxisValue(_ value: Double) -> String {
        switch unit {
        case "%":
            return String(format: "%.0f%%", value)
        case "B":
            let abs = Swift.abs(value)
            if abs >= 1_073_741_824 { return String(format: "%.1f GB", value / 1_073_741_824) }
            if abs >= 1_048_576     { return String(format: "%.1f MB", value / 1_048_576) }
            if abs >= 1_024         { return String(format: "%.0f KB", value / 1_024) }
            return String(format: "%.0f B", value)
        case "KB":
            let abs = Swift.abs(value)
            if abs >= 1_048_576 { return String(format: "%.1f GB", value / 1_048_576) }
            if abs >= 1_024     { return String(format: "%.1f MB", value / 1_024) }
            return String(format: "%.0f KB", value)
        case "MB":
            if Swift.abs(value) >= 1_024 { return String(format: "%.1f GB", value / 1_024) }
            return String(format: "%.0f MB", value)
        case "GB":
            return String(format: "%.1f GB", value)
        case "s":
            let abs = Swift.abs(value)
            if abs < 0.000_001 { return String(format: "%.0f ns", value * 1_000_000_000) }
            if abs < 0.001     { return String(format: "%.0f µs", value * 1_000_000) }
            if abs < 1         { return String(format: "%.0f ms", value * 1_000) }
            return String(format: "%.1f s", value)
        case "ms":
            let abs = Swift.abs(value)
            if abs < 1       { return String(format: "%.0f µs", value * 1_000) }
            if abs >= 1_000  { return String(format: "%.1f s", value / 1_000) }
            return String(format: "%.0f ms", value)
        case "min":
            return String(format: "%.1f min", value)
        default:
            let abs = Swift.abs(value)
            if abs >= 100_000_000 { return String(format: "%.1fB", value / 1_000_000_000) }
            if abs >= 100_000     { return String(format: "%.1fM", value / 1_000_000) }
            if abs >= 1_000       { return String(format: "%.1fk", value / 1_000) }
            if abs >= 1           { return String(format: "%.1f", value) }
            return String(format: "%.2f", value)
        }
    }

    private func formatHoverDate(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss"
        return formatter.string(from: date)
    }

    private func formatHoverValue(_ value: Double) -> String {
        switch unit {
        case "%":
            return String(format: "%.1f%%", value)
        case "B":
            let abs = Swift.abs(value)
            if abs >= 1_073_741_824 { return String(format: "%.2f GB", value / 1_073_741_824) }
            if abs >= 1_048_576     { return String(format: "%.2f MB", value / 1_048_576) }
            if abs >= 1_024         { return String(format: "%.1f KB", value / 1_024) }
            return String(format: "%.0f B", value)
        case "KB":
            let abs = Swift.abs(value)
            if abs >= 1_048_576 { return String(format: "%.2f GB", value / 1_048_576) }
            if abs >= 1_024     { return String(format: "%.2f MB", value / 1_024) }
            return String(format: "%.1f KB", value)
        case "MB":
            if Swift.abs(value) >= 1_024 { return String(format: "%.2f GB", value / 1_024) }
            return String(format: "%.1f MB", value)
        case "GB":
            return String(format: "%.2f GB", value)
        case "s":
            let abs = Swift.abs(value)
            if abs < 0.000_001 { return String(format: "%.0f ns", value * 1_000_000_000) }
            if abs < 0.001     { return String(format: "%.1f µs", value * 1_000_000) }
            if abs < 1         { return String(format: "%.1f ms", value * 1_000) }
            return String(format: "%.2f s", value)
        case "ms":
            let abs = Swift.abs(value)
            if abs < 1       { return String(format: "%.1f µs", value * 1_000) }
            if abs >= 1_000  { return String(format: "%.2f s", value / 1_000) }
            return String(format: "%.1f ms", value)
        case "min":
            return String(format: "%.1f min", value)
        default:
            let abs = Swift.abs(value)
            if abs >= 100_000_000 { return String(format: "%.2fB", value / 1_000_000_000) }
            if abs >= 100_000     { return String(format: "%.2fM", value / 1_000_000) }
            if abs >= 1_000       { return String(format: "%.1fk", value / 1_000) }
            if abs >= 1           { return String(format: "%.2f", value) }
            return String(format: "%.3f", value)
        }
    }
}

#Preview("DraggableChart - Line") {
    let data = (0..<20).map { i in
        MetricDataPoint(
            timestamp: Date().addingTimeInterval(Double(i) * 60),
            value: Double.random(in: 20...80)
        )
    }

    return DraggableChart(
        data: data,
        color: .blue,
        unit: "%",
        chartType: .line,
        bucketDuration: 60,
        timeRange: nil,
        onSelection: { _, _ in }
    )
    .frame(height: 200)
    .padding()
}

#Preview("DraggableChart - Area") {
    let data = (0..<20).map { i in
        MetricDataPoint(
            timestamp: Date().addingTimeInterval(Double(i) * 60),
            value: Double.random(in: 50...200)
        )
    }

    return DraggableChart(
        data: data,
        color: .green,
        unit: "MB",
        chartType: .area,
        bucketDuration: 60,
        timeRange: nil,
        onSelection: { _, _ in }
    )
    .frame(height: 200)
    .padding()
}
