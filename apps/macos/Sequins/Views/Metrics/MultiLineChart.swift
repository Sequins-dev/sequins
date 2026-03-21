import SwiftUI
import Charts
import SequinsData

/// Data for a single series in a multi-line chart
struct ChartSeries: Identifiable {
    let id: String
    let label: String
    let color: Color
    let data: [MetricDataPoint]
    let bucketDuration: TimeInterval?

    init(id: String, label: String, color: Color, data: [MetricDataPoint], bucketDuration: TimeInterval? = nil) {
        self.id = id
        self.label = label
        self.color = color
        self.data = data
        self.bucketDuration = bucketDuration
    }

    /// Data points segmented by time gaps for gap-aware rendering.
    ///
    /// Uses the larger of the expected bin size and the observed data interval so that
    /// metrics reporting less frequently than the query bin size still draw connected lines.
    /// (e.g. a metric reporting every 30 s with bin=5s would otherwise have a 30 s gap that
    /// exceeds the 7.5 s threshold and renders each point as a disconnected PointMark.)
    var segmentedData: [SegmentedDataPoint] {
        let observed = ChartDataUtils.estimateBucketDuration(from: data)
        let effective = max(bucketDuration ?? 0, observed ?? 0)
        guard effective > 0 else {
            let isSingle = data.count == 1
            return data.map { SegmentedDataPoint(timestamp: $0.timestamp, value: $0.value, segmentId: 0, isSinglePointSegment: isSingle) }
        }
        return ChartDataUtils.segmentDataPoints(data, bucketDuration: effective)
    }
}

/// Interactive chart with multiple lines and hover/drag functionality
struct MultiLineChart: View {
    let series: [ChartSeries]
    let unit: String
    let timeRange: SequinsData.TimeRange?
    let onSelection: (Date, Date) -> Void

    @State private var isDragging = false
    @State private var dragStart: CGFloat = 0
    @State private var dragEnd: CGFloat = 0
    @State private var hoveredTimestamp: Date?
    @State private var hoverLocation: CGPoint = .zero
    @State private var plotAreaFrame: CGRect = .zero
    @State private var now = Date()

    private var yAxisDomain: ClosedRange<Double> {
        let minValue: Double = 0

        if unit == "%" {
            return 0...100
        }

        let allValues = series.flatMap { $0.data.map { $0.value } }
        let maxValue = allValues.max() ?? 100
        let paddedMax = maxValue * 1.1

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

    /// Fallback X-axis domain from data
    private var dataXAxisDomain: ClosedRange<Date> {
        let allDates = series.flatMap { $0.data.map { $0.timestamp } }
        let minDate = allDates.min() ?? Date()
        let maxDate = allDates.max() ?? Date()
        return minDate...maxDate
    }

    var body: some View {
        GeometryReader { _ in
            Chart {
                ForEach(series) { seriesData in
                    ForEach(seriesData.segmentedData) { dataPoint in
                        // Single-point segments need PointMark since LineMark won't render
                        if dataPoint.isSinglePointSegment {
                            PointMark(
                                x: .value("Time", dataPoint.timestamp),
                                y: .value("Value", dataPoint.value)
                            )
                            .foregroundStyle(seriesData.color)
                            .symbolSize(30) // Small dot, roughly the width of a 2pt line
                        } else {
                            LineMark(
                                x: .value("Time", dataPoint.timestamp),
                                y: .value("Value", dataPoint.value),
                                series: .value("Series", "\(seriesData.id)-\(dataPoint.segmentId)")
                            )
                            .foregroundStyle(seriesData.color)
                            .lineStyle(StrokeStyle(lineWidth: 2))
                        }
                    }
                }

                if let timestamp = hoveredTimestamp {
                    RuleMark(x: .value("Time", timestamp))
                        .foregroundStyle(Color.secondary.opacity(0.3))

                    ForEach(series) { seriesData in
                        if let point = closestPoint(in: seriesData.data, to: timestamp) {
                            PointMark(
                                x: .value("Time", point.timestamp),
                                y: .value("Value", point.value)
                            )
                            .foregroundStyle(seriesData.color)
                            .symbolSize(80)
                        }
                    }
                }
            }
            .chartXAxis {
                let domain = xAxisDomain ?? dataXAxisDomain
                let interval = ChartDataUtils.axisTickInterval(for: domain.upperBound.timeIntervalSince(domain.lowerBound))
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
            .animation(.easeInOut(duration: 0.3), value: series.map { $0.data.count })
            .chartYScale(domain: yAxisDomain)
            .chartXScale(domain: xAxisDomain ?? dataXAxisDomain)
            .chartLegend(.hidden)
            .chartPlotStyle { plotArea in
                plotArea
                    .padding(.trailing, 8)
                    .padding(.top, 4)
                    .clipped()
                    .padding(.bottom, 4)
                    .clipped()  // Clip marks to plot area
            }
            .chartOverlay { proxy in
                GeometryReader { geometry in
                    let frame = proxy.plotFrame.map { geometry[$0] } ?? .zero

                    Color.clear
                        .contentShape(Rectangle())
                        .onAppear {
                            plotAreaFrame = frame
                        }
                        .onChange(of: frame) { _, newFrame in
                            plotAreaFrame = newFrame
                        }
                        .onContinuousHover { phase in
                            handleHover(phase: phase, proxy: proxy, plotFrame: frame)
                        }
                        .gesture(createDragGesture(proxy: proxy, plotFrame: frame))

                    // Selection overlay
                    if isDragging && dragStart != dragEnd {
                        Rectangle()
                            .fill(Color.blue.opacity(0.2))
                            .frame(width: abs(dragEnd - dragStart))
                            .position(
                                x: frame.origin.x + min(dragStart, dragEnd) + abs(dragEnd - dragStart) / 2,
                                y: geometry.size.height / 2
                            )
                    }

                    // Tooltip
                    if let timestamp = hoveredTimestamp {
                        tooltipView(timestamp: timestamp, geometrySize: geometry.size)
                            .allowsHitTesting(false)
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

    // MARK: - Interaction Handlers

    private func handleHover(phase: HoverPhase, proxy: ChartProxy, plotFrame: CGRect) {
        switch phase {
        case .active(let location):
            let plotX = location.x - plotFrame.origin.x

            guard plotX >= 0, plotX <= plotFrame.width else {
                hoveredTimestamp = nil
                return
            }

            hoverLocation = location
            hoveredTimestamp = proxy.value(atX: plotX) as Date?

        case .ended:
            hoveredTimestamp = nil
        }
    }

    private func createDragGesture(proxy: ChartProxy, plotFrame: CGRect) -> some Gesture {
        DragGesture()
            .onChanged { value in
                if !isDragging {
                    isDragging = true
                    dragStart = value.startLocation.x - plotFrame.origin.x
                }
                dragEnd = value.location.x - plotFrame.origin.x

                // Clamp to plot bounds
                dragStart = max(0, min(dragStart, plotFrame.width))
                dragEnd = max(0, min(dragEnd, plotFrame.width))
            }
            .onEnded { _ in
                defer {
                    isDragging = false
                    dragStart = 0
                    dragEnd = 0
                }

                let minX = min(dragStart, dragEnd)
                let maxX = max(dragStart, dragEnd)

                guard let startDate: Date = proxy.value(atX: minX),
                      let endDate: Date = proxy.value(atX: maxX),
                      abs(dragEnd - dragStart) > plotFrame.width * 0.05 else {
                    return
                }

                onSelection(startDate, endDate)
            }
    }

    // MARK: - Tooltip

    private func tooltipView(timestamp: Date, geometrySize: CGSize) -> some View {
        // Get the actual data point timestamp from the first series (they should all be aligned)
        let actualTimestamp = series.first.flatMap { closestPoint(in: $0.data, to: timestamp)?.timestamp } ?? timestamp

        return VStack(alignment: .leading, spacing: 4) {
            Text(formatHoverDate(actualTimestamp))
                .font(.caption2)
                .foregroundColor(.secondary)

            ForEach(series) { seriesData in
                if let point = closestPoint(in: seriesData.data, to: timestamp) {
                    HStack(spacing: 4) {
                        Circle()
                            .fill(seriesData.color)
                            .frame(width: 6, height: 6)
                        Text("\(seriesData.label):")
                            .font(.caption2)
                            .foregroundColor(.secondary)
                        Text("\(formatHoverValue(point.value)) \(unit)")
                            .font(.caption2)
                            .fontWeight(.medium)
                    }
                }
            }
        }
        .padding(6)
        .background(Color(NSColor.controlBackgroundColor))
        .cornerRadius(4)
        .overlay(
            RoundedRectangle(cornerRadius: 4)
                .stroke(Color.secondary.opacity(0.3), lineWidth: 1)
        )
        .position(
            x: min(max(hoverLocation.x, 80), geometrySize.width - 80),
            y: max(40, hoverLocation.y - 50)
        )
    }

    // MARK: - Helpers

    private func closestPoint(in data: [MetricDataPoint], to date: Date) -> MetricDataPoint? {
        data.min(by: {
            abs($0.timestamp.timeIntervalSince(date)) < abs($1.timestamp.timeIntervalSince(date))
        })
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

#Preview("MultiLineChart - Event Loop Delays") {
    let baseTime = Date()
    let minData = (0..<20).map { i in
        MetricDataPoint(
            timestamp: baseTime.addingTimeInterval(Double(i) * 60),
            value: Double.random(in: 1...5)
        )
    }
    let meanData = (0..<20).map { i in
        MetricDataPoint(
            timestamp: baseTime.addingTimeInterval(Double(i) * 60),
            value: Double.random(in: 5...15)
        )
    }
    let maxData = (0..<20).map { i in
        MetricDataPoint(
            timestamp: baseTime.addingTimeInterval(Double(i) * 60),
            value: Double.random(in: 15...50)
        )
    }
    let p99Data = (0..<20).map { i in
        MetricDataPoint(
            timestamp: baseTime.addingTimeInterval(Double(i) * 60),
            value: Double.random(in: 30...80)
        )
    }

    return MultiLineChart(
        series: [
            ChartSeries(id: "min", label: "min", color: .green, data: minData),
            ChartSeries(id: "mean", label: "mean", color: .blue, data: meanData),
            ChartSeries(id: "max", label: "max", color: .orange, data: maxData),
            ChartSeries(id: "p99", label: "p99", color: .red, data: p99Data)
        ],
        unit: "ms",
        timeRange: nil,
        onSelection: { _, _ in }
    )
    .frame(height: 200)
    .padding()
}
