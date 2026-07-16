import SwiftUI
import Charts
import SequinsData

/// A time-series line/area chart shared by the `line` and `area` viz types (and the
/// Explore time-series shape). When the first column is a timestamp it renders a real
/// temporal x-axis (HH:mm ticks) with a hover crosshair + tooltip; otherwise it plots
/// the numeric first column. Y values are unit-formatted; the legend uses the SeQL
/// aliases (now the real column names).
struct TimeSeriesChart: View {
    let columns: [String]
    let rows: [[Any?]]
    let columnTypes: [NodeTypeLabel]
    /// Fill under the line (area) vs. a plain line.
    var filled: Bool = false
    /// Stack the areas/series (area only).
    var stacked: Bool = false

    private struct Datum: Identifiable {
        let id = UUID()
        let date: Date
        let num: Double
        let series: String
        let y: Double
    }

    @State private var hoverDate: Date?
    @State private var plotFrame: CGRect = .zero

    private var isTemporal: Bool {
        if columnTypes.first == .timestamp { return true }
        // Fall back to the data: the backend now emits time buckets as Arrow timestamps,
        // which arrive as `Date`.
        return rows.first?.first is Date
    }

    /// Numeric "value" columns (every column after the first that carries numbers).
    private var seriesColumns: [(index: Int, name: String)] {
        guard columns.count > 1 else { return [] }
        return (1..<columns.count).compactMap { idx in
            let numeric = rows.contains { $0.count > idx && VizFormat.numeric($0[idx]) != nil }
            return numeric ? (idx, columns[idx]) : nil
        }
    }

    private var data: [Datum] {
        guard !columns.isEmpty else { return [] }
        let cols = seriesColumns
        var out: [Datum] = []
        for row in rows {
            guard let xCell = row.first else { continue }
            let date = VizFormat.date(xCell, column: columns[0]) ?? (xCell as? Date) ?? Date(timeIntervalSince1970: 0)
            let num = VizFormat.numeric(xCell) ?? 0
            for (idx, name) in cols where idx < row.count {
                if let y = VizFormat.numeric(row[idx]) {
                    out.append(Datum(date: date, num: num, series: name, y: y))
                }
            }
        }
        // Sort so each series' line runs left-to-right in time (rows aren't guaranteed
        // to arrive time-ordered), otherwise the polyline zig-zags.
        return out.sorted { isTemporal ? $0.date < $1.date : $0.num < $1.num }
    }

    var body: some View {
        let points = data
        if points.isEmpty {
            VizMessage(icon: "chart.xyaxis.line", text: "No numeric data")
        } else if isTemporal {
            temporalChart(points)
        } else {
            numericChart(points)
        }
    }

    // MARK: - Temporal

    private func temporalChart(_ points: [Datum]) -> some View {
        Chart {
            ForEach(points) { p in
                if filled {
                    AreaMark(
                        x: .value("Time", p.date),
                        y: .value("value", p.y),
                        stacking: stacked ? .standard : .unstacked
                    )
                    .foregroundStyle(by: .value("Series", p.series))
                    .interpolationMethod(.monotone)
                    .opacity(0.55)
                }
                LineMark(x: .value("Time", p.date), y: .value("value", p.y))
                    .foregroundStyle(by: .value("Series", p.series))
                    .interpolationMethod(.monotone)
                if points.count <= 60 {
                    PointMark(x: .value("Time", p.date), y: .value("value", p.y))
                        .foregroundStyle(by: .value("Series", p.series))
                        .symbolSize(18)
                }
            }
            if let hoverDate {
                RuleMark(x: .value("Time", hoverDate))
                    .foregroundStyle(.secondary.opacity(0.4))
                    .lineStyle(StrokeStyle(lineWidth: 1, dash: [3]))
            }
        }
        .chartYAxis {
            AxisMarks { value in
                AxisGridLine()
                AxisValueLabel {
                    if let v = value.as(Double.self) { Text(VizFormat.compact(v)) }
                }
            }
        }
        .chartLegend(seriesColumns.count > 1 ? .visible : .hidden)
        .chartOverlay { proxy in
            GeometryReader { geo in
                let frame = proxy.plotFrame.map { geo[$0] } ?? .zero
                Color.clear
                    .contentShape(Rectangle())
                    .onContinuousHover { phase in
                        switch phase {
                        case .active(let loc):
                            let x = loc.x - frame.origin.x
                            hoverDate = (x >= 0 && x <= frame.width) ? proxy.value(atX: x) as Date? : nil
                        case .ended:
                            hoverDate = nil
                        }
                    }
                if let hoverDate {
                    tooltip(near: hoverDate, points: points, frame: frame, proxy: proxy, size: geo.size)
                        .allowsHitTesting(false)
                }
            }
        }
        .padding(.vertical, 4)
    }

    @ViewBuilder
    private func tooltip(near date: Date, points: [Datum], frame: CGRect, proxy: ChartProxy, size: CGSize) -> some View {
        // Nearest actual x present in the data.
        let uniqueDates = Array(Set(points.map(\.date))).sorted()
        if let nearest = uniqueDates.min(by: { abs($0.timeIntervalSince(date)) < abs($1.timeIntervalSince(date)) }) {
            let atX = points.filter { $0.date == nearest }
            let rows = seriesColumns.map { col -> (String, Double) in
                (col.name, atX.first { $0.series == col.name }?.y ?? 0)
            }
            let xPos = (proxy.position(forX: nearest) ?? 0) + frame.origin.x
            VStack(alignment: .leading, spacing: 3) {
                Text(VizFormat.axisTime(nearest, includeSeconds: true))
                    .font(.caption2).foregroundStyle(.secondary)
                ForEach(rows, id: \.0) { name, y in
                    Text("\(name): \(VizFormat.number(y))").font(.caption2)
                }
            }
            .padding(6)
            .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 6))
            .fixedSize()
            .position(
                x: min(max(xPos, 60), size.width - 60),
                y: 34
            )
        }
    }

    // MARK: - Numeric fallback

    private func numericChart(_ points: [Datum]) -> some View {
        Chart(points) { p in
            LineMark(x: .value(columns.first ?? "x", p.num), y: .value("value", p.y))
                .foregroundStyle(by: .value("Series", p.series))
                .interpolationMethod(.monotone)
        }
        .chartYAxis {
            AxisMarks { value in
                AxisGridLine()
                AxisValueLabel { if let v = value.as(Double.self) { Text(VizFormat.compact(v)) } }
            }
        }
        .chartLegend(seriesColumns.count > 1 ? .visible : .hidden)
        .padding(.vertical, 4)
    }
}
