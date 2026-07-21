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
    var columnRoles: [SeQLColumnRole] = []
    /// Fill under the line (area) vs. a plain line.
    var filled: Bool = false
    /// Stack the areas/series (area only).
    var stacked: Bool = false
    /// Presentation overrides (unit, legend, thresholds, series limit, y-scale).
    var options: VisualizationOptions = VisualizationOptions()

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
        VizFormat.isTemporalFirstColumn(columnTypes: columnTypes, rows: rows)
    }

    /// Numeric "value" columns (measures when roles are known, else every column after
    /// the first that carries numbers), capped to `options.seriesLimit` by peak magnitude.
    private var seriesColumns: [(index: Int, name: String)] {
        let cols = VizFormat.valueColumns(columns: columns, rows: rows, roles: columnRoles)
        guard let limit = options.seriesLimit, limit > 0, cols.count > Int(limit) else {
            return cols
        }
        let ranked = cols.sorted { peakMagnitude($0.index) > peakMagnitude($1.index) }
        return Array(ranked.prefix(Int(limit)))
    }

    /// Largest absolute value in a column, for ranking series under `series_limit`.
    private func peakMagnitude(_ index: Int) -> Double {
        var m = 0.0
        for row in rows where index < row.count {
            if let v = VizFormat.numeric(row[index]) { m = max(m, abs(v)) }
        }
        return m
    }

    /// Legend visibility: explicit `options.legend`, else visible only when multi-series.
    private var legendVisibility: Visibility {
        if let l = options.legend { return l ? .visible : .hidden }
        return seriesColumns.count > 1 ? .visible : .hidden
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
            thresholdMarks
        }
        .chartYAxis {
            AxisMarks { value in
                AxisGridLine()
                AxisValueLabel {
                    if let v = value.as(Double.self) { Text(VizFormat.compact(v) + options.unitSuffix) }
                }
            }
        }
        .chartYPresentation(options)
        .chartLegend(legendVisibility)
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
                    Text("\(name): \(VizFormat.number(y))\(options.unitSuffix)").font(.caption2)
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
        Chart {
            ForEach(points) { p in
                LineMark(x: .value(columns.first ?? "x", p.num), y: .value("value", p.y))
                    .foregroundStyle(by: .value("Series", p.series))
                    .interpolationMethod(.monotone)
            }
            thresholdMarks
        }
        .chartYAxis {
            AxisMarks { value in
                AxisGridLine()
                AxisValueLabel { if let v = value.as(Double.self) { Text(VizFormat.compact(v) + options.unitSuffix) } }
            }
        }
        .chartYPresentation(options)
        .chartLegend(legendVisibility)
        .padding(.vertical, 4)
    }

    // MARK: - Thresholds

    /// Horizontal reference lines (SLO/alert boundaries) drawn across the plot.
    @ChartContentBuilder
    private var thresholdMarks: some ChartContent {
        ForEach(Array(options.thresholds.enumerated()), id: \.offset) { _, t in
            RuleMark(y: .value("threshold", t.value))
                .foregroundStyle(thresholdColor(t.color))
                .lineStyle(StrokeStyle(lineWidth: 1, dash: [4, 3]))
                .annotation(position: .top, alignment: .trailing) {
                    if let label = t.label, !label.isEmpty {
                        Text(label)
                            .font(.caption2)
                            .foregroundStyle(thresholdColor(t.color))
                    }
                }
        }
    }

    private func thresholdColor(_ spec: String?) -> Color {
        Color(cssLike: spec) ?? .orange
    }
}

extension Color {
    /// Parse a CSS-like color: a few common names or a `#rgb`/`#rrggbb` hex string.
    /// Returns `nil` for empty/unrecognized input so callers can pick a default.
    init?(cssLike spec: String?) {
        guard let raw = spec?.trimmingCharacters(in: .whitespaces), !raw.isEmpty else { return nil }
        switch raw.lowercased() {
        case "red": self = .red
        case "orange": self = .orange
        case "yellow": self = .yellow
        case "green": self = .green
        case "blue": self = .blue
        case "purple": self = .purple
        case "pink": self = .pink
        case "gray", "grey": self = .gray
        case "black": self = .black
        case "white": self = .white
        default:
            var hex = raw
            if hex.hasPrefix("#") { hex.removeFirst() }
            if hex.count == 3 {
                hex = hex.map { "\($0)\($0)" }.joined()
            }
            guard hex.count == 6, let v = UInt64(hex, radix: 16) else { return nil }
            self = Color(
                .sRGB,
                red: Double((v >> 16) & 0xff) / 255,
                green: Double((v >> 8) & 0xff) / 255,
                blue: Double(v & 0xff) / 255
            )
        }
    }
}
