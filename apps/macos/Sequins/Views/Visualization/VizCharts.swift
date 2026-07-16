import SwiftUI
import Charts
import SequinsData

// Chart leaf views for the shared visualization component. Each consumes generic
// `columns` + `rows` (+ per-column `columnTypes`), formats via `VizFormat`, and degrades
// gracefully when the data doesn't fit.

/// Max categories a bar/pie shows before folding the tail into "Other".
private let categoryCap = 12

// MARK: - Area

struct AreaChartView: View {
    let columns: [String]
    let rows: [[Any?]]
    var columnTypes: [NodeTypeLabel] = []

    var body: some View {
        TimeSeriesChart(columns: columns, rows: rows, columnTypes: columnTypes,
                        filled: true, stacked: true)
    }
}

// MARK: - Bar / Stacked Bar

struct BarChartView: View {
    let columns: [String]
    let rows: [[Any?]]
    var stacked: Bool = false
    var columnTypes: [NodeTypeLabel] = []

    private struct Bar: Identifiable {
        let id = UUID(); let category: String; let value: Double; let series: String
    }

    /// Numeric value columns.
    private var valueCols: [(index: Int, name: String)] {
        guard columns.count > 1 else { return [] }
        return (1..<columns.count).compactMap { idx in
            rows.contains { $0.count > idx && VizFormat.numeric($0[idx]) != nil } ? (idx, columns[idx]) : nil
        }
    }

    private var singleSeries: Bool { valueCols.count == 1 }

    /// Bars, sorted by total value desc with the tail folded into "Other" (single-series).
    private var bars: [Bar] {
        let cols = valueCols
        guard !cols.isEmpty else { return [] }
        var raw: [(category: String, series: [(String, Double)])] = []
        for row in rows {
            guard let cat = row.first else { continue }
            let category = VizFormat.string(cat)
            let series = cols.compactMap { c -> (String, Double)? in
                guard c.index < row.count, let v = VizFormat.numeric(row[c.index]) else { return nil }
                return (c.name, v)
            }
            raw.append((category, series))
        }
        // Sort + top-N only for the single-series case (where a category ranking is meaningful).
        if singleSeries {
            raw.sort { ($0.series.first?.1 ?? 0) > ($1.series.first?.1 ?? 0) }
            if raw.count > categoryCap {
                let head = Array(raw.prefix(categoryCap))
                let tail = raw.dropFirst(categoryCap)
                let otherTotal = tail.reduce(0.0) { $0 + ($1.series.first?.1 ?? 0) }
                var combined = head
                combined.append((category: "Other", series: [(cols[0].name, otherTotal)]))
                raw = combined
            }
        }
        return raw.flatMap { entry in
            entry.series.map { Bar(category: entry.category, value: $0.1, series: $0.0) }
        }
    }

    var body: some View {
        let bars = self.bars
        if bars.isEmpty {
            VizMessage(icon: "chart.bar", text: "No numeric data")
        } else {
            Chart(bars) { bar in
                let mark = BarMark(
                    x: .value(columns.first ?? "category", bar.category),
                    y: .value("value", bar.value)
                )
                .foregroundStyle(by: .value("Series", bar.series))
                if stacked {
                    mark
                } else {
                    mark.position(by: .value("Series", bar.series))
                }
            }
            .chartYAxis {
                AxisMarks { v in
                    AxisGridLine()
                    AxisValueLabel { if let d = v.as(Double.self) { Text(VizFormat.compact(d)) } }
                }
            }
            .chartXAxis {
                AxisMarks { _ in
                    AxisValueLabel(orientation: bars.count > 6 ? .verticalReversed : .horizontal)
                }
            }
            .chartLegend(valueCols.count > 1 ? .visible : .hidden)
            .padding()
        }
    }
}

// MARK: - Pie / Donut

struct PieChartView: View {
    let columns: [String]
    let rows: [[Any?]]
    var columnTypes: [NodeTypeLabel] = []

    private struct Slice: Identifiable {
        let id = UUID(); let category: String; let value: Double
    }

    /// True when the data looks like a time series (pie is the wrong chart for it).
    private var looksTemporal: Bool {
        columnTypes.first == .timestamp || rows.first?.first is Date
    }

    private var slices: [Slice] {
        guard columns.count >= 2 else { return [] }
        let valueIdx = (1..<columns.count).first { idx in
            rows.contains { $0.count > idx && VizFormat.numeric($0[idx]) != nil }
        } ?? 1
        var out: [Slice] = rows.compactMap { row in
            guard !row.isEmpty, valueIdx < row.count, let v = VizFormat.numeric(row[valueIdx]) else { return nil }
            return Slice(category: VizFormat.string(row.first ?? nil), value: v)
        }
        out.sort { $0.value > $1.value }
        if out.count > categoryCap {
            let head = Array(out.prefix(categoryCap))
            let other = out.dropFirst(categoryCap).reduce(0.0) { $0 + $1.value }
            out = head + [Slice(category: "Other", value: other)]
        }
        return out
    }

    var body: some View {
        let slices = self.slices
        let total = slices.reduce(0.0) { $0 + $1.value }
        if looksTemporal {
            VizMessage(icon: "chart.pie", text: "Pie needs categorical data — try a line or bar chart for a time series.")
        } else if slices.isEmpty || total <= 0 {
            VizMessage(icon: "chart.pie", text: "No data")
        } else {
            Chart(slices) { slice in
                SectorMark(
                    angle: .value("value", slice.value),
                    innerRadius: .ratio(0.6),
                    angularInset: 1.5
                )
                .foregroundStyle(by: .value("category", slice.category))
                .cornerRadius(3)
                .annotation(position: .overlay) {
                    let pct = slice.value / total
                    if pct >= 0.05 {
                        Text(VizFormat.percent(pct * 100))
                            .font(.caption2).bold()
                            .foregroundStyle(.white)
                    }
                }
            }
            .chartBackground { _ in
                // Center KPI (donut hole): the total.
                VStack(spacing: 0) {
                    Text(VizFormat.compact(total)).font(.headline)
                    Text("total").font(.caption2).foregroundStyle(.secondary)
                }
            }
            .padding()
        }
    }
}

// MARK: - Gauge

struct GaugeChartView: View {
    let columns: [String]
    let rows: [[Any?]]
    var columnTypes: [NodeTypeLabel] = []

    private var value: Double? {
        rows.first?.compactMap { VizFormat.numeric($0) }.first
    }

    /// A "max" from a second numeric column named max/limit/total, else a nice upper bound.
    private var upperBound: Double {
        if let row = rows.first {
            for (i, name) in columns.enumerated() where i > 0 {
                let n = name.lowercased()
                if (n.contains("max") || n.contains("limit") || n.contains("total")),
                   i < row.count, let m = VizFormat.numeric(row[i]), m > 0 {
                    return m
                }
            }
        }
        guard let v = value, v > 0 else { return 1 }
        // Round up to a "nice" bound above the value.
        let magnitude = pow(10, floor(log10(v)))
        return (ceil(v / magnitude) * magnitude).nice()
    }

    var body: some View {
        if let value {
            let upper = max(upperBound, value)
            Gauge(value: value, in: 0...upper) {
                Text(VizFormat.label(columns.first ?? "value"))
            } currentValueLabel: {
                Text(VizFormat.number(value))
            } minimumValueLabel: {
                Text(VizFormat.compact(0)).font(.caption2)
            } maximumValueLabel: {
                Text(VizFormat.compact(upper)).font(.caption2)
            }
            .gaugeStyle(.accessoryCircular)
            .tint(Gradient(colors: [.green, .yellow, .orange, .red]))
            .scaleEffect(1.8)
            .frame(maxWidth: .infinity, maxHeight: .infinity)
            .padding(40)
        } else {
            VizMessage(icon: "gauge.medium", text: "No numeric value")
        }
    }
}

private extension Double {
    /// Round a bound up to 1/2/5×10ⁿ.
    func nice() -> Double {
        guard self > 0 else { return 1 }
        let mag = pow(10, floor(log10(self)))
        let norm = self / mag
        let n: Double = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10
        return n * mag
    }
}

// MARK: - Heatmap

struct HeatmapChartView: View {
    let columns: [String]
    let rows: [[Any?]]
    var columnTypes: [NodeTypeLabel] = []

    private struct Cell: Identifiable {
        let id = UUID(); let date: Date?; let x: String; let y: String; let value: Double
    }

    private var isTemporalX: Bool {
        columnTypes.first == .timestamp || rows.first?.first is Date
    }

    private var cells: [Cell] {
        guard columns.count >= 3 else { return [] }
        return rows.compactMap { row in
            guard row.count >= 3, let v = VizFormat.numeric(row[2]) else { return nil }
            return Cell(
                date: VizFormat.date(row[0], column: columns[0]) ?? row[0] as? Date,
                x: VizFormat.string(row[0]),
                y: VizFormat.string(row[1]),
                value: v
            )
        }
    }

    var body: some View {
        let cells = self.cells
        if cells.isEmpty {
            VizMessage(icon: "square.grid.3x3.fill", text: "Heatmap needs 3 columns (x, y, value)")
        } else {
            Chart(cells) { c in
                if isTemporalX, let date = c.date {
                    RectangleMark(
                        x: .value(columns[0], date, unit: .minute),
                        y: .value(columns[1], c.y)
                    )
                    .foregroundStyle(by: .value(columns[2], c.value))
                } else {
                    RectangleMark(x: .value(columns[0], c.x), y: .value(columns[1], c.y))
                        .foregroundStyle(by: .value(columns[2], c.value))
                }
            }
            .chartForegroundStyleScale(
                range: Gradient(colors: [
                    Color(.sRGB, red: 0.1, green: 0.15, blue: 0.35),
                    .blue, .green, .yellow, .orange, .red,
                ])
            )
            .chartLegend(.visible)
            .padding()
        }
    }
}
