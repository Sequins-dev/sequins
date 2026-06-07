@preconcurrency import SwiftUI
import Charts
import SequinsData

// MARK: - Column data model

/// One pre-computed time column. Created once per export interval and never modified.
/// The `ratios` and `bucketCounts` arrays are stable — only the x-position derived
/// from the timestamp changes when the visible window slides.
private struct HeatMapColumn: Sendable {
    let timestamp: Date
    /// Time boundaries for this column (used by Canvas to compute pixel width).
    let timeStart: Date
    let timeEnd: Date
    /// Per-bucket normalized opacity (0...1), indexed by bucket.
    let ratios: [Double]
    /// Raw per-bucket delta counts for tooltip display.
    let bucketCounts: [UInt64]
}

/// Metadata computed once per layout rebuild and shared between the Canvas draw
/// call and the axis configuration. Columns inside are independently stable.
private struct HeatMapMeta: Sendable {
    let columns: [HeatMapColumn]        // oldest-first, non-overlapping time bins
    let numBuckets: Int                 // highest rendered bucket index + 1
    let firstVisibleBucket: Int         // 0 normally; 1 when underflow bucket is hidden
    let boundsRef: [Double]             // explicit bucket upper bounds for labels
    let axisMarkLabels: [Double: String]
    let yDomain: ClosedRange<Double>
    /// Representative snapshot for tooltip (delta, newest snapshot per column).
    let sortedSnapshots: [HistogramSnapshot]
}

// MARK: - HeatMapChart

/// Grafana-style histogram heat map.
///
/// Architecture:
///  - Column data is computed ONCE per export in a background task. Each column
///    stores per-bucket ratios; existing columns are never touched when new data
///    arrives or when the visible window slides.
///  - Rendering uses `chartBackground` + `Canvas` (CoreGraphics). On each `now`
///    tick the Canvas redraws from the stable column array using fresh x-positions
///    from the `ChartProxy` — no SwiftUI mark re-layout, no animation overhead.
///  - The `Chart` shell contains only two invisible `PointMark`s whose sole job is
///    to drive the x/y scale machinery for axis labels and hover hit-testing.
struct HeatMapChart: View {
    let snapshots: [HistogramSnapshot]
    let maxActiveBucket: Int
    let unit: String
    let timeRange: SequinsData.TimeRange?

    @State private var meta: HeatMapMeta?
    @State private var now = Date()
    @State private var hoveredSnapshot: HistogramSnapshot?
    @State private var hoveredBucketIndex: Int?

    // MARK: - Static formatters

    private static let axisFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "HH:mm"
        return f
    }()

    private static let tooltipFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "HH:mm:ss"
        return f
    }()

    // MARK: - Layout key (cache invalidation)

    private struct LayoutKey: Equatable {
        let count: Int
        let lastTimestamp: Double
        let maxActiveBucket: Int
    }
    private var layoutKey: LayoutKey {
        LayoutKey(
            count: snapshots.count,
            lastTimestamp: snapshots.last?.timestamp.timeIntervalSince1970 ?? 0,
            maxActiveBucket: maxActiveBucket
        )
    }

    // MARK: - Computed x-axis domain

    private func xAxisDomain(meta: HeatMapMeta) -> ClosedRange<Date> {
        if let tr = timeRange {
            if tr.isLive { _ = now }
            return tr.bounds.start...tr.bounds.end
        }
        guard let first = meta.columns.first, let last = meta.columns.last else {
            return Date()...Date()
        }
        return first.timeStart...last.timeEnd
    }

    // MARK: - Background layout build

    /// Maximum number of time columns to render. Caps mark count at ~maxColumns × numBuckets.
    private static let maxColumns = 80

    nonisolated private static func buildMeta(
        snapshots: [HistogramSnapshot],
        maxActiveBucket: Int,
        unit: String
    ) -> HeatMapMeta? {
        guard !snapshots.isEmpty else { return nil }

        let rawSorted = snapshots.sorted { $0.timestamp < $1.timestamp }

        // Bin width: target ≤ maxColumns time columns.
        let span = rawSorted.count > 1
            ? rawSorted.last!.timestamp.timeIntervalSince(rawSorted.first!.timestamp)
            : 1.0
        let binSecs = max(5.0, ceil(span / Double(maxColumns)))

        // Merge snapshots within each bin (same-series, same export interval).
        let merged: [HistogramSnapshot] = {
            var result: [HistogramSnapshot] = []
            var group: [HistogramSnapshot] = [rawSorted[0]]
            for i in 1..<rawSorted.count {
                let cur = rawSorted[i]
                if abs(cur.timestamp.timeIntervalSince(group[0].timestamp)) < binSecs {
                    group.append(cur)
                } else {
                    result.append(mergeGroup(group))
                    group = [cur]
                }
            }
            result.append(mergeGroup(group))
            return result
        }()

        // Convert cumulative snapshots to per-bin delta counts.
        let deltas: [HistogramSnapshot] = {
            guard merged.count > 1 else { return merged }
            var out: [HistogramSnapshot] = [merged[0]]
            for i in 1..<merged.count {
                let cur = merged[i], prev = merged[i - 1]
                let n = max(cur.bucketCounts.count, prev.bucketCounts.count)
                var dc = [UInt64](repeating: 0, count: n)
                for j in 0..<n {
                    let c = j < cur.bucketCounts.count ? cur.bucketCounts[j] : 0
                    let p = j < prev.bucketCounts.count ? prev.bucketCounts[j] : 0
                    dc[j] = c >= p ? c - p : c
                }
                out.append(HistogramSnapshot(
                    timestamp: cur.timestamp,
                    count: cur.count >= prev.count ? cur.count - prev.count : cur.count,
                    sum: cur.sum >= prev.sum ? cur.sum - prev.sum : cur.sum,
                    bucketCounts: dc,
                    explicitBounds: cur.explicitBounds
                ))
            }
            return out
        }()

        // Bucket geometry
        let boundsRef = deltas.max(by: { $0.explicitBounds.count < $1.explicitBounds.count })?.explicitBounds ?? []
        let allFinite = boundsRef.filter { $0.isFinite }
        guard allFinite.count >= 2 else { return nil }

        let totalBuckets = allFinite.count + 1
        let numBuckets = max(2, min(maxActiveBucket + 1, totalBuckets))
        let firstVisibleBucket = (allFinite.first.map { $0 <= 0 } ?? false) ? 1 : 0

        // Column time boundaries (midpoint between adjacent column timestamps).
        let n = deltas.count
        var starts = [Date](repeating: deltas[0].timestamp, count: n)
        var ends   = [Date](repeating: deltas[0].timestamp, count: n)
        if n == 1 {
            starts[0] = deltas[0].timestamp.addingTimeInterval(-binSecs / 2)
            ends[0]   = deltas[0].timestamp.addingTimeInterval( binSecs / 2)
        } else {
            for i in 0..<n {
                let prevT = i == 0
                    ? deltas[0].timestamp.addingTimeInterval(-binSecs)
                    : deltas[i - 1].timestamp
                let nextT = i == n - 1
                    ? deltas[n-1].timestamp.addingTimeInterval(binSecs)
                    : deltas[i + 1].timestamp
                starts[i] = Date(timeIntervalSince1970: (prevT.timeIntervalSince1970 + deltas[i].timestamp.timeIntervalSince1970) / 2)
                ends[i]   = Date(timeIntervalSince1970: (deltas[i].timestamp.timeIntervalSince1970 + nextT.timeIntervalSince1970) / 2)
            }
        }

        // Build one column per delta snapshot (index 0 is baseline — skip it).
        var columns: [HeatMapColumn] = []
        columns.reserveCapacity(n - 1)
        for i in 1..<n {
            let snap = deltas[i]
            let segMax = snap.bucketCounts.prefix(numBuckets).dropFirst(firstVisibleBucket).max() ?? 1
            var ratios = [Double](repeating: 0, count: numBuckets)
            for b in firstVisibleBucket..<numBuckets {
                let cnt = b < snap.bucketCounts.count ? snap.bucketCounts[b] : 0
                ratios[b] = segMax > 0 ? Double(cnt) / Double(segMax) : 0
            }
            columns.append(HeatMapColumn(
                timestamp: snap.timestamp,
                timeStart: starts[i],
                timeEnd: ends[i],
                ratios: ratios,
                bucketCounts: snap.bucketCounts
            ))
        }
        guard !columns.isEmpty else { return nil }

        // Y-axis labels
        let visibleBoundCount = min(allFinite.count, numBuckets - 1)
        let step = max(1, visibleBoundCount / 5)
        let (mult, sym) = timeScaleStatic(unit: unit, bounds: allFinite)
        let labels: [Double: String] = Dictionary(uniqueKeysWithValues:
            stride(from: max(1, firstVisibleBucket + 1), through: visibleBoundCount, by: step).map { i in
                let v = allFinite[i - 1] * mult
                let num = formatStatic(v)
                return (Double(i), sym.isEmpty ? num : "\(num) \(sym)")
            }
        )

        return HeatMapMeta(
            columns: columns,
            numBuckets: numBuckets,
            firstVisibleBucket: firstVisibleBucket,
            boundsRef: allFinite,
            axisMarkLabels: labels,
            yDomain: Double(firstVisibleBucket)...Double(numBuckets),
            sortedSnapshots: deltas
        )
    }

    nonisolated private static func mergeGroup(_ group: [HistogramSnapshot]) -> HistogramSnapshot {
        guard group.count > 1 else { return group[0] }
        let ref = group.max(by: { $0.explicitBounds.count < $1.explicitBounds.count })!
        let n = ref.bucketCounts.count
        var counts = [UInt64](repeating: 0, count: n)
        var total: UInt64 = 0; var sum: Double = 0
        for s in group {
            total += s.count; sum += s.sum
            for (i, c) in s.bucketCounts.enumerated() where i < n { counts[i] += c }
        }
        return HistogramSnapshot(timestamp: ref.timestamp, count: total, sum: sum,
                                 bucketCounts: counts, explicitBounds: ref.explicitBounds)
    }

    // MARK: - Body

    var body: some View {
        Group {
            if let m = meta {
                chartView(meta: m)
            } else {
                Rectangle()
                    .fill(Color.gray.opacity(0.1))
                    .overlay(Text("No data").foregroundColor(.secondary).font(.caption))
            }
        }
        .task(id: layoutKey) {
            let snaps = snapshots, maxB = maxActiveBucket, u = unit
            let result = await Task.detached(priority: .userInitiated) {
                HeatMapChart.buildMeta(snapshots: snaps, maxActiveBucket: maxB, unit: u)
            }.value
            meta = result
        }
        .task(id: timeRange?.isLive) {
            guard timeRange?.isLive == true else { return }
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                // Updating `now` triggers a Canvas redraw — the columns slide left
                // purely via updated ChartProxy x-positions; no column recomputation.
                now = Date()
            }
        }
    }

    // MARK: - Chart shell + Canvas renderer

    @ViewBuilder
    private func chartView(meta: HeatMapMeta) -> some View {
        let domain = xAxisDomain(meta: meta)

        Chart {
            // Two invisible anchors drive x/y scale — zero rendering cost.
            if let first = meta.columns.first, let last = meta.columns.last {
                PointMark(x: .value("T", first.timeStart), y: .value("B", Double(meta.firstVisibleBucket)))
                    .opacity(0)
                PointMark(x: .value("T", last.timeEnd), y: .value("B", Double(meta.numBuckets)))
                    .opacity(0)
            }
            // Hover crosshair
            if let snap = hoveredSnapshot {
                RuleMark(x: .value("Hover", snap.timestamp))
                    .foregroundStyle(Color.white.opacity(0.6))
                    .lineStyle(StrokeStyle(lineWidth: 1))
            }
        }
        .chartXScale(domain: domain)
        .chartYScale(domain: meta.yDomain)
        .chartXAxis {
            let interval = ChartDataUtils.axisTickInterval(for: domain.upperBound.timeIntervalSince(domain.lowerBound))
            let labelSafeStart = domain.lowerBound.addingTimeInterval(interval * 0.3)
            AxisMarks(values: ChartDataUtils.axisTicks(for: domain)) { value in
                AxisGridLine().foregroundStyle(Color.secondary.opacity(0.2))
                let date = value.as(Date.self)
                if date.map({ $0 >= labelSafeStart }) ?? false, let d = date {
                    AxisValueLabel {
                        Text(Self.axisFormatter.string(from: d))
                            .font(.caption2)
                            .foregroundColor(.secondary)
                    }
                }
            }
        }
        .chartYAxis {
            AxisMarks(position: .leading, values: (meta.firstVisibleBucket...meta.numBuckets).map { Double($0) }) { value in
                AxisGridLine()
                AxisValueLabel {
                    if let idx = value.as(Double.self), let label = meta.axisMarkLabels[idx] {
                        Text(label).font(.caption2)
                    }
                }
            }
        }
        .chartPlotStyle { $0.padding(.trailing, 8).clipped() }
        // chartOverlay gives us a ChartProxy with a valid plotFrame anchor that resolves
        // correctly in a GeometryReader. The Canvas draws heat-map cells clipped to the
        // plot frame so they never paint over axis labels. allowsHitTesting(false) lets
        // hover events fall through to the Color.clear hit target below.
        .chartOverlay { proxy in
            GeometryReader { geo in
                let frame = proxy.plotFrame.map { geo[$0] } ?? .zero
                // Heat-map canvas — non-interactive, clipped to plot area
                let _ = now  // register `now` as a dependency so canvas redraws each second
                Canvas { ctx, _ in
                    // Clip to the plot area so cells never overflow into axis label gutters
                    ctx.clip(to: Path(CGRect(origin: frame.origin, size: frame.size)))
                    drawColumns(ctx: ctx, meta: meta, proxy: proxy, plotOrigin: frame.origin)
                }
                .allowsHitTesting(false)
                // Hover hit target
                Color.clear
                    .contentShape(Rectangle())
                    .onContinuousHover { phase in
                        handleHover(phase: phase, proxy: proxy, frame: frame, meta: meta)
                    }
                if let snap = hoveredSnapshot,
                   let xPos = proxy.position(forX: snap.timestamp) {
                    let tipHalfWidth: CGFloat = 90
                    let centerX = min(
                        max(frame.origin.x + xPos, frame.origin.x + tipHalfWidth),
                        frame.origin.x + frame.width - tipHalfWidth
                    )
                    tooltipView(snap: snap, bucketIndex: hoveredBucketIndex, meta: meta)
                        .position(x: centerX, y: frame.origin.y + 60)
                        .allowsHitTesting(false)
                }
            }
        }
    }

    // MARK: - Canvas draw

    private func drawColumns(ctx: GraphicsContext, meta: HeatMapMeta, proxy: ChartProxy, plotOrigin: CGPoint) {
        let visibleBuckets = meta.numBuckets - meta.firstVisibleBucket
        guard visibleBuckets > 0 else { return }

        // proxy.position(forX/Y:) returns coordinates relative to the plot area origin.
        // chartBackground draws in the full chart frame (including axis gutters), so we
        // must add the plot area's offset to avoid painting over the y-axis labels.
        let ox = plotOrigin.x
        let oy = plotOrigin.y

        for col in meta.columns {
            guard let xStart = proxy.position(forX: col.timeStart),
                  let xEnd   = proxy.position(forX: col.timeEnd) else { continue }
            let colW = max(1, xEnd - xStart)

            for b in meta.firstVisibleBucket..<meta.numBuckets {
                let ratio = b < col.ratios.count ? col.ratios[b] : 0
                guard ratio > 0.005 else { continue }

                guard let yTop    = proxy.position(forY: Double(b + 1)),
                      let yBottom = proxy.position(forY: Double(b)) else { continue }
                let cellH = max(1, yBottom - yTop)

                let opacity = pow(ratio, 0.4)
                let color = Color(hue: 0.6, saturation: 0.85, brightness: 0.9, opacity: opacity)
                ctx.fill(Path(CGRect(x: ox + xStart, y: oy + yTop, width: colW, height: cellH)),
                         with: .color(color))
            }
        }
    }

    // MARK: - Hover

    private func handleHover(phase: HoverPhase, proxy: ChartProxy, frame: CGRect, meta: HeatMapMeta) {
        switch phase {
        case .active(let loc):
            let plotX = loc.x - frame.origin.x
            let plotY = loc.y - frame.origin.y
            guard plotX >= 0, plotX <= frame.width, plotY >= 0, plotY <= frame.height else {
                hoveredSnapshot = nil; hoveredBucketIndex = nil; return
            }
            if let date: Date = proxy.value(atX: plotX) {
                hoveredSnapshot = meta.sortedSnapshots.min(by: {
                    abs($0.timestamp.timeIntervalSince(date)) < abs($1.timestamp.timeIntervalSince(date))
                })
            }
            if let yVal: Double = proxy.value(atY: plotY) {
                let idx = Int(yVal.rounded(.down))
                hoveredBucketIndex = (idx >= 0 && idx < meta.numBuckets) ? idx : nil
            }
        case .ended:
            hoveredSnapshot = nil; hoveredBucketIndex = nil
        }
    }

    // MARK: - Tooltip

    private func tooltipView(snap: HistogramSnapshot, bucketIndex: Int?, meta: HeatMapMeta) -> some View {
        let total = snap.bucketCounts.count
        let i = bucketIndex ?? 0
        let count = i < total ? snap.bucketCounts[i] : 0
        let label = bucketLabel(index: i, bounds: meta.boundsRef, total: total)
        return VStack(alignment: .leading, spacing: 3) {
            Text(Self.tooltipFormatter.string(from: snap.timestamp))
                .font(.caption2).foregroundColor(.secondary)
            Divider()
            HStack(spacing: 6) {
                Text(label).font(.caption2).foregroundColor(.secondary).lineLimit(1).fixedSize()
                Spacer()
                Text("\(count)").font(.caption2)
                    .fontWeight(count > 0 ? .medium : .regular)
                    .foregroundColor(count > 0 ? .primary : .secondary)
            }
        }
        .padding(6)
        .background(Color(NSColor.controlBackgroundColor))
        .cornerRadius(4)
        .overlay(RoundedRectangle(cornerRadius: 4).stroke(Color.secondary.opacity(0.3), lineWidth: 1))
        .fixedSize()
    }

    // MARK: - Helpers (static so buildMeta can call them off main actor)

    nonisolated private static func timeScaleStatic(unit: String, bounds: [Double]) -> (multiplier: Double, symbol: String) {
        guard unit.lowercased() == "ms" else { return (1, unit) }
        let minBound = bounds.filter { $0.isFinite && $0 > 0 }.min() ?? 1
        if minBound >= 1     { return (1,         "ms") }
        if minBound >= 0.001 { return (1_000,     "µs") }
        return                        (1_000_000, "ns")
    }

    nonisolated private static func formatStatic(_ v: Double) -> String {
        if abs(v) >= 1_000_000 { return String(format: "%.1fM", v / 1_000_000) }
        if abs(v) >= 1_000     { return String(format: "%.1fk", v / 1_000) }
        if abs(v) < 0.01 && v != 0 { return String(format: "%.3f", v) }
        return String(format: "%.2f", v)
    }

    private func bucketLabel(index i: Int, bounds: [Double], total: Int) -> String {
        let (mult, sym) = Self.timeScaleStatic(unit: unit, bounds: bounds)
        let u = sym.isEmpty ? "" : " \(sym)"
        func fmt(_ v: Double) -> String {
            let c = v * mult; let a = abs(c)
            if a >= 1000 { return String(format: "%.0fk", c / 1000) }
            if a >= 1    { return String(format: "%.0f", c) }
            if a >= 0.1  { return String(format: "%.1f", c) }
            return String(format: "%.2f", c)
        }
        if i == 0                              { return "≤\(fmt(bounds.first ?? 0))\(u)" }
        if i == total - 1                      { return ">\(fmt(bounds.last ?? 0))\(u)" }
        if i - 1 < bounds.count && i < bounds.count { return "\(fmt(bounds[i-1]))–\(fmt(bounds[i]))\(u)" }
        return "bucket \(i)"
    }
}

// MARK: - View extension

private extension View {
    @ViewBuilder
    func `if`<Content: View>(_ condition: Bool, transform: (Self) -> Content) -> some View {
        if condition { transform(self) } else { self }
    }
}
