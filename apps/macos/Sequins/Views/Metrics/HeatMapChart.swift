import SwiftUI
import Charts
import SequinsData

// MARK: - Heat Map Cell

private struct HeatMapCell: Identifiable {
    let id: String
    let timeStart: Date
    let timeEnd: Date
    let bucketLower: Double
    let bucketUpper: Double
    let count: UInt64
    /// Opacity ratio pre-normalised against the max bucket count in this segment.
    /// Computed once at layout build time so adding new segments never rescales
    /// existing columns.
    let ratio: Double
}

// MARK: - Precomputed layout

private struct HeatMapLayout {
    let cells: [HeatMapCell]
    let yDomain: ClosedRange<Double>   // yMin...numBuckets (index space)
    let yMin: Double                   // 0 normally; 1 when underflow bucket is hidden
    let numBuckets: Int
    let sortedSnapshots: [HistogramSnapshot]
    let boundsRef: [Double]            // explicit bounds for tooltip labels
    let axisMarkLabels: [Double: String]  // bucket-index position → label string
}

// MARK: - HeatMapChart

/// Grafana-style histogram heat map using SwiftUI Charts RectangleMark.
///
/// X-axis: time (one column per snapshot)
/// Y-axis: bucket index space (equal-height bands)
/// Color:  delta bucket count → blue opacity (power curve)
///
/// Layout is cached and rebuilt only when snapshots change, not on every
/// animation tick — so the per-second smooth-scroll timer does not trigger
/// expensive sorting/merging/delta work.
struct HeatMapChart: View {
    let snapshots: [HistogramSnapshot]
    /// Pre-maintained by HistogramLine — the highest bucket index that has
    /// any non-zero delta count across current snapshots. Avoids rescanning
    /// on every render; updated incrementally in the model as data arrives.
    let maxActiveBucket: Int
    let unit: String
    let timeRange: SequinsData.TimeRange?

    @State private var hoveredSnapshot: HistogramSnapshot?
    @State private var hoveredBucketIndex: Int?
    @State private var hoverLocation: CGPoint = .zero
    @State private var now = Date()
    @State private var cachedLayout: HeatMapLayout?

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

    // MARK: - Fingerprint for cache invalidation

    /// Changes when snapshots are added, pruned, or replaced, or when the model's
    /// maxActiveBucket changes — but NOT on every `now` tick.
    private struct LayoutKey: Equatable {
        let count: Int
        let lastTimestamp: Double
    }
    private var layoutKey: LayoutKey {
        LayoutKey(
            count: snapshots.count,
            lastTimestamp: snapshots.last?.timestamp.timeIntervalSince1970 ?? 0
        )
    }

    private var xAxisDomain: ClosedRange<Date>? {
        guard let timeRange = timeRange else { return nil }
        if timeRange.isLive { _ = now }
        let bounds = timeRange.bounds
        return bounds.start...bounds.end
    }

    // MARK: - Layout

    /// Sum bucket_counts across a group of snapshots sharing the same timestamp.
    private func mergeSnapshots(_ group: [HistogramSnapshot]) -> HistogramSnapshot {
        guard group.count > 1 else { return group[0] }
        let ref = group.max(by: { $0.explicitBounds.count < $1.explicitBounds.count })!
        let nBuckets = ref.bucketCounts.count
        var counts = [UInt64](repeating: 0, count: nBuckets)
        var totalCount: UInt64 = 0
        var totalSum: Double = 0
        for snap in group {
            totalCount += snap.count
            totalSum += snap.sum
            for (i, c) in snap.bucketCounts.enumerated() where i < nBuckets {
                counts[i] += c
            }
        }
        return HistogramSnapshot(
            timestamp: ref.timestamp,
            count: totalCount,
            sum: totalSum,
            bucketCounts: counts,
            explicitBounds: ref.explicitBounds
        )
    }

    /// Build the full render layout from current snapshots.
    ///
    /// Called only when snapshots change (via `.task(id: layoutKey)`), not on
    /// every animation tick, so all O(n×b) sorting/merging/delta work is amortised
    /// across the export interval rather than repeated 60 times per second.
    private func buildLayout() -> HeatMapLayout? {
        guard !snapshots.isEmpty else { return nil }

        let rawSorted = snapshots.sorted { $0.timestamp < $1.timestamp }

        // Group same-timestamp snapshots (one per attribute-labelled series per export)
        // by summing bucket_counts so each export interval produces one column.
        let merged: [HistogramSnapshot] = {
            var result: [HistogramSnapshot] = []
            var group: [HistogramSnapshot] = [rawSorted[0]]
            for i in 1..<rawSorted.count {
                let cur = rawSorted[i]
                if abs(cur.timestamp.timeIntervalSince(group[0].timestamp)) < 5.0 {
                    group.append(cur)
                } else {
                    result.append(mergeSnapshots(group))
                    group = [cur]
                }
            }
            result.append(mergeSnapshots(group))
            return result
        }()

        // Convert cumulative bucket_counts to per-interval deltas.
        let sorted: [HistogramSnapshot] = {
            guard merged.count > 1 else { return merged }
            var result: [HistogramSnapshot] = [merged[0]]
            for i in 1..<merged.count {
                let cur = merged[i]
                let prev = merged[i - 1]
                let nBuckets = max(cur.bucketCounts.count, prev.bucketCounts.count)
                var deltaCounts = [UInt64](repeating: 0, count: nBuckets)
                for j in 0..<nBuckets {
                    let c = j < cur.bucketCounts.count ? cur.bucketCounts[j] : 0
                    let p = j < prev.bucketCounts.count ? prev.bucketCounts[j] : 0
                    deltaCounts[j] = c >= p ? c - p : c
                }
                let deltaCount = cur.count >= prev.count ? cur.count - prev.count : cur.count
                let deltaSum   = cur.sum   >= prev.sum   ? cur.sum   - prev.sum   : cur.sum
                result.append(HistogramSnapshot(
                    timestamp: cur.timestamp,
                    count: deltaCount,
                    sum: deltaSum,
                    bucketCounts: deltaCounts,
                    explicitBounds: cur.explicitBounds
                ))
            }
            return result
        }()

        let boundsRef = sorted
            .max(by: { $0.explicitBounds.count < $1.explicitBounds.count })?
            .explicitBounds ?? []
        let allFinite = boundsRef.filter { $0.isFinite }
        guard allFinite.count >= 2 else { return nil }

        let totalBuckets = allFinite.count + 1  // underflow + explicit + overflow

        // Derive the max active bucket directly from the already-processed delta
        // snapshots (sorted[1...], after same-timestamp merging and delta conversion).
        // This is more reliable than the model's maxActiveBucket, which is computed
        // from raw cumulative snapshots that may mix multiple series.
        let effectiveMaxBucket: Int = sorted.dropFirst().reduce(0) { current, snap in
            let highest = snap.bucketCounts.indices.reversed().first { snap.bucketCounts[$0] > 0 }
            return max(current, highest ?? 0)
        }
        let numBuckets = max(2, min(effectiveMaxBucket + 1, totalBuckets))

        let n = sorted.count
        var timeStarts = [Date](repeating: sorted[0].timestamp, count: n)
        var timeEnds   = [Date](repeating: sorted[0].timestamp, count: n)
        if n == 1 {
            timeStarts[0] = sorted[0].timestamp.addingTimeInterval(-30)
            timeEnds[0]   = sorted[0].timestamp.addingTimeInterval(30)
        } else {
            for i in 0..<n {
                let prev = i == 0
                    ? sorted[0].timestamp.addingTimeInterval(sorted[0].timestamp.timeIntervalSince(sorted[1].timestamp))
                    : sorted[i - 1].timestamp
                let next = i == n - 1
                    ? sorted[n-1].timestamp.addingTimeInterval(sorted[n-1].timestamp.timeIntervalSince(sorted[n-2].timestamp))
                    : sorted[i + 1].timestamp
                timeStarts[i] = Date(timeIntervalSince1970: (prev.timeIntervalSince1970 + sorted[i].timestamp.timeIntervalSince1970) / 2)
                timeEnds[i]   = Date(timeIntervalSince1970: (sorted[i].timestamp.timeIntervalSince1970 + next.timeIntervalSince1970) / 2)
            }
        }

        // Hide bucket 0 (underflow: values ≤ first explicit bound) when the first
        // bound is 0 or negative — nothing can be in "≤0 ms" for duration metrics.
        let yMin: Double = (allFinite.first.map { $0 <= 0 } ?? false) ? 1.0 : 0.0
        let firstVisibleBucket = Int(yMin)

        // sorted[0] is the cumulative baseline — it must NOT produce cells.
        // Only sorted[1...] are per-interval delta counts.
        //
        // Opacity is normalised per-segment (per time column) against that
        // column's own peak bucket count, so adding a new segment never rescales
        // existing columns' appearance.
        var cells: [HeatMapCell] = []
        cells.reserveCapacity((n - 1) * numBuckets)
        for snapIdx in 1..<n {
            let snapshot = sorted[snapIdx]
            // Find the max count within this segment for per-column normalisation.
            let segMax: UInt64 = snapshot.bucketCounts
                .prefix(numBuckets)
                .dropFirst(firstVisibleBucket)
                .max() ?? 1
            for (i, count) in snapshot.bucketCounts.enumerated() {
                guard i >= firstVisibleBucket && i < numBuckets else { continue }
                let ratio = segMax > 0 ? Double(count) / Double(segMax) : 0
                cells.append(HeatMapCell(
                    id: "\(snapshot.timestamp.timeIntervalSince1970)-\(i)",
                    timeStart: timeStarts[snapIdx],
                    timeEnd: timeEnds[snapIdx],
                    bucketLower: Double(i),
                    bucketUpper: Double(i + 1),
                    count: count,
                    ratio: ratio
                ))
            }
        }
        guard !cells.isEmpty else { return nil }

        let yDomain = yMin...Double(numBuckets)

        let visibleBoundCount = min(allFinite.count, numBuckets - 1)
        let step = max(1, visibleBoundCount / 5)
        let (axisMultiplier, axisSymbol) = timeScale(for: allFinite)
        let axisMarkLabels: [Double: String] = Dictionary(
            uniqueKeysWithValues: stride(from: max(1, firstVisibleBucket + 1), through: visibleBoundCount, by: step).map { i in
                let v = allFinite[i - 1] * axisMultiplier
                let num = formatLinear(v)
                let label = axisSymbol.isEmpty ? num : "\(num) \(axisSymbol)"
                return (Double(i), label)
            }
        )

        return HeatMapLayout(
            cells: cells,
            yDomain: yDomain,
            yMin: yMin,
            numBuckets: numBuckets,
            sortedSnapshots: sorted,
            boundsRef: allFinite,
            axisMarkLabels: axisMarkLabels
        )
    }

    // MARK: - Body

    var body: some View {
        Group {
            if let layout = cachedLayout {
                chartView(layout: layout)
            } else {
                Rectangle()
                    .fill(Color.gray.opacity(0.1))
                    .overlay(Text("No data").foregroundColor(.secondary).font(.caption))
            }
        }
        .task(id: layoutKey) {
            // Rebuild only when data changes — not on every `now` tick.
            cachedLayout = buildLayout()
        }
        .task(id: timeRange?.isLive) {
            guard timeRange?.isLive == true else { return }
            while !Task.isCancelled {
                try? await Task.sleep(nanoseconds: 1_000_000_000)
                withAnimation(.linear(duration: 1.0)) {
                    now = Date()
                }
            }
        }
    }

    // MARK: - Chart view

    @ViewBuilder
    private func chartView(layout: HeatMapLayout) -> some View {
        Chart {
            ForEach(layout.cells) { cell in
                RectangleMark(
                    xStart: .value("Start", cell.timeStart),
                    xEnd: .value("End", cell.timeEnd),
                    yStart: .value("Lower", cell.bucketLower),
                    yEnd: .value("Upper", cell.bucketUpper)
                )
                .foregroundStyle(colorForRatio(cell.ratio))
            }
            if let snap = hoveredSnapshot {
                RuleMark(x: .value("Hover", snap.timestamp))
                    .foregroundStyle(Color.white.opacity(0.6))
                    .lineStyle(StrokeStyle(lineWidth: 1))
            }
        }
        .chartXAxis {
            let domain = xAxisDomain ?? (layout.sortedSnapshots.first?.timestamp ?? Date())...(layout.sortedSnapshots.last?.timestamp ?? Date())
            let interval = ChartDataUtils.axisTickInterval(for: domain.upperBound.timeIntervalSince(domain.lowerBound))
            let labelSafeStart = domain.lowerBound.addingTimeInterval(interval * 0.3)
            AxisMarks(values: ChartDataUtils.axisTicks(for: domain)) { value in
                AxisGridLine()
                    .foregroundStyle(Color.secondary.opacity(0.2))
                let date = value.as(Date.self)
                let showLabel = date.map { $0 >= labelSafeStart } ?? false
                if showLabel, let d = date {
                    AxisValueLabel {
                        Text(Self.axisFormatter.string(from: d))
                            .font(.caption2)
                            .foregroundColor(.secondary)
                    }
                }
            }
        }
        .chartYAxis {
            AxisMarks(position: .leading, values: (Int(layout.yMin)...layout.numBuckets).map { Double($0) }) { value in
                AxisGridLine()
                AxisValueLabel {
                    if let idx = value.as(Double.self),
                       let label = layout.axisMarkLabels[idx] {
                        Text(label)
                            .font(.caption2)
                    }
                }
            }
        }
        .animation(.linear(duration: 1.0), value: now)
        .chartYScale(domain: layout.yDomain)
        .chartPlotStyle { plotArea in
            plotArea
                .padding(.trailing, 8)
                .clipped()
        }
        .if(xAxisDomain != nil) { chart in
            chart.chartXScale(domain: xAxisDomain!)
        }
        .chartOverlay { proxy in
            GeometryReader { geo in
                let frame = proxy.plotFrame.map { geo[$0] } ?? .zero
                Color.clear
                    .contentShape(Rectangle())
                    .onContinuousHover { phase in
                        switch phase {
                        case .active(let location):
                            hoverLocation = location
                            let plotX = location.x - frame.origin.x
                            let plotY = location.y - frame.origin.y
                            guard plotX >= 0, plotX <= frame.width,
                                  plotY >= 0, plotY <= frame.height else {
                                hoveredSnapshot = nil
                                hoveredBucketIndex = nil
                                return
                            }
                            if let date: Date = proxy.value(atX: plotX) {
                                hoveredSnapshot = layout.sortedSnapshots.min(by: {
                                    abs($0.timestamp.timeIntervalSince(date)) <
                                    abs($1.timestamp.timeIntervalSince(date))
                                })
                            } else {
                                hoveredSnapshot = nil
                            }
                            if let yVal: Double = proxy.value(atY: plotY) {
                                let idx = Int(yVal.rounded(.down))
                                hoveredBucketIndex = (idx >= 0 && idx < layout.numBuckets) ? idx : nil
                            } else {
                                hoveredBucketIndex = nil
                            }
                        case .ended:
                            hoveredSnapshot = nil
                            hoveredBucketIndex = nil
                        }
                    }

                if let snap = hoveredSnapshot,
                   let xPos = proxy.position(forX: snap.timestamp) {
                    let tipHalfWidth: CGFloat = 90
                    let centerX = min(
                        max(frame.origin.x + xPos, frame.origin.x + tipHalfWidth),
                        frame.origin.x + frame.width - tipHalfWidth
                    )
                    tooltipView(snap: snap, bucketIndex: hoveredBucketIndex, bounds: layout.boundsRef)
                        .position(x: centerX, y: frame.origin.y + 60)
                        .allowsHitTesting(false)
                }
            }
        }
    }

    // MARK: - Tooltip

    private func tooltipView(snap: HistogramSnapshot, bucketIndex: Int?, bounds: [Double]) -> some View {
        let total = snap.bucketCounts.count
        let i = bucketIndex ?? 0
        let count = i < total ? snap.bucketCounts[i] : 0
        let label = bucketLabel(index: i, bounds: bounds, total: total)

        return VStack(alignment: .leading, spacing: 3) {
            Text(Self.tooltipFormatter.string(from: snap.timestamp))
                .font(.caption2)
                .foregroundColor(.secondary)

            Divider()

            HStack(spacing: 6) {
                Text(label)
                    .font(.caption2)
                    .foregroundColor(.secondary)
                    .lineLimit(1)
                    .fixedSize()
                Spacer()
                Text("\(count)")
                    .font(.caption2)
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

    private func timeScale(for bounds: [Double]) -> (multiplier: Double, symbol: String) {
        guard unit.lowercased() == "ms" else { return (1, unit) }
        let minBound = bounds.filter { $0.isFinite && $0 > 0 }.min() ?? 1
        if minBound >= 1      { return (1,         "ms") }
        if minBound >= 0.001  { return (1_000,     "µs") }
        return                         (1_000_000, "ns")
    }

    private func bucketLabel(index i: Int, bounds: [Double], total: Int) -> String {
        let (multiplier, symbol) = timeScale(for: bounds)
        let u = symbol.isEmpty ? "" : " \(symbol)"

        func fmt(_ v: Double) -> String {
            if v == 0 { return "0" }
            let c = v * multiplier
            let absC = abs(c)
            if absC >= 1000 { return String(format: "%.0fk", c / 1000) }
            if absC >= 1    { return String(format: "%.0f", c) }
            if absC >= 0.1  { return String(format: "%.1f", c) }
            return String(format: "%.2f", c)
        }

        if i == 0 {
            return "≤\(fmt(bounds.first ?? 0))\(u)"
        } else if i == total - 1 {
            return ">\(fmt(bounds.last ?? 0))\(u)"
        } else if i - 1 < bounds.count && i < bounds.count {
            return "\(fmt(bounds[i - 1]))–\(fmt(bounds[i]))\(u)"
        }
        return "bucket \(i)"
    }

    // MARK: - Color mapping

    private func colorForRatio(_ ratio: Double) -> Color {
        let opacity = pow(ratio, 0.4)
        return Color(hue: 0.6, saturation: 0.85, brightness: 0.9, opacity: opacity)
    }

    private func formatLinear(_ v: Double) -> String {
        if abs(v) >= 1_000_000 { return String(format: "%.1fM", v / 1_000_000) }
        if abs(v) >= 1_000     { return String(format: "%.1fk", v / 1_000) }
        if abs(v) < 0.01 && v != 0 { return String(format: "%.3f", v) }
        return String(format: "%.2f", v)
    }
}

// MARK: - View extension

private extension View {
    @ViewBuilder
    func `if`<Content: View>(_ condition: Bool, transform: (Self) -> Content) -> some View {
        if condition { transform(self) } else { self }
    }
}
