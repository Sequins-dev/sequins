import Foundation

/// Data point with segment information for gap-aware chart rendering
struct SegmentedDataPoint: Identifiable {
    /// Stable ID so SwiftUI Charts can track marks across re-renders without animating from scratch.
    var id: Double { timestamp.timeIntervalSince1970 }
    let timestamp: Date
    let value: Double
    let segmentId: Int
    /// True if this point is the only point in its segment (needs PointMark instead of LineMark)
    let isSinglePointSegment: Bool
}

/// Utility functions for chart data processing
enum ChartDataUtils {
    /// Gap threshold multiplier (1.5x bucket duration)
    static let gapMultiplier: Double = 1.5

    /// Segments data points based on time gaps
    /// Points separated by more than gapMultiplier * bucketDuration are placed in different segments
    static func segmentDataPoints(
        _ dataPoints: [MetricDataPoint],
        bucketDuration: TimeInterval
    ) -> [SegmentedDataPoint] {
        guard !dataPoints.isEmpty else { return [] }

        let gapThreshold = bucketDuration * gapMultiplier
        var segmentAssignments: [(point: MetricDataPoint, segmentId: Int)] = []
        var currentSegment = 0
        var previousTimestamp: Date?

        // DataFusion GROUP BY doesn't guarantee output order, so sort by timestamp first.
        // Without sorting, unsorted consecutive points produce large positive gaps that
        // trigger spurious segment splits, resulting in many disconnected line segments.
        let sorted = dataPoints.sorted { $0.timestamp < $1.timestamp }

        // First pass: assign segment IDs
        for point in sorted {
            if let prev = previousTimestamp {
                let gap = point.timestamp.timeIntervalSince(prev)
                if gap > gapThreshold {
                    currentSegment += 1
                }
            }
            segmentAssignments.append((point: point, segmentId: currentSegment))
            previousTimestamp = point.timestamp
        }

        // Count points per segment to identify single-point segments
        var segmentCounts: [Int: Int] = [:]
        for assignment in segmentAssignments {
            segmentCounts[assignment.segmentId, default: 0] += 1
        }

        // Second pass: create SegmentedDataPoints with isSinglePointSegment flag
        return segmentAssignments.map { assignment in
            SegmentedDataPoint(
                timestamp: assignment.point.timestamp,
                value: assignment.point.value,
                segmentId: assignment.segmentId,
                isSinglePointSegment: segmentCounts[assignment.segmentId] == 1
            )
        }
    }

    // MARK: - Axis tick helpers

    /// Tick interval that produces ~5 labels for a given domain duration.
    static func axisTickInterval(for duration: TimeInterval) -> TimeInterval {
        let target = duration / 5.0
        let nice: [Double] = [1, 2, 5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600, 7200]
        return nice.min(by: { abs($0 - target) < abs($1 - target) }) ?? 60
    }

    /// Concrete Date tick values anchored to clock boundaries, strictly within the domain.
    ///
    /// Starting from `ceil(domainStart / interval)` ensures no tick falls outside the
    /// left edge of the plot area (which would render its AxisGridLine in the y-axis gutter).
    /// The domain itself is animated via withAnimation(.linear), so ticks slide smoothly
    /// without needing any out-of-bounds padding ticks.
    static func axisTicks(for domain: ClosedRange<Date>) -> [Date] {
        let duration = domain.upperBound.timeIntervalSince(domain.lowerBound)
        let interval = axisTickInterval(for: duration)

        // First tick boundary at or after domain start — never before it
        let startTs = ceil(domain.lowerBound.timeIntervalSince1970 / interval) * interval
        let endTs = domain.upperBound.timeIntervalSince1970

        var ticks: [Date] = []
        var t = startTs
        while t <= endTs {
            ticks.append(Date(timeIntervalSince1970: t))
            t += interval
        }
        return ticks
    }

    /// Estimate bucket duration from data using the median interval
    static func estimateBucketDuration(from dataPoints: [MetricDataPoint]) -> TimeInterval? {
        guard dataPoints.count >= 2 else { return nil }
        var intervals: [TimeInterval] = []
        for i in 1..<dataPoints.count {
            intervals.append(dataPoints[i].timestamp.timeIntervalSince(dataPoints[i-1].timestamp))
        }
        let sorted = intervals.sorted()
        return sorted[sorted.count / 2]  // Median
    }
}
