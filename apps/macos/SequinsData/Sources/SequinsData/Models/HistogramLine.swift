//
//  HistogramLine.swift
//  SequinsData
//
//  An @Observable histogram metric line whose snapshots are patched in-place by
//  live delta ops. Parallel to MetricLine but for histogram-type metrics.
//

import Foundation

/// A single histogram observation at a point in time.
public struct HistogramSnapshot: Equatable {
    /// Timestamp of this observation
    public let timestamp: Date
    /// Total count of observations
    public let count: UInt64
    /// Sum of all observed values
    public let sum: Double
    /// Per-bucket observation counts (N buckets)
    public let bucketCounts: [UInt64]
    /// Upper-bound for each of the first N-1 buckets (last bucket is +∞)
    public let explicitBounds: [Double]

    public init(timestamp: Date, count: UInt64, sum: Double, bucketCounts: [UInt64], explicitBounds: [Double]) {
        self.timestamp = timestamp
        self.count = count
        self.sum = sum
        self.bucketCounts = bucketCounts
        self.explicitBounds = explicitBounds
    }

    /// Mean value for this snapshot (sum / count), or nil if count is zero.
    public var mean: Double? {
        guard count > 0 else { return nil }
        return sum / Double(count)
    }
}

/// An observable histogram metric line with live-patchable snapshots.
///
/// Created once per histogram metric from the Phase 1 historical snapshot. In live
/// mode, `snapshots` is replaced when a Replace/Append delta arrives, preserving
/// the efficient per-metric update path.
///
/// `maxActiveBucket` is maintained incrementally: computed once per delta segment
/// when it is added, and recomputed from remaining segments only when pruning
/// removes the current maximum. This avoids rescanning on every render tick.
@Observable
public final class HistogramLine: Identifiable {
    /// Metric ID (UUID hex string) — also the `id` for `Identifiable`
    public let id: String

    /// Row index in the live query outer result (used to match Update delta row_ids)
    public let rowId: UInt64

    // ── Metric metadata (immutable after creation) ────────────────────────────
    public let name: String
    public let description: String
    public let unit: String
    public let serviceName: String

    /// Histogram snapshots — replaced in-place when a delta arrives.
    public var snapshots: [HistogramSnapshot]

    /// Highest bucket index that has any non-zero delta count across all current
    /// snapshots. Used by HeatMapChart to trim empty upper buckets without
    /// rescanning on every render.
    public private(set) var maxActiveBucket: Int = 0

    /// Last cumulative snapshot seen — used to compute per-segment delta max
    /// bucket when new live snapshots arrive.
    private var lastCumulative: HistogramSnapshot?

    public init(
        id: String,
        rowId: UInt64,
        name: String,
        description: String,
        unit: String,
        serviceName: String,
        snapshots: [HistogramSnapshot] = []
    ) {
        self.id = id
        self.rowId = rowId
        self.name = name
        self.description = description
        self.unit = unit
        self.serviceName = serviceName
        self.snapshots = snapshots
    }

    // MARK: - Mutation API

    /// Append new cumulative snapshots, update `maxActiveBucket` for each new
    /// delta segment, and prune snapshots older than `cutoff` in one mutation.
    ///
    /// Only called for live Append ops where snapshots arrive one export at a time.
    /// The delta max bucket for each new segment is computed once here and never
    /// re-scanned unless pruning removes the current maximum.
    public func appendCumulativeAndPrune(_ newSnapshots: [HistogramSnapshot], olderThan cutoff: Date) {
        let incoming = newSnapshots.sorted { $0.timestamp < $1.timestamp }
        for snap in incoming {
            if let prev = lastCumulative {
                maxActiveBucket = max(maxActiveBucket, deltaMaxBucket(from: prev, to: snap))
            }
            lastCumulative = snap
        }

        let countBefore = snapshots.count
        snapshots.append(contentsOf: incoming)
        snapshots = snapshots.filter { $0.timestamp >= cutoff }

        // Only rescan if pruning may have removed the segment that held maxActiveBucket.
        if snapshots.count < countBefore {
            recomputeMaxActiveBucket()
        }
    }

    /// Replace all snapshots from a full cumulative set (historical load or Update op)
    /// and recompute `maxActiveBucket` from scratch.
    public func replaceWithCumulatives(_ newSnapshots: [HistogramSnapshot]) {
        snapshots = newSnapshots
        lastCumulative = newSnapshots.sorted { $0.timestamp < $1.timestamp }.last
        recomputeMaxActiveBucket()
    }

    // MARK: - Private helpers

    /// Scan all current cumulative snapshots to recompute `maxActiveBucket` from
    /// their pairwise deltas. Called only after pruning or a full replace — not
    /// on every render.
    private func recomputeMaxActiveBucket() {
        let sorted = snapshots.sorted { $0.timestamp < $1.timestamp }
        guard sorted.count >= 2 else {
            maxActiveBucket = 0
            return
        }
        var maxB = 0
        for i in 1..<sorted.count {
            maxB = max(maxB, deltaMaxBucket(from: sorted[i - 1], to: sorted[i]))
        }
        maxActiveBucket = maxB
    }

    /// Return the highest bucket index whose delta count (cur − prev) is non-zero.
    private func deltaMaxBucket(from prev: HistogramSnapshot, to cur: HistogramSnapshot) -> Int {
        let n = max(cur.bucketCounts.count, prev.bucketCounts.count)
        for i in (0..<n).reversed() {
            let c = i < cur.bucketCounts.count ? cur.bucketCounts[i] : 0
            let p = i < prev.bucketCounts.count ? prev.bucketCounts[i] : 0
            if (c >= p ? c - p : c) > 0 { return i }
        }
        return 0
    }
}
