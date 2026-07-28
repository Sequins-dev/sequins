//
//  HistogramLineTests.swift
//  SequinsDataTests
//
//  Regression coverage for HistogramLine.maxActiveBucket recomputation, whose
//  `1..<count` scan traps when the snapshot set is empty (1 > 0).
//

import XCTest
@testable import SequinsData

final class HistogramLineTests: XCTestCase {
    private func makeLine() -> HistogramLine {
        HistogramLine(
            id: "abc",
            rowId: 0,
            name: "latency",
            description: "",
            unit: "ms",
            serviceName: "svc"
        )
    }

    private func snap(_ ts: Double, _ buckets: [UInt64]) -> HistogramSnapshot {
        HistogramSnapshot(
            timestamp: Date(timeIntervalSince1970: ts),
            count: buckets.reduce(0, +),
            sum: 0,
            bucketCounts: buckets,
            explicitBounds: [1, 2, 3]
        )
    }

    /// The crash: a live stream delivering an empty histogram set drove
    /// `replaceWithCumulatives([])` → `recomputeMaxActiveBucket()` → `1..<0` trap.
    func testReplaceWithEmptyDoesNotCrash() {
        let line = makeLine()
        line.replaceWithCumulatives([])
        XCTAssertEqual(line.maxActiveBucket, 0)
        XCTAssertTrue(line.snapshots.isEmpty)
    }

    /// A single snapshot has no pairwise delta — must not trap and leaves max at 0.
    func testReplaceWithSingleSnapshotDoesNotCrash() {
        let line = makeLine()
        line.replaceWithCumulatives([snap(1, [0, 0, 5, 0])])
        XCTAssertEqual(line.maxActiveBucket, 0)
    }

    /// Two cumulative snapshots: the delta's highest non-zero bucket is index 2.
    func testReplaceWithCumulativesComputesMaxActiveBucket() {
        let line = makeLine()
        line.replaceWithCumulatives([
            snap(1, [1, 0, 0, 0]),
            snap(2, [1, 0, 3, 0]),
        ])
        XCTAssertEqual(line.maxActiveBucket, 2)
    }

    /// Pruning that drops the snapshot count triggers `recomputeMaxActiveBucket()`
    /// (the actual crash site). Starting from two snapshots and pruning down to one
    /// must recompute over a single-element array without trapping, yielding 0.
    func testPruneTriggersRecomputeToOneDoesNotCrash() {
        let line = makeLine()
        line.replaceWithCumulatives([snap(1, [1, 0, 0, 0]), snap(2, [1, 0, 5, 0])])
        XCTAssertEqual(line.maxActiveBucket, 2)

        // Append snap3 and prune everything older than ts=3 → only snap3 remains.
        // count (1) < countBefore (2) → recompute runs on a single snapshot.
        line.appendCumulativeAndPrune(
            [snap(3, [1, 0, 0, 0])],
            olderThan: Date(timeIntervalSince1970: 3)
        )
        XCTAssertEqual(line.snapshots.count, 1)
        XCTAssertEqual(line.maxActiveBucket, 0)
    }
}
