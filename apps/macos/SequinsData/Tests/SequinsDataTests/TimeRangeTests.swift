import XCTest
@testable import SequinsData

final class TimeRangeTests: XCTestCase {
    func testAbsoluteTimeRange() {
        let start = Date(timeIntervalSince1970: 1000)
        let end = Date(timeIntervalSince1970: 2000)
        let range = TimeRange.absolute(start: start, end: end)

        let bounds = range.bounds
        XCTAssertEqual(bounds.start, start)
        XCTAssertEqual(bounds.end, end)
        XCTAssertFalse(range.isLive)
        XCTAssertEqual(range.duration, 1000, accuracy: 0.001)
    }

    func testRelativeTimeRange() {
        let range = TimeRange.relative(duration: 3600) // 1 hour

        let bounds = range.bounds
        let now = Date()

        // The bounds should be approximately 1 hour ago to now
        XCTAssertEqual(bounds.end.timeIntervalSince1970, now.timeIntervalSince1970, accuracy: 1.0)
        XCTAssertEqual(bounds.start.timeIntervalSince1970, now.timeIntervalSince1970 - 3600, accuracy: 1.0)
        XCTAssertTrue(range.isLive)
        XCTAssertEqual(range.duration, 3600, accuracy: 0.001)
    }

    func testConvenienceConstructors() {
        // Test last(minutes:)
        let minutes15 = TimeRange.last(minutes: 15)
        XCTAssertTrue(minutes15.isLive)
        XCTAssertEqual(minutes15.duration, 15 * 60, accuracy: 0.001)

        // Test last(hours:)
        let hours2 = TimeRange.last(hours: 2)
        XCTAssertTrue(hours2.isLive)
        XCTAssertEqual(hours2.duration, 2 * 3600, accuracy: 0.001)

        // Test last(seconds:)
        let seconds30 = TimeRange.last(seconds: 30)
        XCTAssertTrue(seconds30.isLive)
        XCTAssertEqual(seconds30.duration, 30, accuracy: 0.001)

        // Test between(_:and:)
        let start = Date(timeIntervalSince1970: 1000)
        let end = Date(timeIntervalSince1970: 2000)
        let between = TimeRange.between(start, and: end)
        XCTAssertFalse(between.isLive)
        XCTAssertEqual(between.bounds.start, start)
        XCTAssertEqual(between.bounds.end, end)

        // Test since(_:)
        let startDate = Date(timeIntervalSince1970: 1000)
        let since = TimeRange.since(startDate)
        XCTAssertFalse(since.isLive)
        XCTAssertEqual(since.bounds.start, startDate)

        // Test allTime
        let allTime = TimeRange.allTime
        XCTAssertFalse(allTime.isLive)
        XCTAssertEqual(allTime.bounds.start.timeIntervalSince1970, 0, accuracy: 0.001)
    }

    func testEquatable() {
        let start = Date(timeIntervalSince1970: 1000)
        let end = Date(timeIntervalSince1970: 2000)

        let range1 = TimeRange.absolute(start: start, end: end)
        let range2 = TimeRange.absolute(start: start, end: end)
        let range3 = TimeRange.relative(duration: 3600)
        let range4 = TimeRange.relative(duration: 3600)

        XCTAssertEqual(range1, range2)
        XCTAssertEqual(range3, range4)
        XCTAssertNotEqual(range1, range3)
    }

    func testSlidingWindowBoundsChange() {
        let range = TimeRange.relative(duration: 60) // 1 minute

        let bounds1 = range.bounds
        // Sleep briefly to let time advance
        Thread.sleep(forTimeInterval: 0.1)
        let bounds2 = range.bounds

        // The bounds should have advanced slightly
        XCTAssertGreaterThan(bounds2.start, bounds1.start)
        XCTAssertGreaterThan(bounds2.end, bounds1.end)
    }
}
