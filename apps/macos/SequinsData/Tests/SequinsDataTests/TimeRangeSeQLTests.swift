import XCTest
@testable import SequinsData

final class TimeRangeSeQLTests: XCTestCase {

    func testRelativeReplacesLastClause() {
        let range = TimeRange.relative(duration: 1800)  // 30 minutes
        XCTAssertEqual(
            range.applied(to: "logs last 1h | where severity_number >= 9 | take 500"),
            "logs last 30m | where severity_number >= 9 | take 500"
        )
    }

    func testRelativeUnitSelection() {
        XCTAssertEqual(TimeRange.relative(duration: 3600).seqlTimeScope, "last 1h")
        XCTAssertEqual(TimeRange.relative(duration: 86_400).seqlTimeScope, "last 1d")
        XCTAssertEqual(TimeRange.relative(duration: 300).seqlTimeScope, "last 5m")
        XCTAssertEqual(TimeRange.relative(duration: 45).seqlTimeScope, "last 45s")
        // 90 minutes is not a whole number of hours → minutes.
        XCTAssertEqual(TimeRange.relative(duration: 5400).seqlTimeScope, "last 90m")
    }

    func testAbsoluteBecomesBetween() {
        let start = Date(timeIntervalSince1970: 1)   // 1e9 ns
        let end = Date(timeIntervalSince1970: 2)     // 2e9 ns
        let range = TimeRange.absolute(start: start, end: end)
        XCTAssertEqual(
            range.applied(to: "spans last 1h | take 5"),
            "spans between(1000000000, 2000000000) | take 5"
        )
    }

    func testReplacesTodayAndBetween() {
        let range = TimeRange.relative(duration: 3600)
        XCTAssertEqual(range.applied(to: "logs today | where x == 1"), "logs last 1h | where x == 1")
        XCTAssertEqual(
            range.applied(to: "spans between(1000, 2000) | take 5"),
            "spans last 1h | take 5"
        )
        XCTAssertEqual(range.applied(to: "logs yesterday"), "logs last 1h")
    }

    func testLeavesUnrewritableQueriesUntouched() {
        let range = TimeRange.relative(duration: 3600)
        // Not a signal keyword.
        XCTAssertEqual(range.applied(to: "foobar last 1h"), "foobar last 1h")
        // `signal(id)` shorthand has no time-scope.
        XCTAssertEqual(range.applied(to: "span(abc123)"), "span(abc123)")
        // Signal with no time-scope (malformed) — left as-is rather than corrupted.
        XCTAssertEqual(range.applied(to: "resources"), "resources")
    }

    func testPreservesLeadingWhitespace() {
        let range = TimeRange.relative(duration: 3600)
        XCTAssertEqual(range.applied(to: "  spans last 30m"), "  spans last 1h")
    }
}
