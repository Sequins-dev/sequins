import XCTest
@testable import SequinsData

final class VizFormatTests: XCTestCase {

    func testNumberGroupsThousands() {
        XCTAssertEqual(VizFormat.number(6784), "6,784")
        XCTAssertEqual(VizFormat.number(1_234_567), "1,234,567")
        XCTAssertEqual(VizFormat.number(42), "42")
        XCTAssertEqual(VizFormat.number(3.14159), "3.14")
    }

    func testCompact() {
        XCTAssertEqual(VizFormat.compact(1_500), "1.5k")
        XCTAssertEqual(VizFormat.compact(2_000_000), "2.0M")
        XCTAssertEqual(VizFormat.compact(3_000_000_000), "3.0B")
        XCTAssertEqual(VizFormat.compact(42), "42")
    }

    func testBytes() {
        XCTAssertEqual(VizFormat.bytes(1_048_576), "1.0 MB")
        XCTAssertEqual(VizFormat.bytes(1_073_741_824), "1.0 GB")
        XCTAssertEqual(VizFormat.bytes(512), "512 B")
    }

    func testDurationNs() {
        XCTAssertEqual(VizFormat.durationNs(500), "500 ns")
        XCTAssertEqual(VizFormat.durationNs(1_500_000), "1.50 ms")
        XCTAssertEqual(VizFormat.durationNs(2_500_000_000), "2.50 s")
    }

    func testPercent() {
        XCTAssertEqual(VizFormat.percent(12.5), "12.5%")
    }

    func testColumnHeuristics() {
        XCTAssertTrue(VizFormat.isTimeColumn("bucket"))
        XCTAssertTrue(VizFormat.isTimeColumn("start_time_unix_nano"))
        XCTAssertTrue(VizFormat.isDurationColumn("duration_ns"))
        XCTAssertTrue(VizFormat.isIdColumn("trace_id"))
        XCTAssertFalse(VizFormat.isIdColumn("count"))
    }

    func testLabelTitleCases() {
        XCTAssertEqual(VizFormat.label("total_spans"), "Total Spans")
        XCTAssertEqual(VizFormat.label("n"), "N")
        XCTAssertEqual(VizFormat.label("error_rate"), "Error Rate")
    }

    func testCellDispatch() {
        XCTAssertEqual(VizFormat.cell(6784 as Int64, type: .number, column: "total"), "6,784")
        XCTAssertEqual(VizFormat.cell(1_500_000 as Int64, type: .duration, column: "duration_ns"), "1.50 ms")
        XCTAssertEqual(VizFormat.cell(nil, type: .number, column: "x"), "—")
        XCTAssertEqual(VizFormat.cell(true, type: .boolean, column: "ok"), "true")
    }

    func testDateFromNanos() {
        // A large integer in a time column is treated as epoch nanoseconds.
        let nanos = 1_700_000_000_000_000_000 as Int64
        let date = VizFormat.date(nanos, column: "bucket")
        XCTAssertNotNil(date)
        XCTAssertEqual(date!.timeIntervalSince1970, 1_700_000_000, accuracy: 1)
        // A plain number in a non-time column is not a date.
        XCTAssertNil(VizFormat.date(42 as Int64, column: "count"))
    }
}
