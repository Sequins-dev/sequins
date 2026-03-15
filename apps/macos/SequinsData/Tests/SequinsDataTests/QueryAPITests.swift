import XCTest
@testable import SequinsData

final class QueryAPITests: XCTestCase {

    func testTimeRangeLastHour() {
        let range = TimeRange.last(hours: 1)
        XCTAssertEqual(range.duration, 3600, accuracy: 0.001)
        XCTAssertTrue(range.isLive)
    }

    func testTimeRangeLast24Hours() {
        let range = TimeRange.last(hours: 24)
        XCTAssertEqual(range.duration, 24 * 3600, accuracy: 0.001)
        XCTAssertTrue(range.isLive)
    }

    func testTimeRangeAbsolute() {
        let start = Date(timeIntervalSince1970: 1000)
        let end = Date(timeIntervalSince1970: 4600)
        let range = TimeRange.between(start, and: end)
        XCTAssertFalse(range.isLive)
        XCTAssertEqual(range.duration, 3600, accuracy: 0.001)
    }

    func testSeQLParseErrorMessage() {
        let err = SeQLParseError(message: "unexpected token", offset: 10)
        XCTAssertEqual(err.message, "unexpected token")
        XCTAssertEqual(err.offset, 10)
    }

    func testSeQLSchemaColumnNames() {
        let schema = SeQLSchema(shape: .table, columnNames: ["col_a", "col_b", "col_c"], initialWatermarkNs: 0)
        XCTAssertEqual(schema.columnNames.count, 3)
        XCTAssertEqual(schema.columnNames[0], "col_a")
        XCTAssertEqual(schema.columnNames[2], "col_c")
        XCTAssertEqual(schema.shape, .table)
    }

    func testSeQLStatsFields() {
        let stats = SeQLStats(executionTimeUs: 1234, rowsScanned: 100, bytesRead: 4096, rowsReturned: 42, warningCount: 0)
        XCTAssertEqual(stats.rowsReturned, 42)
        XCTAssertEqual(stats.executionTimeUs, 1234)
    }
}
