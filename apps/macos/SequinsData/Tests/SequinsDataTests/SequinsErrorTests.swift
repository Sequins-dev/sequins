import XCTest
@testable import SequinsData

final class SequinsErrorTests: XCTestCase {
    func testErrorCreation() {
        let error = SequinsError.ffiError("Test error message")
        XCTAssertEqual(error.localizedDescription, "Test error message")
    }

    func testNullPointerError() {
        let error = SequinsError.nullPointer
        XCTAssert(error.localizedDescription.contains("null"))
    }

    func testInvalidUtf8Error() {
        let error = SequinsError.invalidUtf8
        XCTAssert(error.localizedDescription.contains("UTF-8"))
    }
}
