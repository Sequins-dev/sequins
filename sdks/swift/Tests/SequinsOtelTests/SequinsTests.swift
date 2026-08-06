import XCTest
@testable import SequinsOtel

final class SequinsTests: XCTestCase {
    func testInitializeReturnsHandle() throws {
        let handle = try Sequins.initialize(serviceName: "test-service")
        XCTAssertNotNil(handle)
        handle.shutdown()
    }

    func testTracerProviderIsNotNil() throws {
        let handle = try Sequins.initialize(serviceName: "test-tracer")
        XCTAssertNotNil(handle.tracerProvider)
        handle.shutdown()
    }

    func testShutdownDoesNotCrash() throws {
        let handle = try Sequins.initialize(serviceName: "test-shutdown")
        handle.shutdown()
    }

    func testCustomConfig() throws {
        let config = SequinsConfig(
            serviceName: "custom-service",
            endpoint: "http://localhost:4318"
        )
        let handle = try Sequins.initialize(config: config)
        XCTAssertNotNil(handle)
        handle.shutdown()
    }
}
