import XCTest
@testable import SequinsData

final class ServiceTypesTests: XCTestCase {
    func testServiceCreation() {
        let service = Service(name: "test-service", spanCount: 42, logCount: 100)

        XCTAssertEqual(service.id, "test-service")
        XCTAssertEqual(service.name, "test-service")
        XCTAssertEqual(service.spanCount, 42)
        XCTAssertEqual(service.logCount, 100)
    }

    func testServiceIdentifiable() {
        let service1 = Service(name: "service-a", spanCount: 10, logCount: 5)
        let service2 = Service(name: "service-b", spanCount: 20, logCount: 10)

        // id should be same as name
        XCTAssertEqual(service1.id, service1.name)
        XCTAssertNotEqual(service1.id, service2.id)
    }

    func testServiceHashable() {
        let service1 = Service(name: "test-service", spanCount: 42, logCount: 100)
        let service2 = Service(name: "test-service", spanCount: 42, logCount: 100)
        let service3 = Service(name: "other-service", spanCount: 42, logCount: 100)

        XCTAssertEqual(service1, service2)
        XCTAssertNotEqual(service1, service3)

        var set = Set<Service>()
        set.insert(service1)
        set.insert(service2) // Should not increase count (same service)
        set.insert(service3)

        XCTAssertEqual(set.count, 2)
    }
}
