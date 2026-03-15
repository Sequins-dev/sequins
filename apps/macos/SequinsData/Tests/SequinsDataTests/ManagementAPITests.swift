import XCTest
@testable import SequinsData

final class ManagementAPITests: XCTestCase {
    func testRetentionPolicyCreation() {
        let policy = RetentionPolicy(
            spansRetention: 86400,      // 1 day
            logsRetention: 604800,      // 7 days
            metricsRetention: 2592000,  // 30 days
            profilesRetention: 172800   // 2 days
        )

        XCTAssertEqual(policy.spansRetention, 86400)
        XCTAssertEqual(policy.logsRetention, 604800)
        XCTAssertEqual(policy.metricsRetention, 2592000)
        XCTAssertEqual(policy.profilesRetention, 172800)
    }

    func testStorageStats() {
        let stats = StorageStats(
            spanCount: 100,
            logCount: 200,
            metricCount: 300,
            profileCount: 50
        )

        XCTAssertEqual(stats.spanCount, 100)
        XCTAssertEqual(stats.logCount, 200)
        XCTAssertEqual(stats.metricCount, 300)
        XCTAssertEqual(stats.profileCount, 50)
    }

    func testMaintenanceStats() {
        let stats = MaintenanceStats(
            entriesEvicted: 42,
            batchesFlushed: 7
        )

        XCTAssertEqual(stats.entriesEvicted, 42)
        XCTAssertEqual(stats.batchesFlushed, 7)
    }

    // Note: Integration tests with actual FFI calls are disabled for now
    // The storage layer needs to be properly initialized before we can test management operations
    //
    // func testUpdateRetentionPolicy() async throws { ... }
    // func testGetRetentionPolicy() async throws { ... }
    // func testGetStorageStats() async throws { ... }
    // func testRunRetentionCleanup() async throws { ... }
    // func testRunMaintenance() async throws { ... }
}
