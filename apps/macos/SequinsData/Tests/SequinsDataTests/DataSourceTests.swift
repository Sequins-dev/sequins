import XCTest
@testable import SequinsData

final class DataSourceTests: XCTestCase {
    var tempDir: URL!

    override func setUp() async throws {
        // Create a temporary directory for test databases
        tempDir = FileManager.default.temporaryDirectory
            .appendingPathComponent(UUID().uuidString)
        try FileManager.default.createDirectory(at: tempDir, withIntermediateDirectories: true)
    }

    override func tearDown() async throws {
        // Clean up temporary directory
        if FileManager.default.fileExists(atPath: tempDir.path) {
            try? FileManager.default.removeItem(at: tempDir)
        }
    }

    func testCreateLocalDataSource() throws {
        let dbPath = tempDir.appendingPathComponent("test.db").path
        let config = OTLPServerConfig(grpcPort: 4317, httpPort: 4318)

        let dataSource = try DataSource.local(dbPath: dbPath, config: config)
        XCTAssertNotNil(dataSource)

        // DataSource should be automatically freed when it goes out of scope
    }

    func testCreateLocalDataSourceMultipleTimes() throws {
        // Two independent data sources must use different ports
        let dbPath1 = tempDir.appendingPathComponent("test1.db").path
        let dbPath2 = tempDir.appendingPathComponent("test2.db").path
        let config1 = OTLPServerConfig(grpcPort: 14317, httpPort: 14318)
        let config2 = OTLPServerConfig(grpcPort: 14319, httpPort: 14320)

        let dataSource1 = try DataSource.local(dbPath: dbPath1, config: config1)
        let dataSource2 = try DataSource.local(dbPath: dbPath2, config: config2)

        XCTAssertNotNil(dataSource1)
        XCTAssertNotNil(dataSource2)
    }

    func testCreateRemoteDataSource() throws {
        let queryURL = "http://localhost:8080/query"
        let managementURL = "http://localhost:8080/management"

        let dataSource = try DataSource.remote(queryURL: queryURL, managementURL: managementURL)
        XCTAssertNotNil(dataSource)
    }

    func testOTLPServerConfig() {
        // Test default configuration
        let defaultConfig = OTLPServerConfig()
        XCTAssertEqual(defaultConfig.grpcPort, 4317)
        XCTAssertEqual(defaultConfig.httpPort, 4318)

        // Test custom configuration
        let customConfig = OTLPServerConfig(grpcPort: 5000, httpPort: 5001)
        XCTAssertEqual(customConfig.grpcPort, 5000)
        XCTAssertEqual(customConfig.httpPort, 5001)
    }
}
