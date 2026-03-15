import Foundation
import SwiftData

@Model
final class ConnectionEnvironment {
    var id: UUID
    var name: String
    var isLocal: Bool
    var isSelected: Bool
    var createdAt: Date

    // Local mode settings
    var dbPath: String?
    var grpcPort: Int?
    var httpPort: Int?

    // Remote mode settings
    var remoteQueryURL: String?
    var remoteManagementURL: String?

    init(
        id: UUID = UUID(),
        name: String,
        isLocal: Bool,
        isSelected: Bool = false,
        dbPath: String? = nil,
        grpcPort: Int? = nil,
        httpPort: Int? = nil,
        remoteQueryURL: String? = nil,
        remoteManagementURL: String? = nil
    ) {
        self.id = id
        self.name = name
        self.isLocal = isLocal
        self.isSelected = isSelected
        self.createdAt = Date()
        self.dbPath = dbPath
        self.grpcPort = grpcPort
        self.httpPort = httpPort
        self.remoteQueryURL = remoteQueryURL
        self.remoteManagementURL = remoteManagementURL
    }

    /// The fixed UUID for the local environment - never changes
    static let localEnvironmentId = UUID(uuidString: "00000000-0000-0000-0000-000000000001")!

    /// Creates the default local/development environment
    static func createLocalEnvironment(dbPath: String) -> ConnectionEnvironment {
        ConnectionEnvironment(
            id: localEnvironmentId,
            name: "Development",
            isLocal: true,
            isSelected: true,
            dbPath: dbPath,
            grpcPort: 4317,
            httpPort: 4318
        )
    }

    /// Creates a new remote environment
    static func createRemoteEnvironment(
        name: String,
        queryURL: String,
        managementURL: String
    ) -> ConnectionEnvironment {
        ConnectionEnvironment(
            name: name,
            isLocal: false,
            isSelected: false,
            remoteQueryURL: queryURL,
            remoteManagementURL: managementURL
        )
    }

    /// Whether this environment can be deleted (local environment cannot)
    var canDelete: Bool {
        !isLocal
    }

    /// Effective gRPC port (defaults to 4317)
    var effectiveGrpcPort: UInt16 {
        UInt16(grpcPort ?? 4317)
    }

    /// Effective HTTP port (defaults to 4318)
    var effectiveHttpPort: UInt16 {
        UInt16(httpPort ?? 4318)
    }
}
