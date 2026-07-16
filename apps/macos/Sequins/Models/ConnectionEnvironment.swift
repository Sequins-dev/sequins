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

    // Assistant (AI) settings — the API key / bearer token is stored in the Keychain
    // (keyed by this environment's id), NOT here.
    /// Local: the OpenAI-compatible provider base URL (nil = api.openai.com).
    /// Remote: the daemon's assistant `/v1` base URL (e.g. `http://host:8082/v1`).
    var assistantBaseURL: String?
    /// The model id to use (required for local; optional for remote).
    var assistantModel: String?

    init(
        id: UUID = UUID(),
        name: String,
        isLocal: Bool,
        isSelected: Bool = false,
        dbPath: String? = nil,
        grpcPort: Int? = nil,
        httpPort: Int? = nil,
        remoteQueryURL: String? = nil,
        remoteManagementURL: String? = nil,
        assistantBaseURL: String? = nil,
        assistantModel: String? = nil
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
        self.assistantBaseURL = assistantBaseURL
        self.assistantModel = assistantModel
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
