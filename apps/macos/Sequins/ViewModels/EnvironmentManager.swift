import Foundation
import SwiftData
import SwiftUI

@Observable
final class EnvironmentManager {
    private var modelContext: ModelContext?
    private(set) var environments: [ConnectionEnvironment] = []
    private(set) var selectedEnvironment: ConnectionEnvironment?

    private let defaultDbPath: String

    init() {
        // Default storage directory in user's Library/Application Support
        let appSupport = FileManager.default.urls(
            for: .applicationSupportDirectory,
            in: .userDomainMask
        ).first!
        let sequinsDir = appSupport.appendingPathComponent("SequinsSwift", isDirectory: true)
        // New Storage uses a directory, not a file path
        self.defaultDbPath = sequinsDir.path

        // Ensure directory exists
        try? FileManager.default.createDirectory(
            at: sequinsDir,
            withIntermediateDirectories: true
        )
    }

    func configure(with modelContext: ModelContext) {
        self.modelContext = modelContext
        loadEnvironments()
        ensureLocalEnvironmentExists()
    }

    private func loadEnvironments() {
        guard let modelContext else { return }

        let descriptor = FetchDescriptor<ConnectionEnvironment>(
            sortBy: [SortDescriptor(\.createdAt)]
        )

        do {
            let fetched = try modelContext.fetch(descriptor)
            // Sort with local/development first, then by creation date
            environments = fetched.sorted { lhs, rhs in
                if lhs.isLocal != rhs.isLocal {
                    return lhs.isLocal // Local comes first
                }
                return lhs.createdAt < rhs.createdAt
            }
            selectedEnvironment = environments.first { $0.isSelected }
        } catch {
            print("Failed to fetch environments: \(error)")
            environments = []
        }
    }

    private func ensureLocalEnvironmentExists() {
        guard let modelContext else { return }

        // Check if local environment exists
        var localEnv = environments.first { $0.isLocal }

        if localEnv == nil {
            // Create the local environment (created with isSelected: true)
            let newLocal = ConnectionEnvironment.createLocalEnvironment(dbPath: defaultDbPath)
            modelContext.insert(newLocal)
            saveAndReload()
            localEnv = environments.first { $0.isLocal }
        }

        // Ensure at least one environment is selected (default to development/local)
        if selectedEnvironment == nil, let localEnv {
            selectEnvironment(localEnv)
        }
    }

    private func saveAndReload() {
        guard let modelContext else { return }

        do {
            try modelContext.save()
            loadEnvironments()
        } catch {
            print("Failed to save environments: \(error)")
        }
    }

    // MARK: - Environment Operations

    func selectEnvironment(_ environment: ConnectionEnvironment) {
        guard modelContext != nil else { return }

        // Deselect all environments
        for env in environments {
            env.isSelected = false
        }

        // Select the specified environment
        environment.isSelected = true
        saveAndReload()
    }

    func addRemoteEnvironment(name: String, queryURL: String, managementURL: String) -> ConnectionEnvironment {
        guard let modelContext else {
            fatalError("EnvironmentManager not configured with ModelContext")
        }

        let environment = ConnectionEnvironment.createRemoteEnvironment(
            name: name,
            queryURL: queryURL,
            managementURL: managementURL
        )
        modelContext.insert(environment)
        saveAndReload()
        return environment
    }

    func updateEnvironment(_ environment: ConnectionEnvironment) {
        saveAndReload()
    }

    func deleteEnvironment(_ environment: ConnectionEnvironment) {
        guard let modelContext else { return }
        guard environment.canDelete else {
            print("Cannot delete local environment")
            return
        }

        // If deleting selected environment, switch to local
        if environment.isSelected {
            if let localEnv = environments.first(where: { $0.isLocal }) {
                localEnv.isSelected = true
            }
        }

        modelContext.delete(environment)
        saveAndReload()
    }

    /// The local environment (always exists after configure is called)
    var localEnvironment: ConnectionEnvironment? {
        environments.first { $0.isLocal }
    }

    /// All remote environments
    var remoteEnvironments: [ConnectionEnvironment] {
        environments.filter { !$0.isLocal }
    }
}
