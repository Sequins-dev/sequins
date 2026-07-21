import Foundation
import SwiftUI
import SwiftData
import SequinsData

@MainActor
@Observable
final class AppStateViewModel {
    // Environment management
    let environmentManager = EnvironmentManager()

    // DataSource (local or remote)
    var dataSource: DataSource?
    var dataSourceError: String?
    var dataSourceId: UUID = UUID() // Changes when dataSource changes

    // Server status
    var serverStatus: ServerStatus = .stopped

    // UI State
    var selectedService: Service?
    var selectedView: NavigationItem = .traces
    var showServerInfo = false
    var isServiceAttributesExpanded = false

    // Time range state (manages live vs paused modes with separate settings)
    let timeRangeState = TimeRangeState()

    // Health monitoring service
    let healthMonitorService = HealthMonitorService()

    // Log level filter (shared across tabs)
    var selectedLogLevels: Set<LogSeverity> = Set(LogSeverity.allCases)

    // MARK: - Time Range Convenience Accessors

    /// Whether we're in live mode (continuously updating)
    var isLive: Bool {
        get { timeRangeState.isLive }
        set { timeRangeState.isLive = newValue }
    }

    /// Current start time for queries
    var startTime: Date {
        timeRangeState.startTime
    }

    /// Current end time for queries
    var endTime: Date {
        timeRangeState.endTime
    }

    /// Refresh the time range (call before querying in live mode)
    func refreshTimeRange() {
        // In live mode, startTime/endTime are computed properties that always use Date()
        // No explicit refresh needed - just accessing them gets fresh values
        // This method exists for compatibility with existing code
    }

    /// Set a custom absolute time range (switches to paused mode)
    func setCustomTimeRange(start: Date, end: Date) {
        timeRangeState.setCustomRange(start: start, end: end)
    }

    /// Build the assistant's LLM/daemon config from the selected environment plus the
    /// Keychain-stored secret. Returns `nil` when the assistant is not configured for
    /// the current environment (so the UI can prompt the user to set it up).
    ///
    /// The model is chosen at runtime in the Assistant tab (from the provider's model
    /// list), so it isn't required here — only enough to reach the provider/daemon:
    /// - Local: an API key (base URL optional → api.openai.com).
    /// - Remote: the daemon `/v1` base URL + a bearer token.
    func assistantConfig() -> AssistantConfig? {
        if let env = environmentManager.selectedEnvironment {
            let secret = KeychainStore.shared.assistantSecret(environmentId: env.id)
            let base = env.assistantBaseURL?.isEmpty == false ? env.assistantBaseURL : nil
            let model = env.assistantModel?.isEmpty == false ? env.assistantModel : nil
            if let secret, !secret.isEmpty {
                if env.isLocal {
                    return AssistantConfig(baseURL: base, model: model, apiKey: secret)
                } else if let base {
                    return AssistantConfig(baseURL: base, model: model, apiKey: secret)
                }
            }
        }
        // Dev/CI fallback: OPENAI_* environment variables (mirrors the daemon).
        return AssistantConfig.fromProcessEnvironment()
    }

    init() {
        // Don't initialize data source in init - let it happen lazily
        // This prevents crashes during app startup
    }

    func configure(with modelContext: ModelContext) {
        environmentManager.configure(with: modelContext)
    }

    func connectToDataSource() {
        guard let environment = environmentManager.selectedEnvironment else {
            dataSourceError = "No environment selected"
            serverStatus = .error("No environment selected")
            return
        }

        connectToEnvironment(environment)
    }

    func connectToEnvironment(_ environment: ConnectionEnvironment) {
        dataSourceError = nil
        serverStatus = .starting

        // Stop health monitoring before switching
        healthMonitorService.stop()

        // Update selection if different
        if !environment.isSelected {
            environmentManager.selectEnvironment(environment)
        }

        do {
            if environment.isLocal {
                guard let dbPath = environment.dbPath else {
                    dataSourceError = "No database path configured"
                    serverStatus = .error("No database path configured")
                    return
                }

                print("📂 Using database path: \(dbPath)")

                let config = OTLPServerConfig(
                    grpcPort: environment.effectiveGrpcPort,
                    httpPort: environment.effectiveHttpPort
                )
                dataSource = try DataSource.local(
                    dbPath: dbPath,
                    config: config
                )
                dataSourceId = UUID() // Trigger view updates
                serverStatus = .running(
                    grpcPort: environment.effectiveGrpcPort,
                    httpPort: environment.effectiveHttpPort
                )
                print("✅ OTLP server started - gRPC: \(environment.effectiveGrpcPort), HTTP: \(environment.effectiveHttpPort)")
            } else {
                guard let queryURL = environment.remoteQueryURL,
                      let managementURL = environment.remoteManagementURL else {
                    dataSourceError = "Remote URLs not configured"
                    serverStatus = .error("Remote URLs not configured")
                    return
                }

                dataSource = try DataSource.remote(
                    queryURL: queryURL,
                    managementURL: managementURL
                )
                dataSourceId = UUID() // Trigger view updates
                serverStatus = .connected
                print("✅ Connected to remote: \(queryURL)")
            }

            // Start health monitoring with the new data source
            if let ds = dataSource {
                healthMonitorService.start(
                    dataSource: ds,
                    environmentId: environment.id.uuidString
                )
            }
        } catch {
            dataSourceError = "Failed to connect: \(error.localizedDescription)"
            dataSource = nil
            dataSourceId = UUID() // Trigger view updates even on error
            serverStatus = .error(error.localizedDescription)
            print("❌ Failed to start data source: \(error)")
        }
    }

    func reconnect() {
        dataSource = nil
        serverStatus = .stopped
        connectToDataSource()
    }

    func switchToEnvironment(_ environment: ConnectionEnvironment) {
        dataSource = nil
        serverStatus = .stopped
        connectToEnvironment(environment)
    }
}

enum ServerStatus: Equatable {
    case stopped
    case starting
    case running(grpcPort: UInt16, httpPort: UInt16)
    case connected // Remote mode
    case error(String)

    var isRunning: Bool {
        if case .running = self { return true }
        if case .connected = self { return true }
        return false
    }

    var statusText: String {
        switch self {
        case .stopped: return "Stopped"
        case .starting: return "Starting..."
        case .running: return "Running"
        case .connected: return "Connected"
        case .error: return "Error"
        }
    }

    var statusColor: Color {
        switch self {
        case .stopped: return .secondary
        case .starting: return .orange
        case .running, .connected: return .green
        case .error: return .red
        }
    }
}

enum NavigationItem: String, CaseIterable, Identifiable {
    case health = "Health"
    case metrics = "Metrics"
    case traces = "Traces"
    case logs = "Logs"
    case profiles = "Profiles"
    case explore = "Explore"
    case assistant = "Assistant"
    case dashboards = "Dashboards"

    var id: String { rawValue }

    var systemImage: String {
        switch self {
        case .health: return "heart.fill"
        case .metrics: return "chart.line.uptrend.xyaxis"
        case .traces: return "arrow.triangle.branch"
        case .logs: return "doc.text"
        case .profiles: return "flame"
        case .explore: return "terminal"
        case .assistant: return "bubble.left.and.text.bubble.right"
        case .dashboards: return "square.grid.2x2"
        }
    }
}

enum LogSeverity: String, CaseIterable {
    case error = "Error"
    case warn = "Warn"
    case info = "Info"
    case debug = "Debug"
    case trace = "Trace"
    case fatal = "Fatal"

    /// Initialize from SequinsData.LogSeverity
    init(from dataSeverity: SequinsData.LogSeverity) {
        switch dataSeverity {
        case .trace: self = .trace
        case .debug: self = .debug
        case .info: self = .info
        case .warn: self = .warn
        case .error: self = .error
        case .fatal: self = .fatal
        }
    }

    /// Convert to SequinsData.LogSeverity for queries
    var dataLogSeverity: SequinsData.LogSeverity {
        switch self {
        case .trace: return .trace
        case .debug: return .debug
        case .info: return .info
        case .warn: return .warn
        case .error: return .error
        case .fatal: return .fatal
        }
    }

    var color: Color {
        switch self {
        case .error: return .red
        case .warn: return .orange
        case .info: return .blue
        case .debug: return .teal
        case .trace: return .mint
        case .fatal: return .purple
        }
    }

    var emoji: String {
        switch self {
        case .error: return "🔴"
        case .warn: return "🟠"
        case .info: return "🔵"
        case .debug: return "⚪"
        case .trace: return "⚫"
        case .fatal: return "💀"
        }
    }
}
