import Foundation
import SwiftUI
import SwiftData
import SequinsData

@MainActor
@Observable
final class AppStateViewModel {
    // Environment management
    let environmentManager = EnvironmentManager()

    // MARK: - Always-on local OTLP server
    //
    // The embedded server runs continuously regardless of which environment is
    // selected in the UI.  This mirrors the behaviour of remote environments
    // (which collect data independently of the app) and lets the local profile
    // gather telemetry even while the user is inspecting a remote data source.

    /// The always-on local DataSource that owns the embedded storage + OTLP server.
    private var localDataSource: DataSource?

    /// Status of the embedded local OTLP server.
    private(set) var localServerStatus: ServerStatus = .stopped

    // MARK: - Query data source (follows selected environment)

    /// DataSource used by the UI for queries. Points to `localDataSource` when
    /// Development is selected, or to a remote client otherwise.
    var dataSource: DataSource?
    var dataSourceError: String?
    var dataSourceId: UUID = UUID()

    /// Status of the active query connection.
    var serverStatus: ServerStatus = .stopped

    // UI State
    var selectedService: Service?
    var selectedView: NavigationItem = .traces
    var showSettings = false
    var showServerInfo = false
    var isServiceAttributesExpanded = false

    // Time range state (manages live vs paused modes with separate settings)
    let timeRangeState = TimeRangeState()

    // Health monitoring service
    let healthMonitorService = HealthMonitorService()

    // Log level filter (shared across tabs)
    var selectedLogLevels: Set<LogSeverity> = Set(LogSeverity.allCases)

    // MARK: - Time Range Convenience Accessors

    var isLive: Bool {
        get { timeRangeState.isLive }
        set { timeRangeState.isLive = newValue }
    }

    var startTime: Date { timeRangeState.startTime }
    var endTime: Date { timeRangeState.endTime }

    func refreshTimeRange() {}

    func setCustomTimeRange(start: Date, end: Date) {
        timeRangeState.setCustomRange(start: start, end: end)
    }

    init() {}

    func configure(with modelContext: ModelContext) {
        environmentManager.configure(with: modelContext)
    }

    // MARK: - Connection entry points

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
        healthMonitorService.stop()

        if !environment.isSelected {
            environmentManager.selectEnvironment(environment)
        }

        // Always start the local server (no-op if already running).
        startLocalServerIfNeeded()

        // Wire up the query data source for the selected environment.
        if environment.isLocal {
            connectQueryToLocal()
        } else {
            connectRemoteEnvironment(environment)
        }
    }

    /// Reconnect the query data source, restarting the local server only if it's not healthy.
    func reconnect() {
        // Only restart the local server if it's not currently running.
        // If it's healthy, keep it alive to avoid the port-release/rebind race.
        if case .running = localServerStatus {
            // Local server is healthy — keep it, just reset the query connection.
        } else {
            localDataSource = nil
            localServerStatus = .stopped
        }

        // Reset the query data source and emit a sentinel UUID so any in-flight
        // .task(id: dataSourceId) bodies cancel and return early.
        dataSource = nil
        dataSourceError = nil
        serverStatus = .stopped
        dataSourceId = UUID()
        healthMonitorService.stop()

        // Re-connect. For a healthy local server, connectQueryToLocal() will wire up
        // the query data source synchronously and issue a second dataSourceId change
        // immediately — so live queries restart in the same run-loop cycle.
        connectToDataSource()
    }

    func switchToEnvironment(_ environment: ConnectionEnvironment) {
        // Keep the local server running — only reset the query connection.
        dataSource = nil
        dataSourceError = nil
        serverStatus = .stopped
        healthMonitorService.stop()
        connectToEnvironment(environment)
    }

    // MARK: - Private helpers

    /// Starts the embedded local server if it is not already running.
    private func startLocalServerIfNeeded() {
        guard localDataSource == nil,
              let localEnv = environmentManager.localEnvironment,
              let dbPath = localEnv.dbPath else { return }

        let grpcPort = localEnv.effectiveGrpcPort
        let httpPort = localEnv.effectiveHttpPort
        let config = OTLPServerConfig(grpcPort: grpcPort, httpPort: httpPort)

        localServerStatus = .starting

        // If local env is currently selected, mirror the starting state in serverStatus.
        if environmentManager.selectedEnvironment?.isLocal == true {
            serverStatus = .starting
        }

        Task {
            do {
                let ds = try await Task.detached(priority: .userInitiated) {
                    try DataSource.local(dbPath: dbPath, config: config)
                }.value

                self.localDataSource = ds
                self.localServerStatus = .running(grpcPort: grpcPort, httpPort: httpPort)
                print("✅ Local OTLP server running — gRPC: \(grpcPort), HTTP: \(httpPort)")

                // If local env is still selected, hook up the query data source.
                if self.environmentManager.selectedEnvironment?.isLocal == true {
                    self.dataSource = ds
                    self.dataSourceId = UUID()
                    self.serverStatus = .running(grpcPort: grpcPort, httpPort: httpPort)
                    self.healthMonitorService.start(
                        dataSource: ds,
                        environmentId: localEnv.id.uuidString
                    )
                }
            } catch {
                self.localDataSource = nil
                self.localServerStatus = .error(error.localizedDescription)
                print("❌ Local OTLP server failed: \(error)")

                if self.environmentManager.selectedEnvironment?.isLocal == true {
                    self.dataSourceError = error.localizedDescription
                    self.serverStatus = .error(error.localizedDescription)
                }
            }
        }
    }

    /// Sets up the query data source to use the already-running local server.
    private func connectQueryToLocal() {
        if let localDs = localDataSource {
            // Server is already up — connect immediately.
            dataSource = localDs
            dataSourceId = UUID()
            serverStatus = localServerStatus
            if let localEnv = environmentManager.localEnvironment {
                healthMonitorService.start(
                    dataSource: localDs,
                    environmentId: localEnv.id.uuidString
                )
            }
        } else {
            // Server is still starting (startLocalServerIfNeeded is in flight).
            // When the Task completes it will set dataSource and serverStatus.
            serverStatus = localServerStatus // .starting or .stopped
        }
    }

    private func connectRemoteEnvironment(_ environment: ConnectionEnvironment) {
        guard let queryURL = environment.remoteQueryURL,
              let managementURL = environment.remoteManagementURL else {
            dataSourceError = "Remote URLs not configured"
            serverStatus = .error("Remote URLs not configured")
            return
        }

        do {
            dataSource = try DataSource.remote(
                queryURL: queryURL,
                managementURL: managementURL
            )
            dataSourceId = UUID()
            serverStatus = .connected
            print("✅ Connected to remote: \(queryURL)")
            if let ds = dataSource {
                healthMonitorService.start(dataSource: ds, environmentId: environment.id.uuidString)
            }
        } catch {
            dataSourceError = "Failed to connect: \(error.localizedDescription)"
            dataSource = nil
            dataSourceId = UUID()
            serverStatus = .error(error.localizedDescription)
            print("❌ Failed to connect to remote: \(error)")
        }
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

    var id: String { rawValue }

    var systemImage: String {
        switch self {
        case .health: return "heart.fill"
        case .metrics: return "chart.line.uptrend.xyaxis"
        case .traces: return "arrow.triangle.branch"
        case .logs: return "doc.text"
        case .profiles: return "flame"
        case .explore: return "terminal"
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
