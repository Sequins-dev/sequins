import SwiftUI
import SwiftData
import SequinsData

@main
struct SequinsApp: App {
    @State private var appState: AppStateViewModel
    private let modelContainer: ModelContainer

    init() {
        // Bootstrap structured logging before anything else
        SequinsLogging.bootstrap()

        // Create model container first
        let container: ModelContainer
        do {
            let schema = Schema([ConnectionEnvironment.self])
            let modelConfiguration = ModelConfiguration(
                schema: schema,
                isStoredInMemoryOnly: false
            )
            container = try ModelContainer(
                for: schema,
                configurations: [modelConfiguration]
            )
        } catch {
            fatalError("Could not create ModelContainer: \(error)")
        }
        self.modelContainer = container

        // Create appState and auto-start the OTLP server immediately
        let state = AppStateViewModel()
        state.configure(with: container.mainContext)
        state.connectToDataSource()
        // print("🚀 SequinsApp: OTLP server starting on app launch")

        self._appState = State(initialValue: state)

        // Set up notification service
        setupNotifications(appState: state)
    }

    private func setupNotifications(appState: AppStateViewModel) {
        // Register notification categories
        NotificationService.shared.registerCategories()

        // Request notification authorization
        Task {
            await NotificationService.shared.requestAuthorization()
        }

        // Set up notification click handler
        NotificationService.shared.onNotificationClicked = { [weak appState] serviceName, environmentId in
            guard let appState = appState else { return }

            // If we have an environment ID, find and switch to that environment
            if let envId = environmentId,
               let uuid = UUID(uuidString: envId),
               let environment = appState.environmentManager.environments.first(where: { $0.id == uuid }) {
                if !environment.isSelected {
                    appState.switchToEnvironment(environment)
                }
            }

            // TODO: Find and select the service with SeQL
            // For now, just navigate to health tab
            appState.selectedView = .health
        }
    }

    var body: some Scene {
        MenuBarExtra("Sequins", image: "MenuBarIcon") {
            MenuBarView()
                .environment(appState)
        }
        .menuBarExtraStyle(.window)

        #if os(macOS)
        Settings {
            SettingsView()
                .environment(appState)
        }
        #endif
    }
}
