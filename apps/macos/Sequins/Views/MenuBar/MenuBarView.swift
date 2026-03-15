import SwiftUI

struct MenuBarView: View {
    @Environment(AppStateViewModel.self) private var appState
    @Environment(\.openSettings) private var openSettings

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Header with status
            HStack {
                Text("Sequins")
                    .font(.headline)
                Spacer()
                Circle()
                    .fill(appState.serverStatus.statusColor)
                    .frame(width: 8, height: 8)
            }
            .padding()

            // Current environment indicator
            if let env = appState.environmentManager.selectedEnvironment {
                HStack(spacing: 6) {
                    Image(systemName: env.isLocal ? "laptopcomputer" : "network")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(env.name)
                        .font(.subheadline)
                    Spacer()
                    Text(appState.serverStatus.statusText)
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .padding(.horizontal)
                .padding(.bottom, 8)
            }

            Divider()

            // Actions
            VStack(alignment: .leading, spacing: 4) {
                Button(action: openMainWindow) {
                    Label("Open Sequins", systemImage: "macwindow")
                }
                .keyboardShortcut("o", modifiers: .command)

                SettingsLink {
                    Label("Settings...", systemImage: "gearshape")
                }
                .keyboardShortcut(",", modifiers: .command)

                Divider()

                Button(action: { NSApplication.shared.terminate(nil) }) {
                    Label("Quit", systemImage: "power")
                }
                .keyboardShortcut("q", modifiers: .command)
            }
            .buttonStyle(.plain)
            .padding()
        }
        .frame(width: 220)
    }

    private func openMainWindow() {
        MainWindowController.shared.showWindow(appState: appState)
    }
}

#Preview {
    MenuBarView()
        .environment(AppStateViewModel())
}
