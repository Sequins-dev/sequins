import SwiftUI

struct WelcomeBanner: View {
    @Environment(AppStateViewModel.self) private var appState
    @Environment(\.openSettings) private var openSettings

    var body: some View {
        if !appState.preferences.hasAcknowledgedWelcome {
            HStack(alignment: .top, spacing: 16) {
                Image(systemName: "menubar.rectangle")
                    .font(.title2)
                    .foregroundStyle(.tint)
                VStack(alignment: .leading, spacing: 6) {
                    Text("Welcome to Sequins").font(.headline)
                    Text("Sequins keeps collecting telemetry when you close this window. Reopen it from the Sequins menu bar icon. You can change startup behavior in Settings.")
                        .font(.callout)
                        .foregroundStyle(.secondary)
                    HStack {
                        Button("Open Settings") {
                            appState.selectedSettingsTab = .general
                            openSettings()
                        }
                        Button("Got it") { appState.preferences.hasAcknowledgedWelcome = true }
                    }
                }
                Spacer(minLength: 0)
            }
            .padding(20)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(Color.accentColor.opacity(0.06))
            Divider()
        }
    }
}
