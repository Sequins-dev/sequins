import SwiftUI
import SequinsData

// MARK: - Content Only Wrapper

/// Wrapper following the *ContentOnly pattern — refreshes the conversation list when
/// the data source changes.
struct AssistantContentOnly: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: AssistantViewModel

    var body: some View {
        AssistantContentView(viewModel: viewModel)
            .task(id: appState.dataSourceId) {
                if let ds = appState.dataSource {
                    viewModel.refreshConversations(dataSource: ds)
                }
            }
    }
}

// MARK: - Assistant tab

/// The Assistant tab: a conversation list on the left, the active chat on the right.
struct AssistantContentView: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: AssistantViewModel

    var body: some View {
        HSplitView {
            ChatListPanel(viewModel: viewModel)
                .frame(minWidth: 200, idealWidth: 240, maxWidth: 340)

            chatDetail
                .frame(minWidth: 420, maxWidth: .infinity, maxHeight: .infinity)
        }
    }

    @ViewBuilder
    private var chatDetail: some View {
        if appState.dataSource == nil {
            VizMessage(icon: "bolt.slash", text: "Not connected to a data source")
        } else if appState.assistantConfig() == nil {
            configPrompt
        } else {
            VStack(spacing: 0) {
                ChatTranscriptView(viewModel: viewModel)
                if let modelsError = viewModel.modelsError {
                    modelsErrorBanner(modelsError)
                }
                Divider()
                ChatInputBar(viewModel: viewModel, onSend: sendMessage)
            }
            // Load the provider's model list whenever the provider (base URL / key)
            // changes; the picker in the input bar selects among them.
            .task(id: assistantProviderKey) {
                if let config = appState.assistantConfig() {
                    await viewModel.loadModels(config: config)
                }
            }
            // Remember the chosen model on the environment so it persists across launches
            // and feeds `assistantConfig()`.
            .onChange(of: viewModel.selectedModel) { _, newValue in
                persistSelectedModel(newValue)
            }
        }
    }

    /// Identity of the current assistant provider — changes when the base URL or the
    /// presence of an API key changes, so the model list reloads.
    private var assistantProviderKey: String {
        let config = appState.assistantConfig()
        return "\(config?.resolvedBaseURL ?? "")|\(config?.apiKey?.isEmpty == false)"
    }

    /// A visible banner explaining why the model list is empty (e.g. an invalid API key
    /// or an unreachable provider), with a shortcut to fix it in Settings.
    private func modelsErrorBanner(_ message: String) -> some View {
        HStack(spacing: 8) {
            Image(systemName: "exclamationmark.triangle.fill")
                .foregroundStyle(.orange)
            VStack(alignment: .leading, spacing: 1) {
                Text("Couldn't load models")
                    .font(.caption.weight(.semibold))
                Text(message)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
                    .lineLimit(2)
            }
            Spacer()
            SettingsLink { Text("Settings") }
                .font(.caption)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 6)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(Color.orange.opacity(0.12))
    }

    /// Persist the selected model to the current environment (when there is one).
    private func persistSelectedModel(_ model: String?) {
        guard let env = appState.environmentManager.selectedEnvironment,
              env.assistantModel != model else { return }
        env.assistantModel = model
        appState.environmentManager.updateEnvironment(env)
    }

    private var configPrompt: some View {
        VStack(spacing: 12) {
            Image(systemName: "key.horizontal")
                .font(.system(size: 44))
                .foregroundStyle(.secondary)
            Text("Assistant not configured")
                .font(.title3)
            Text("Add an LLM model and API key for this environment in Settings to start chatting.")
                .font(.callout)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
                .frame(maxWidth: 320)
            SettingsLink { Text("Open Settings") }
                .buttonStyle(.borderedProminent)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .padding()
    }

    private func sendMessage() {
        guard let ds = appState.dataSource, let config = appState.assistantConfig() else { return }
        viewModel.send(dataSource: ds, config: config)
    }
}
