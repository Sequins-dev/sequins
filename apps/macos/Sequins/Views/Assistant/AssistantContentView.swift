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
                Divider()
                ChatInputBar(viewModel: viewModel, onSend: sendMessage)
            }
        }
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
            Button("Open Settings") { appState.showSettings = true }
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
