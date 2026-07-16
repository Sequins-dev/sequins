import SwiftUI
import SequinsData

/// Sidebar list of past conversations plus a "New chat" action.
struct ChatListPanel: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: AssistantViewModel

    var body: some View {
        VStack(spacing: 0) {
            HStack {
                Text("Chats")
                    .font(.headline)
                Spacer()
                Button {
                    viewModel.newChat()
                } label: {
                    Image(systemName: "square.and.pencil")
                }
                .buttonStyle(.borderless)
                .help("New chat")
            }
            .padding(.horizontal, 10)
            .padding(.vertical, 8)

            Divider()

            if viewModel.conversations.isEmpty {
                VStack(spacing: 6) {
                    Image(systemName: "bubble.left.and.text.bubble.right")
                        .font(.title2)
                        .foregroundStyle(.tertiary)
                    Text("No chats yet")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                }
                .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else {
                List(selection: selectionBinding) {
                    ForEach(viewModel.conversations) { convo in
                        VStack(alignment: .leading, spacing: 2) {
                            Text(convo.displayTitle)
                                .lineLimit(1)
                            Text("\(convo.itemCount) message\(convo.itemCount == 1 ? "" : "s")")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        .tag(convo.id)
                    }
                }
                .listStyle(.sidebar)
            }
        }
        .background(Color(nsColor: .controlBackgroundColor).opacity(0.5))
    }

    private var selectionBinding: Binding<String?> {
        Binding(
            get: { viewModel.selectedConversationId },
            set: { newValue in
                if let id = newValue, let ds = appState.dataSource {
                    viewModel.openConversation(id, dataSource: ds)
                }
            }
        )
    }
}
