import SwiftUI
import SequinsData

/// Sidebar list of past conversations plus a "New chat" action.
struct ChatListPanel: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: AssistantViewModel

    /// The conversation pending deletion (drives the confirmation dialog).
    @State private var conversationToDelete: ConversationSummary?

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
                        ConversationRow(convo: convo) {
                            conversationToDelete = convo
                        }
                        .tag(convo.id)
                    }
                }
                .listStyle(.sidebar)
            }
        }
        .background(Color(nsColor: .controlBackgroundColor).opacity(0.5))
        .confirmationDialog(
            "Delete this chat?",
            isPresented: Binding(
                get: { conversationToDelete != nil },
                set: { if !$0 { conversationToDelete = nil } }
            ),
            presenting: conversationToDelete
        ) { convo in
            Button("Delete", role: .destructive) {
                if let ds = appState.dataSource {
                    viewModel.deleteConversation(convo.id, dataSource: ds)
                }
                conversationToDelete = nil
            }
            Button("Cancel", role: .cancel) { conversationToDelete = nil }
        } message: { convo in
            Text("“\(convo.displayTitle)” will be permanently deleted.")
        }
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

/// One conversation row: title + message count, with a delete (✕) button revealed on
/// hover.
private struct ConversationRow: View {
    let convo: ConversationSummary
    let onDelete: () -> Void

    @State private var hovering = false

    var body: some View {
        HStack(spacing: 6) {
            VStack(alignment: .leading, spacing: 2) {
                Text(convo.displayTitle)
                    .lineLimit(1)
                Text("\(convo.itemCount) message\(convo.itemCount == 1 ? "" : "s")")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }
            Spacer(minLength: 0)
            if hovering {
                Button(action: onDelete) {
                    Image(systemName: "xmark")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                .buttonStyle(.borderless)
                .help("Delete chat")
            }
        }
        .contentShape(Rectangle())
        .onHover { hovering = $0 }
    }
}
