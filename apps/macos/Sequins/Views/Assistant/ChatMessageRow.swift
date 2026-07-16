import SwiftUI
import SequinsData

/// Renders a single transcript item: a user/assistant message, a server tool-activity
/// card, or an inline visualization.
struct ChatMessageRow: View {
    let item: ChatItem
    let dataSource: DataSource?

    var body: some View {
        switch item.kind {
        case .userText(let text):
            messageBubble(text, role: .user)
        case .assistantText(let text):
            messageBubble(text, role: .assistant)
        case .toolActivity:
            // A lone tool call still renders in the shared full-width group container.
            ToolActivityGroup(items: [item])
        case .visualization(let viz):
            InlineVisualizationCard(visualization: viz, dataSource: dataSource)
        }
    }

    private enum Role { case user, assistant }

    /// A Markdown-rendered message bubble: user messages tint-filled and trailing,
    /// assistant messages neutral and leading. Both go through ``MarkdownText``.
    private func messageBubble(_ text: String, role: Role) -> some View {
        HStack {
            if role == .user { Spacer(minLength: 40) }
            MarkdownText(text: text)
                .textSelection(.enabled)
                .padding(.horizontal, 12)
                .padding(.vertical, 8)
                .background(bubbleColor(role), in: RoundedRectangle(cornerRadius: 12))
                .overlay(
                    RoundedRectangle(cornerRadius: 12)
                        .strokeBorder(Color.primary.opacity(0.08), lineWidth: 1)
                )
            if role == .assistant { Spacer(minLength: 40) }
        }
    }

    private func bubbleColor(_ role: Role) -> Color {
        switch role {
        // Distinct from the window background so the bubble reads as a bubble in both
        // light and dark (`controlBackgroundColor` blends into the transcript).
        case .user: return Color.accentColor.opacity(0.18)
        case .assistant: return Color.secondary.opacity(0.12)
        }
    }
}

/// Groups a run of consecutive tool calls into one full-width, contained block —
/// visually separating the model's tool work from the message bubbles around it.
struct ToolActivityGroup: View {
    let items: [ChatItem]

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            ForEach(Array(items.enumerated()), id: \.element.id) { index, item in
                if case .toolActivity(let activity) = item.kind {
                    if index > 0 {
                        Divider().opacity(0.4)
                    }
                    ToolActivityRow(activity: activity)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
        .padding(10)
        .background(Color.secondary.opacity(0.08), in: RoundedRectangle(cornerRadius: 10))
    }
}

/// One expandable tool call + its output. Background-less — the enclosing
/// ``ToolActivityGroup`` provides the shared container.
struct ToolActivityRow: View {
    let activity: AssistantToolActivity
    @State private var expanded = false

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Button {
                expanded.toggle()
            } label: {
                HStack(spacing: 6) {
                    Image(systemName: "wrench.and.screwdriver")
                        .font(.caption)
                        .foregroundStyle(.secondary)
                    Text(activity.name.isEmpty ? "tool" : activity.name)
                        .font(.caption.weight(.medium))
                    Spacer()
                    Image(systemName: expanded ? "chevron.down" : "chevron.right")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                }
                .contentShape(Rectangle())
            }
            .buttonStyle(.plain)

            if expanded {
                if !activity.arguments.isEmpty {
                    Text(activity.arguments)
                        .font(.system(.caption2, design: .monospaced))
                        .foregroundStyle(.secondary)
                        .textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                if !activity.output.isEmpty {
                    Text(activity.output)
                        .font(.system(.caption2, design: .monospaced))
                        .textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
        }
        .frame(maxWidth: .infinity, alignment: .leading)
    }
}
