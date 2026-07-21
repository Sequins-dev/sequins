import SwiftUI
import SequinsData

/// Virtualized, auto-scrolling chat transcript (mirrors the log terminal): a
/// `LazyVStack` in a `ScrollView` pinned to the bottom, scrolling to the newest
/// content as it streams in.
struct ChatTranscriptView: View {
    @Environment(AppStateViewModel.self) private var appState
    @Bindable var viewModel: AssistantViewModel

    /// A cheap signal that changes as new items arrive AND as the streaming assistant
    /// text grows (its item id is stable), so we keep scrolling during a long reply.
    private var scrollSignal: String {
        let count = viewModel.transcript.count
        var lastLen = 0
        if let last = viewModel.transcript.last, case .assistantText(let t) = last.kind {
            lastLen = t.count
        }
        return "\(count)-\(lastLen)-\(viewModel.isStreaming)-\(viewModel.pendingApproval?.id.uuidString ?? "")"
    }

    /// A transcript run: either a standalone message, or a group of consecutive tool
    /// calls that render together in one full-width container.
    private enum TranscriptSegment: Identifiable {
        case message(ChatItem)
        case toolRun([ChatItem])

        var id: String {
            switch self {
            case .message(let item): return "m-\(item.id)"
            case .toolRun(let items): return "t-\(items.first?.id.uuidString ?? "")"
            }
        }
    }

    /// Collapse maximal runs of consecutive `.toolActivity` items into `.toolRun`
    /// segments; everything else stays a standalone `.message`.
    private var segments: [TranscriptSegment] {
        var out: [TranscriptSegment] = []
        var run: [ChatItem] = []
        func flush() {
            if !run.isEmpty {
                out.append(.toolRun(run))
                run.removeAll()
            }
        }
        for item in viewModel.transcript {
            if case .toolActivity = item.kind {
                run.append(item)
            } else {
                flush()
                out.append(.message(item))
            }
        }
        flush()
        return out
    }

    var body: some View {
        ScrollViewReader { proxy in
            ScrollView {
                LazyVStack(alignment: .leading, spacing: 14) {
                    ForEach(segments) { segment in
                        switch segment {
                        case .message(let item):
                            ChatMessageRow(item: item, dataSource: appState.dataSource)
                                .id(item.id)
                        case .toolRun(let items):
                            ToolActivityGroup(items: items)
                                .id(segment.id)
                        }
                    }

                    if let pending = viewModel.pendingApproval {
                        ApprovalCard(
                            pending: pending,
                            onApprove: {
                                if let ds = appState.dataSource {
                                    viewModel.approvePending(dataSource: ds)
                                }
                            },
                            onReject: { viewModel.rejectPending() }
                        )
                        .id("approval-\(pending.id)")
                    }

                    if viewModel.isStreaming {
                        HStack(spacing: 6) {
                            ProgressView().scaleEffect(0.6)
                            Text("Thinking…")
                                .font(.caption)
                                .foregroundStyle(.secondary)
                        }
                        .id("streaming-indicator")
                    }

                    if let error = viewModel.errorMessage {
                        Label(error, systemImage: "exclamationmark.triangle")
                            .font(.caption)
                            .foregroundStyle(.red)
                    }

                    Color.clear.frame(height: 1).id("bottom-anchor")
                }
                .padding()
            }
            .defaultScrollAnchor(.bottom)
            .onChange(of: scrollSignal) { _, _ in
                withAnimation(.easeOut(duration: 0.15)) {
                    proxy.scrollTo("bottom-anchor", anchor: .bottom)
                }
            }
        }
    }
}

/// An in-chat approval prompt for a destructive assistant action. The composer stays
/// live below, so the user can approve, cancel, or type revised instructions instead.
private struct ApprovalCard: View {
    let pending: AssistantViewModel.PendingApproval
    let onApprove: () -> Void
    let onReject: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 8) {
            HStack(spacing: 8) {
                Image(systemName: "exclamationmark.triangle.fill")
                    .foregroundStyle(.orange)
                Text(pending.title)
                    .font(.callout.weight(.semibold))
            }
            Text(pending.detail)
                .font(.caption)
                .foregroundStyle(.secondary)
            HStack {
                Spacer()
                Button("Cancel", role: .cancel, action: onReject)
                Button(pending.confirmLabel, role: .destructive, action: onApprove)
                    .keyboardShortcut(.defaultAction)
            }
        }
        .padding(12)
        .frame(maxWidth: .infinity, alignment: .leading)
        .background(RoundedRectangle(cornerRadius: 10).fill(Color.orange.opacity(0.10)))
        .overlay(
            RoundedRectangle(cornerRadius: 10)
                .strokeBorder(Color.orange.opacity(0.35), lineWidth: 1)
        )
    }
}
