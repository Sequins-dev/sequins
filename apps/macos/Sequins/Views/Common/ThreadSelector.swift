import SwiftUI

/// A reusable thread/worker selector dropdown menu
struct ThreadSelector: View {
    @Binding var selectedThread: String
    let availableThreads: [String]

    var body: some View {
        Menu {
            ForEach(availableThreads, id: \.self) { thread in
                Button(action: { selectedThread = thread }) {
                    HStack {
                        Text(displayName(for: thread))
                        if selectedThread == thread {
                            Image(systemName: "checkmark")
                        }
                    }
                }
            }
        } label: {
            HStack(spacing: 4) {
                Text(displayName(for: selectedThread))
                    .font(.caption)
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(Color(NSColor.controlBackgroundColor))
            .cornerRadius(4)
        }
        .menuStyle(.borderlessButton)
        .fixedSize()
    }

    private func displayName(for thread: String) -> String {
        if thread == "all" {
            return "All Threads"
        }
        return thread.replacingOccurrences(of: "_", with: " ").capitalized
    }
}

#Preview("ThreadSelector") {
    struct PreviewWrapper: View {
        @State private var selectedThread = "all"

        var body: some View {
            ThreadSelector(
                selectedThread: $selectedThread,
                availableThreads: ["all", "main", "worker_1", "worker_2", "worker_3"]
            )
            .padding()
        }
    }

    return PreviewWrapper()
}

#Preview("ThreadSelector - Worker Selected") {
    struct PreviewWrapper: View {
        @State private var selectedThread = "worker_1"

        var body: some View {
            ThreadSelector(
                selectedThread: $selectedThread,
                availableThreads: ["all", "main", "worker_1", "worker_2"]
            )
            .padding()
        }
    }

    return PreviewWrapper()
}
