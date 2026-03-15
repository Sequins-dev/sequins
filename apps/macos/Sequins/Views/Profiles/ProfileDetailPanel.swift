import SwiftUI
import SequinsData

/// Panel showing detailed information about a selected flamegraph node
struct ProfileDetailPanel: View {
    let node: FlamegraphNode
    let stackTrace: [FlamegraphNode]
    let onClose: () -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            headerSection

            Divider()

            stackTraceSection
        }
        .padding()
        .background(Color(NSColor.controlBackgroundColor))
        .frame(height: 400)
    }

    private var headerSection: some View {
        HStack {
            VStack(alignment: .leading, spacing: 4) {
                Text(node.functionName)
                    .font(.headline)

                HStack(spacing: 16) {
                    Label(
                        "Self: \(formatValue(node.selfValue)) (\(String(format: "%.1f%%", node.selfPercentage)))",
                        systemImage: "clock"
                    )
                    Label(
                        "Total: \(formatValue(node.totalValue)) (\(String(format: "%.1f%%", node.totalPercentage)))",
                        systemImage: "clock.fill"
                    )
                }
                .font(.caption)
                .foregroundColor(.secondary)

                if let filename = node.filename {
                    let lineStr = node.line.map { ":\($0)" } ?? ""
                    Text("\(filename)\(lineStr)")
                        .font(.caption2)
                        .foregroundColor(.blue)
                }
            }

            Spacer()

            Button(action: onClose) {
                Image(systemName: "xmark.circle.fill")
                    .foregroundColor(.secondary)
            }
            .buttonStyle(.plain)
        }
    }

    private var stackTraceSection: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text("Call Stack:")
                .font(.caption)
                .foregroundColor(.secondary)

            ScrollView {
                VStack(alignment: .leading, spacing: 2) {
                    ForEach(stackTrace) { stackNode in
                        StackFrameRow(stackNode: stackNode, isCurrentFrame: stackNode.id == node.id)
                    }
                }
            }
            .frame(maxHeight: 300)
        }
    }

    private func formatValue(_ value: Int64) -> String {
        let seconds = Double(value) / 1_000_000_000
        return String(format: "%.2fs", seconds)
    }
}

#Preview("ProfileDetailPanel") {
    let mockNode = FlamegraphNode(
        id: "123456",
        frameId: 123456,
        functionName: "processRequest",
        systemName: nil,
        filename: "server.rs",
        line: 42,
        depth: 3,
        selfValue: 500_000_000,
        totalValue: 1_200_000_000,
        parentId: nil,
        childIds: [],
        selfPercentage: 15.5,
        totalPercentage: 37.2
    )

    ProfileDetailPanel(
        node: mockNode,
        stackTrace: [mockNode],
        onClose: {}
    )
    .frame(width: 600)
}
