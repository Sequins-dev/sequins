import SwiftUI
import SequinsData

/// Individual row in the call stack display
struct StackFrameRow: View {
    let stackNode: FlamegraphNode
    let isCurrentFrame: Bool

    var body: some View {
        VStack(alignment: .leading, spacing: 1) {
            HStack(spacing: 4) {
                Text(String(repeating: "  ", count: stackNode.depth))
                    .font(.system(.caption, design: .monospaced))

                Image(systemName: isCurrentFrame ? "arrow.right.circle.fill" : "arrow.turn.down.right")
                    .font(.caption2)
                    .foregroundColor(isCurrentFrame ? .accentColor : .secondary)

                Text(stackNode.functionName)
                    .font(.caption)
                    .foregroundColor(isCurrentFrame ? .primary : .secondary)

                Spacer()
            }

            if let filename = stackNode.filename {
                HStack(spacing: 4) {
                    Text(String(repeating: "  ", count: stackNode.depth + 1))
                        .font(.system(.caption, design: .monospaced))

                    let lineStr = stackNode.line.map { ":\($0)" } ?? ""
                    Text("\(filename)\(lineStr)")
                        .font(.caption2)
                        .foregroundColor(.blue)

                    Spacer()
                }
            }
        }
    }
}

#Preview("StackFrameRow - Current") {
    let mockNode = FlamegraphNode(
        id: "123456",
        frameId: 123456,
        functionName: "processRequest",
        systemName: nil,
        filename: "server.rs",
        line: 42,
        depth: 2,
        selfValue: 500_000_000,
        totalValue: 1_200_000_000,
        parentId: nil,
        childIds: [],
        selfPercentage: 15.5,
        totalPercentage: 37.2
    )

    StackFrameRow(stackNode: mockNode, isCurrentFrame: true)
        .padding()
}

#Preview("StackFrameRow - Not Current") {
    let mockNode = FlamegraphNode(
        id: "789",
        frameId: 789,
        functionName: "handleHTTP",
        systemName: nil,
        filename: "http.rs",
        line: 128,
        depth: 1,
        selfValue: 100_000_000,
        totalValue: 800_000_000,
        parentId: nil,
        childIds: [],
        selfPercentage: 3.1,
        totalPercentage: 24.8
    )

    StackFrameRow(stackNode: mockNode, isCurrentFrame: false)
        .padding()
}
