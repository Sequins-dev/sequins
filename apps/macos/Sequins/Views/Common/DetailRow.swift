import SwiftUI
import AppKit

/// A reusable key-value display row for detail panels
struct DetailRow: View {
    let label: String
    let values: [String]
    var monospaced: Bool = false
    var copyable: Bool = false

    /// Convenience initializer for a single value
    init(label: String, value: String, monospaced: Bool = false, copyable: Bool = false) {
        self.label = label
        self.values = [value]
        self.monospaced = monospaced
        self.copyable = copyable
    }

    /// Full initializer for multiple values
    init(label: String, values: [String], monospaced: Bool = false, copyable: Bool = false) {
        self.label = label
        self.values = values
        self.monospaced = monospaced
        self.copyable = copyable
    }

    var body: some View {
        HStack(alignment: .top) {
            Text(label + ":")
                .font(.caption)
                .foregroundColor(.secondary)
                .frame(width: 80, alignment: .trailing)

            AttributeValuesView(values: values, monospaced: monospaced, copyable: copyable)

            Spacer()
        }
    }
}

#Preview("DetailRow - Basic") {
    VStack(alignment: .leading, spacing: 12) {
        DetailRow(label: "Service", value: "my-service")
        DetailRow(label: "Duration", value: "123.45 ms")
        DetailRow(label: "Trace ID", value: "abc123def456", monospaced: true)
        DetailRow(label: "Span ID", value: "span-789", monospaced: true)
    }
    .padding()
    .frame(width: 400)
}

#Preview("DetailRow - Multiple Values") {
    VStack(alignment: .leading, spacing: 12) {
        DetailRow(label: "Hosts", values: ["server-1", "server-2", "server-3"], monospaced: true)
        DetailRow(label: "Envs", values: ["production", "staging"])
    }
    .padding()
    .frame(width: 400)
}

#Preview("DetailRow - Many Values Wrap") {
    DetailRow(
        label: "PIDs",
        values: ["12345", "12346", "12347", "12348", "12349", "12350"],
        monospaced: true
    )
    .padding()
    .frame(width: 350)
}
