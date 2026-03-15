import SwiftUI

/// Statistics header for the trace list panel
struct TraceListStatsView: View {
    let traceCount: Int
    let errorCount: Int
    let avgDuration: TimeInterval

    var body: some View {
        HStack {
            VStack(alignment: .leading, spacing: 2) {
                Text("Traces")
                    .font(.caption)
                    .foregroundColor(.secondary)
                Text("\(traceCount)")
                    .font(.title2.monospacedDigit())
            }

            Spacer()

            VStack(alignment: .trailing, spacing: 2) {
                Text("Errors")
                    .font(.caption)
                    .foregroundColor(.secondary)
                Text("\(errorCount)")
                    .font(.title2.monospacedDigit())
                    .foregroundColor(errorCount > 0 ? .red : .primary)
            }

            Spacer()

            VStack(alignment: .trailing, spacing: 2) {
                Text("Avg Duration")
                    .font(.caption)
                    .foregroundColor(.secondary)
                Text(formatDuration(avgDuration))
                    .font(.title2.monospacedDigit())
            }
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 8)
        .background(Color(NSColor.windowBackgroundColor))
        .overlay(
            Rectangle()
                .frame(height: 1)
                .foregroundColor(Color(NSColor.separatorColor)),
            alignment: .bottom
        )
    }

    private func formatDuration(_ duration: TimeInterval) -> String {
        if duration < 0.001 {
            return String(format: "%.0fµs", duration * 1_000_000)
        } else if duration < 1 {
            return String(format: "%.1fms", duration * 1000)
        } else {
            return String(format: "%.2fs", duration)
        }
    }
}

#Preview("TraceListStatsView - Normal") {
    TraceListStatsView(
        traceCount: 42,
        errorCount: 3,
        avgDuration: 0.234
    )
    .frame(width: 300)
}

#Preview("TraceListStatsView - No Errors") {
    TraceListStatsView(
        traceCount: 128,
        errorCount: 0,
        avgDuration: 0.089
    )
    .frame(width: 300)
}

#Preview("TraceListStatsView - Fast") {
    TraceListStatsView(
        traceCount: 1024,
        errorCount: 12,
        avgDuration: 0.000456
    )
    .frame(width: 300)
}
