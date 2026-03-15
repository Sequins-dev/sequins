import SwiftUI

/// Card showing summary metrics for a worker thread
struct ThreadSummaryCard: View {
    let threadName: String
    let cpuUsage: Double
    let memoryUsage: Double
    let eventLoopDelay: Double

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            Text(threadName.replacingOccurrences(of: "_", with: " ").capitalized)
                .font(.caption)
                .fontWeight(.medium)

            VStack(alignment: .leading, spacing: 2) {
                HStack {
                    Text("CPU:")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                    Spacer()
                    Text("\(Int(cpuUsage))%")
                        .font(.caption2)
                        .fontWeight(.medium)
                }

                HStack {
                    Text("Mem:")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                    Spacer()
                    Text("\(Int(memoryUsage))MB")
                        .font(.caption2)
                        .fontWeight(.medium)
                }

                HStack {
                    Text("Loop:")
                        .font(.caption2)
                        .foregroundColor(.secondary)
                    Spacer()
                    Text("\(Int(eventLoopDelay))ms")
                        .font(.caption2)
                        .fontWeight(.medium)
                }
            }
        }
        .padding(8)
        .background(Color(NSColor.controlBackgroundColor))
        .cornerRadius(6)
    }
}

#Preview("ThreadSummaryCard - Main") {
    ThreadSummaryCard(
        threadName: "main",
        cpuUsage: 34.0,
        memoryUsage: 128.0,
        eventLoopDelay: 8.0
    )
    .frame(width: 150)
    .padding()
}

#Preview("ThreadSummaryCard - Worker") {
    ThreadSummaryCard(
        threadName: "worker_1",
        cpuUsage: 67.0,
        memoryUsage: 89.0,
        eventLoopDelay: 25.0
    )
    .frame(width: 150)
    .padding()
}

#Preview("ThreadSummaryCard - Grid") {
    LazyVGrid(columns: Array(repeating: GridItem(.flexible(), spacing: 8), count: 4), spacing: 8) {
        ThreadSummaryCard(threadName: "main", cpuUsage: 34, memoryUsage: 128, eventLoopDelay: 8)
        ThreadSummaryCard(threadName: "worker_1", cpuUsage: 67, memoryUsage: 89, eventLoopDelay: 25)
        ThreadSummaryCard(threadName: "worker_2", cpuUsage: 23, memoryUsage: 156, eventLoopDelay: 4)
        ThreadSummaryCard(threadName: "worker_3", cpuUsage: 45, memoryUsage: 203, eventLoopDelay: 15)
    }
    .padding()
}
