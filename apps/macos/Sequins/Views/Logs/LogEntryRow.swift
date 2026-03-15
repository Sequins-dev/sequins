import SwiftUI
import AppKit

/// Individual log entry row with expandable metadata
struct LogEntryRow: View {
    let log: LogEntry
    let isHovered: Bool
    var isDragSelected: Bool = false
    var isNew: Bool = false

    @State private var isExpanded = false
    @State private var flashOpacity: Double = 0

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            // Main log line - clicking anywhere toggles expansion
            HStack(alignment: .top, spacing: 8) {
                // Timestamp
                Text(formatTimestamp(log.timestamp))
                    .foregroundColor(.gray)
                    .font(.system(.caption, design: .monospaced))

                // Level indicator
                Text(log.severity.emoji)
                    .font(.system(.caption))

                // Level text
                Text(log.severity.rawValue)
                    .foregroundColor(log.severity.color)
                    .frame(width: 50, alignment: .leading)
                    .font(.system(.caption, design: .monospaced))

                // Component
                if let component = log.component {
                    Text(component)
                        .foregroundColor(.teal)
                        .font(.system(.caption, design: .monospaced))
                        .frame(minWidth: 80, alignment: .leading)
                }

                // Message
                Text(log.message)
                    .foregroundColor(.primary)
                    .font(.system(.body, design: .monospaced))
                    .lineLimit(isExpanded ? nil : 1)

                Spacer()

                // Expand indicator for logs with metadata
                if log.isJSON {
                    Image(systemName: isExpanded ? "chevron.up" : "chevron.down")
                        .foregroundColor(.gray)
                        .font(.caption)
                }
            }
            .padding(.horizontal, 12)
            .padding(.vertical, 4)
            .background(backgroundColor)
            .contentShape(Rectangle())
            .onTapGesture {
                if log.isJSON {
                    isExpanded.toggle()
                }
            }
            .contextMenu {
                Button {
                    NSPasteboard.general.clearContents()
                    NSPasteboard.general.setString(log.message, forType: .string)
                } label: {
                    Label("Copy Message", systemImage: "doc.on.doc")
                }

                Button {
                    NSPasteboard.general.clearContents()
                    NSPasteboard.general.setString(logAsJSON(), forType: .string)
                } label: {
                    Label("Copy as JSON", systemImage: "curlybraces")
                }
            }

            // Expanded metadata view (clicking here does NOT collapse)
            if isExpanded && !log.metadata.isEmpty {
                LogMetadataView(metadata: log.metadata)
                    .padding(.leading, 40)
                    .padding(.vertical, 8)
                    .background(Color.secondary.opacity(0.1))
            }
        }
        .overlay(
            Rectangle()
                .fill(Color.accentColor.opacity(flashOpacity))
                .allowsHitTesting(false)
        )
        .onAppear {
            if isNew {
                flashOpacity = 0.35
                withAnimation(.easeOut(duration: 0.25)) {
                    flashOpacity = 0
                }
            }
        }
    }

    private func logAsJSON() -> String {
        var dict: [String: Any] = [
            "timestamp": ISO8601DateFormatter().string(from: log.timestamp),
            "severity": log.severity.rawValue,
            "message": log.message
        ]
        if let component = log.component {
            dict["component"] = component
        }
        if !log.metadata.isEmpty {
            dict["metadata"] = log.metadata
        }

        if let jsonData = try? JSONSerialization.data(withJSONObject: dict, options: [.prettyPrinted, .sortedKeys]),
           let jsonString = String(data: jsonData, encoding: .utf8) {
            return jsonString
        }
        return "{}"
    }

    private var backgroundColor: Color {
        if isDragSelected {
            return Color.blue.opacity(0.3)
        } else if isExpanded {
            return Color.primary.opacity(0.1)
        } else if isHovered {
            return Color.primary.opacity(0.06)
        } else {
            return Color.clear
        }
    }

    private func formatTimestamp(_ date: Date) -> String {
        let formatter = DateFormatter()
        formatter.dateFormat = "HH:mm:ss.SSS"
        return formatter.string(from: date)
    }
}

#Preview("LogEntryRow - Info") {
    LogEntryRow(
        log: LogEntry(
            id: 1,
            timestamp: Date(),
            severity: .info,
            message: "Server started on port 3000",
            component: "HTTP",
            metadata: [:]
        ),
        isHovered: false
    )
    .background(Color.black)
}

#Preview("LogEntryRow - Error with Metadata") {
    LogEntryRow(
        log: LogEntry(
            id: 2,
            timestamp: Date(),
            severity: .error,
            message: "Connection failed",
            component: "Database",
            metadata: [
                "error_code": "ECONNREFUSED",
                "host": "localhost",
                "port": 5432
            ]
        ),
        isHovered: true
    )
    .background(Color.black)
}

#Preview("LogEntryRow - Hovered") {
    LogEntryRow(
        log: LogEntry(
            id: 3,
            timestamp: Date(),
            severity: .warn,
            message: "High memory usage detected",
            component: "Monitor",
            metadata: ["usage": "85%"]
        ),
        isHovered: true
    )
    .background(Color.black)
}

#Preview("LogEntryRow - Drag Selected") {
    LogEntryRow(
        log: LogEntry(
            id: 4,
            timestamp: Date(),
            severity: .debug,
            message: "Processing request batch",
            component: "Worker",
            metadata: [:]
        ),
        isHovered: false,
        isDragSelected: true
    )
    .background(Color.black)
}
