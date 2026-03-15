import SwiftUI
import AppKit
import SequinsData

/// Log entry model for the UI layer
struct LogEntry: Identifiable {
    let id: String
    let timestamp: Date
    let severity: LogSeverity
    let message: String
    let component: String?
    let metadata: [String: Any]
    let traceId: String?
    let spanId: String?

    var isJSON: Bool {
        !metadata.isEmpty
    }

    /// Initialize from SequinsData.LogEntry
    init(from dataLogEntry: SequinsData.LogEntry) {
        self.id = dataLogEntry.id
        self.timestamp = dataLogEntry.timestamp
        self.severity = LogSeverity(from: dataLogEntry.severity)
        self.message = dataLogEntry.body
        self.component = dataLogEntry.serviceName
        self.metadata = LogEntry.convertAttributes(dataLogEntry.attributes)
        self.traceId = dataLogEntry.traceId
        self.spanId = dataLogEntry.spanId
    }

    /// Initialize with raw values (for mock data)
    init(
        id: String,
        timestamp: Date,
        severity: LogSeverity,
        message: String,
        component: String?,
        metadata: [String: Any],
        traceId: String? = nil,
        spanId: String? = nil
    ) {
        self.id = id
        self.timestamp = timestamp
        self.severity = severity
        self.message = message
        self.component = component
        self.metadata = metadata
        self.traceId = traceId
        self.spanId = spanId
    }

    /// Legacy initializer for backward compatibility with mock data
    init(
        id: Int,
        timestamp: Date,
        severity: LogSeverity,
        message: String,
        component: String?,
        metadata: [String: Any]
    ) {
        self.id = String(id)
        self.timestamp = timestamp
        self.severity = severity
        self.message = message
        self.component = component
        self.metadata = metadata
        self.traceId = nil
        self.spanId = nil
    }

    private static func convertAttributes(_ attributes: [String: SequinsData.AttributeValue]) -> [String: Any] {
        var result: [String: Any] = [:]
        for (key, value) in attributes {
            switch value {
            case .string(let s):
                result[key] = s
            case .bool(let b):
                result[key] = b
            case .int(let i):
                result[key] = i
            case .double(let d):
                result[key] = d
            case .stringArray(let arr):
                result[key] = arr
            case .boolArray(let arr):
                result[key] = arr
            case .intArray(let arr):
                result[key] = arr
            case .doubleArray(let arr):
                result[key] = arr
            }
        }
        return result
    }

    /// Convert to JSON-serializable dictionary
    func toJSONDictionary() -> [String: Any] {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]

        var dict: [String: Any] = [
            "id": id,
            "timestamp": formatter.string(from: timestamp),
            "severity": severity.rawValue,
            "message": message
        ]

        if let component = component {
            dict["component"] = component
        }

        if !metadata.isEmpty {
            dict["metadata"] = metadata
        }

        if let traceId = traceId {
            dict["traceId"] = traceId
        }

        if let spanId = spanId {
            dict["spanId"] = spanId
        }

        return dict
    }

    /// Export logs to JSON and save to file
    static func exportToJSON(_ logs: [LogEntry]) {
        let panel = NSSavePanel()
        panel.title = "Export Logs"
        panel.nameFieldStringValue = "logs-export.json"
        panel.allowedContentTypes = [.json]
        panel.canCreateDirectories = true

        guard panel.runModal() == .OK, let url = panel.url else { return }

        let jsonArray = logs.map { $0.toJSONDictionary() }

        do {
            let data = try JSONSerialization.data(withJSONObject: jsonArray, options: [.prettyPrinted, .sortedKeys])
            try data.write(to: url)
            NSLog("📋 Exported \(logs.count) logs to \(url.path)")
        } catch {
            NSLog("📋 Failed to export logs: \(error)")
        }
    }
}
