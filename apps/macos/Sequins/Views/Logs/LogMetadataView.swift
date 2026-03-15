import SwiftUI

/// Expandable metadata view for log entries
struct LogMetadataView: View {
    let metadata: [String: Any]

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            ForEach(sortedKeys, id: \.self) { key in
                HStack(alignment: .top) {
                    Text("\(key):")
                        .foregroundColor(.gray)
                        .frame(minWidth: 100, alignment: .trailing)

                    Text(formatValue(metadata[key]))
                        .foregroundColor(colorForValue(metadata[key]))
                        .textSelection(.enabled)

                    Spacer()
                }
            }
        }
        .font(.system(.caption, design: .monospaced))
    }

    private var sortedKeys: [String] {
        metadata.keys.sorted()
    }

    private func formatValue(_ value: Any?) -> String {
        guard let value = value else { return "null" }

        if let string = value as? String {
            return string
        } else if let number = value as? NSNumber {
            return number.stringValue
        } else if let array = value as? [Any] {
            return "[\(array.count) items]"
        } else if let dict = value as? [String: Any] {
            if let jsonData = try? JSONSerialization.data(withJSONObject: dict, options: [.prettyPrinted]),
               let jsonString = String(data: jsonData, encoding: .utf8) {
                return jsonString
            }
            return "{\(dict.count) fields}"
        }

        return String(describing: value)
    }

    private func colorForValue(_ value: Any?) -> Color {
        guard let value = value else { return .gray }

        if value is String {
            return .green
        } else if value is NSNumber {
            return .teal
        } else if value is [Any] || value is [String: Any] {
            return .purple
        }

        return .primary
    }
}

#Preview("LogMetadataView - Simple") {
    LogMetadataView(metadata: [
        "host": "localhost",
        "port": 3000,
        "status": "running"
    ])
    .padding()
    .background(Color.black)
}

#Preview("LogMetadataView - Complex") {
    LogMetadataView(metadata: [
        "error_code": "ECONNREFUSED",
        "attempts": 3,
        "last_error": "Connection timed out",
        "config": ["timeout": 5000, "retry": true]
    ])
    .padding()
    .background(Color.black)
}
