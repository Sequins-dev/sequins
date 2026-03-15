import SwiftUI

/// Large centered display for a single scalar value
struct ExploreScalarView: View {
    let columnName: String
    let rows: [[Any?]]

    private var value: String {
        guard let first = rows.first, let cell = first.first else { return "—" }
        guard let cell else { return "null" }
        switch cell {
        case let s as String: return s
        case let n as NSNumber: return n.stringValue
        case let b as Bool: return b ? "true" : "false"
        default: return String(describing: cell)
        }
    }

    var body: some View {
        VStack(spacing: 16) {
            Text(columnName)
                .font(.caption)
                .foregroundStyle(.secondary)
                .textCase(.uppercase)

            Text(value)
                .font(.system(size: 56, weight: .bold, design: .monospaced))
                .foregroundStyle(.primary)
                .minimumScaleFactor(0.3)
                .lineLimit(1)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}
