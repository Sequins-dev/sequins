import SwiftUI
import SequinsData

/// Large centered single-value (stat) display: an aliased, title-cased label; the value
/// formatted with thousands separators / units (durations ns→ms/s); and an optional
/// delta versus the previous period.
struct ExploreScalarView: View {
    let columnName: String
    let rows: [[Any?]]
    var columnType: NodeTypeLabel = .number
    /// Value for the same query one window earlier (for the delta), if available.
    var previousValue: Double?

    private var currentValue: Double? {
        VizFormat.firstNumeric(inFirstRowOf: rows)
    }

    private var display: String {
        guard let first = rows.first, let cell = first.first else { return "—" }
        if let v = VizFormat.numeric(cell) {
            switch columnType {
            case .duration: return VizFormat.durationNs(v)
            default: return VizFormat.number(v)
            }
        }
        return VizFormat.string(cell)
    }

    private var delta: (text: String, up: Bool)? {
        guard let cur = currentValue, let prev = previousValue, prev != 0 else { return nil }
        let pct = (cur - prev) / abs(prev) * 100
        let arrow = pct >= 0 ? "▲" : "▼"
        return ("\(arrow) \(VizFormat.percent(abs(pct)))", pct >= 0)
    }

    var body: some View {
        VStack(spacing: 10) {
            Text(VizFormat.label(columnName))
                .font(.caption)
                .foregroundStyle(.secondary)
                .textCase(.uppercase)

            Text(display)
                .font(.system(size: 52, weight: .bold, design: .rounded))
                .foregroundStyle(.primary)
                .minimumScaleFactor(0.3)
                .lineLimit(1)

            if let delta {
                Text("\(delta.text) vs prev")
                    .font(.caption.weight(.medium))
                    .foregroundStyle(delta.up ? Color.green : Color.red)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .padding()
    }
}
