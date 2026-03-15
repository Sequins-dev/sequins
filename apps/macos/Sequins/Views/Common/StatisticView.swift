import SwiftUI

/// A reusable statistics display component
struct StatisticView: View {
    let label: String
    let value: String
    let detail: String?

    init(label: String, value: String, detail: String? = nil) {
        self.label = label
        self.value = value
        self.detail = detail
    }

    var body: some View {
        VStack(alignment: .trailing, spacing: 2) {
            Text(value)
                .font(.headline)
                .monospacedDigit()

            if let detail = detail {
                Text(detail)
                    .font(.caption2)
                    .foregroundStyle(.secondary)
            }

            Text(label)
                .font(.caption2)
                .foregroundStyle(.tertiary)
        }
    }
}

#Preview("StatisticView - Basic") {
    HStack(spacing: 24) {
        StatisticView(label: "Spans", value: "1,234")
        StatisticView(label: "Errors", value: "12")
        StatisticView(label: "Duration", value: "234ms")
    }
    .padding()
}

#Preview("StatisticView - With Detail") {
    HStack(spacing: 24) {
        StatisticView(label: "CPU", value: "45%", detail: "avg")
        StatisticView(label: "Memory", value: "128MB", detail: "peak")
        StatisticView(label: "Last Seen", value: "2m ago", detail: nil)
    }
    .padding()
}
