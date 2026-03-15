import SwiftUI
import SequinsData

/// Color-coded pill badge showing metric type
struct MetricTypePill: View {
    let metricType: MetricType

    private var typeInfo: (label: String, color: Color) {
        switch metricType {
        case .gauge:
            return ("gauge", .blue)
        case .counter:
            return ("counter", .green)
        case .histogram:
            return ("histogram", .orange)
        case .summary:
            return ("summary", .purple)
        }
    }

    var body: some View {
        Text(typeInfo.label)
            .font(.caption2)
            .fontWeight(.medium)
            .foregroundColor(.white)
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .background(typeInfo.color)
            .cornerRadius(4)
    }
}

#Preview("MetricTypePill - All Types") {
    VStack(spacing: 8) {
        MetricTypePill(metricType: .gauge)
        MetricTypePill(metricType: .counter)
        MetricTypePill(metricType: .histogram)
        MetricTypePill(metricType: .summary)
    }
    .padding()
}
