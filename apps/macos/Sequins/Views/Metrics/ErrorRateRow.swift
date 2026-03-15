import SwiftUI

/// Individual row in the error rates display
struct ErrorRateRow: View {
    let label: String
    let percentage: Double
    let color: Color

    var body: some View {
        HStack {
            Circle()
                .fill(color)
                .frame(width: 8, height: 8)
            Text(label)
                .font(.caption2)
                .foregroundColor(.secondary)
            Spacer()
            Text("\(percentage, specifier: "%.1f")%")
                .font(.caption2)
                .fontWeight(.medium)
        }
    }
}

#Preview("ErrorRateRow - Success") {
    ErrorRateRow(label: "2xx Success", percentage: 98.5, color: .green)
        .frame(width: 200)
        .padding()
}

#Preview("ErrorRateRow - All Types") {
    VStack(spacing: 4) {
        ErrorRateRow(label: "2xx Success", percentage: 98.5, color: .green)
        ErrorRateRow(label: "4xx Client", percentage: 1.2, color: .yellow)
        ErrorRateRow(label: "5xx Server", percentage: 0.3, color: .red)
    }
    .frame(width: 200)
    .padding()
}
