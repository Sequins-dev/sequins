import SwiftUI

/// A reusable status badge component for displaying status indicators
struct StatusBadge: View {
    let status: Status

    enum Status {
        case ok
        case error
        case warning
        case unknown

        var text: String {
            switch self {
            case .ok: return "OK"
            case .error: return "Error"
            case .warning: return "Warning"
            case .unknown: return "—"
            }
        }

        var backgroundColor: Color {
            switch self {
            case .ok: return .green
            case .error: return .red
            case .warning: return .yellow
            case .unknown: return .gray
            }
        }

        var textColor: Color {
            switch self {
            case .warning: return .black
            default: return .white
            }
        }
    }

    var body: some View {
        Text(status.text)
            .font(.caption)
            .padding(.horizontal, 6)
            .padding(.vertical, 2)
            .background(status.backgroundColor)
            .foregroundColor(status.textColor)
            .clipShape(Capsule())
    }
}

// MARK: - Convenience initializers for common status types

extension StatusBadge {
    /// Initialize from a boolean success state
    init(isSuccess: Bool) {
        self.status = isSuccess ? .ok : .error
    }
}

#Preview("StatusBadge - All States") {
    HStack(spacing: 12) {
        StatusBadge(status: .ok)
        StatusBadge(status: .error)
        StatusBadge(status: .warning)
        StatusBadge(status: .unknown)
    }
    .padding()
}

#Preview("StatusBadge - Boolean") {
    HStack(spacing: 12) {
        StatusBadge(isSuccess: true)
        StatusBadge(isSuccess: false)
    }
    .padding()
}
