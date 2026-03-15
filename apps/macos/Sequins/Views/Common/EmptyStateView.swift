import SwiftUI

/// A configurable empty state view for when no content is available
struct EmptyStateView: View {
    let icon: String
    let title: String
    let message: String?
    let actionTitle: String?
    let action: (() -> Void)?

    init(
        icon: String,
        title: String,
        message: String? = nil,
        actionTitle: String? = nil,
        action: (() -> Void)? = nil
    ) {
        self.icon = icon
        self.title = title
        self.message = message
        self.actionTitle = actionTitle
        self.action = action
    }

    var body: some View {
        VStack(spacing: 16) {
            Image(systemName: icon)
                .font(.system(size: 48))
                .foregroundColor(.secondary)

            Text(title)
                .font(.title2)
                .foregroundColor(.secondary)

            if let message = message {
                Text(message)
                    .font(.body)
                    .foregroundColor(.secondary)
                    .multilineTextAlignment(.center)
                    .frame(maxWidth: 300)
            }

            if let actionTitle = actionTitle, let action = action {
                Button(actionTitle, action: action)
                    .buttonStyle(.borderedProminent)
            }
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

// MARK: - Convenience initializers for common empty states

extension EmptyStateView {
    /// Empty state for no traces
    static var noTraces: EmptyStateView {
        EmptyStateView(
            icon: "square.3.layers.3d",
            title: "No traces found",
            message: "Try adjusting your filters or time range."
        )
    }

    /// Empty state for no logs
    static var noLogs: EmptyStateView {
        EmptyStateView(
            icon: "doc.text",
            title: "No logs found",
            message: "Try adjusting your filters or time range."
        )
    }

    /// Empty state for no metrics
    static var noMetrics: EmptyStateView {
        EmptyStateView(
            icon: "chart.line.uptrend.xyaxis",
            title: "No metrics available",
            message: "Metrics will appear once your application starts sending telemetry data."
        )
    }

    /// Empty state for no profiles
    static var noProfiles: EmptyStateView {
        EmptyStateView(
            icon: "flame",
            title: "No profiles available",
            message: "Profiles will appear once your application starts sending profiling data."
        )
    }

    /// Empty state for no selection
    static func noSelection(itemType: String) -> EmptyStateView {
        EmptyStateView(
            icon: "cursorarrow.click",
            title: "Select a \(itemType)",
            message: "Choose a \(itemType) from the list to view its details."
        )
    }
}

#Preview("EmptyStateView - Traces") {
    EmptyStateView.noTraces
}

#Preview("EmptyStateView - With Action") {
    EmptyStateView(
        icon: "wifi.slash",
        title: "Connection Failed",
        message: "Unable to connect to the telemetry server.",
        actionTitle: "Retry",
        action: { print("Retry tapped") }
    )
}

#Preview("EmptyStateView - No Selection") {
    EmptyStateView.noSelection(itemType: "trace")
}
