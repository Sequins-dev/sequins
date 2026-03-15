import SwiftUI

/// Indicator showing current zoom state with reset option
struct ZoomIndicatorView: View {
    let frameName: String
    let onReset: () -> Void

    var body: some View {
        VStack(alignment: .trailing, spacing: 4) {
            HStack(spacing: 4) {
                Image(systemName: "magnifyingglass")
                    .font(.caption)
                Text("Zoomed to:")
                    .font(.caption)
            }
            .foregroundColor(.secondary)

            Text(frameName)
                .font(.caption)
                .fontWeight(.medium)
                .lineLimit(1)
                .truncationMode(.middle)
                .frame(maxWidth: 200)

            Button("Reset Zoom", action: onReset)
                .buttonStyle(.plain)
                .font(.caption)
                .foregroundColor(.accentColor)
        }
        .padding(8)
        .background(Color(NSColor.controlBackgroundColor).opacity(0.95))
        .cornerRadius(6)
        .overlay(
            RoundedRectangle(cornerRadius: 6)
                .stroke(Color.secondary.opacity(0.2), lineWidth: 1)
        )
    }
}

/// Empty state for when no profile data is available
struct ProfileEmptyState: View {
    let isLoading: Bool

    var body: some View {
        VStack(spacing: 20) {
            Spacer()

            if isLoading {
                ProgressView("Loading profile...")
            } else {
                Image(systemName: "flame")
                    .font(.system(size: 48))
                    .foregroundColor(.secondary)

                Text("No Profile Data")
                    .font(.title2)
                    .foregroundColor(.secondary)

                Text("Profile data will appear here when your application sends profiling data.")
                    .font(.body)
                    .foregroundStyle(.tertiary)
                    .multilineTextAlignment(.center)
                    .frame(maxWidth: 300)
            }

            Spacer()
        }
    }
}

#Preview("ZoomIndicatorView") {
    ZoomIndicatorView(
        frameName: "processHTTPRequest",
        onReset: { }
    )
    .padding()
    .background(Color(NSColor.textBackgroundColor))
}

#Preview("ZoomIndicatorView - Long Name") {
    ZoomIndicatorView(
        frameName: "veryLongFunctionNameThatShouldBeTruncatedInTheMiddle",
        onReset: { }
    )
    .padding()
    .background(Color(NSColor.textBackgroundColor))
}

#Preview("ProfileEmptyState - Loading") {
    ProfileEmptyState(isLoading: true)
        .frame(width: 400, height: 300)
}

#Preview("ProfileEmptyState - No Data") {
    ProfileEmptyState(isLoading: false)
        .frame(width: 400, height: 300)
}
