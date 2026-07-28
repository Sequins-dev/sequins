import SwiftUI
import SequinsData

/// An inline visualization in the chat transcript: a titled card wrapping the shared
/// ``VisualizationView``, with a type picker and an "Add to dashboard" action.
struct InlineVisualizationCard: View {
    let visualization: SavedVisualization
    let dataSource: DataSource?

    @State private var vizOverride: VizType?
    @State private var showingAdd = false

    /// The visualization with the current override folded into `shape`, so saving to a
    /// dashboard keeps the user's chosen type.
    private var effectiveVisualization: SavedVisualization {
        var v = visualization
        if let vizOverride { v.shape = vizOverride.rawValue }
        return v
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 8) {
                Image(systemName: "chart.bar.xaxis")
                    .foregroundStyle(.secondary)
                Text(visualization.title)
                    .font(.subheadline.weight(.semibold))
                    .lineLimit(1)
                Spacer()
                VizTypePicker(selection: $vizOverride)
                Button {
                    showingAdd = true
                } label: {
                    Image(systemName: "plus.rectangle.on.rectangle")
                }
                .buttonStyle(.borderless)
                .help("Add to dashboard")
            }

            VisualizationView(
                visualization: visualization,
                dataSource: dataSource,
                isLive: false,
                vizTypeOverride: vizOverride
            )
            .frame(height: 260)
            .background(Color(nsColor: .textBackgroundColor))
            .clipShape(RoundedRectangle(cornerRadius: 8))
        }
        .padding(10)
        .background(Color(nsColor: .controlBackgroundColor))
        .clipShape(RoundedRectangle(cornerRadius: 12))
        .overlay(
            RoundedRectangle(cornerRadius: 12)
                .stroke(Color.secondary.opacity(0.15), lineWidth: 1)
        )
        .sheet(isPresented: $showingAdd) {
            AddToDashboardSheet(visualization: effectiveVisualization, dataSource: dataSource)
        }
    }
}
