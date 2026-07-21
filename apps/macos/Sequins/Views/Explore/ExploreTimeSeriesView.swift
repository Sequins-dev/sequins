import SwiftUI
import SequinsData

/// Time-series line chart for SeQL results (first column = time/x). Delegates to the
/// shared `TimeSeriesChart` (temporal axis, hover, formatted y, alias legend).
struct ExploreTimeSeriesView: View {
    let columns: [String]
    let rows: [[Any?]]
    var columnTypes: [NodeTypeLabel] = []
    var options: VisualizationOptions = VisualizationOptions()

    var body: some View {
        TimeSeriesChart(columns: columns, rows: rows, columnTypes: columnTypes, options: options)
            .padding()
    }
}
