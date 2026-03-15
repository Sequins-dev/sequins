import SwiftUI
import SequinsData

/// Switches between result visualizations based on ResponseShape
struct ExploreResultView: View {
    let schema: SeQLSchema
    let rows: [[Any?]]
    let recordTrees: [RecordNode]
    var visualizationOverride: ResponseShape? = nil
    let pageSize: Int
    @Binding var currentPage: Int

    private var effectiveShape: ResponseShape {
        visualizationOverride ?? schema.shape
    }

    var body: some View {
        switch effectiveShape {
        case .timeSeries:
            ExploreTimeSeriesView(columns: schema.columnNames, rows: rows)
        case .scalar:
            ExploreScalarView(
                columnName: schema.columnNames.first ?? "value",
                rows: rows
            )
        case .traceTimeline, .traceTree:
            ExploreTraceTimelineView(columns: schema.columnNames, rows: rows)
        case .table:
            ExploreRecordTreeView(records: recordTrees)
        case .patternGroups, .heatmap:
            ExploreTableView(
                columns: schema.columnNames,
                rows: rows,
                pageSize: pageSize,
                currentPage: $currentPage
            )
        }
    }
}
