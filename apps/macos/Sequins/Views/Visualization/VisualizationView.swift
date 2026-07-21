import SwiftUI
import SequinsData

/// The shared visualization component: given a saved visualization descriptor
/// (`{ seql, title, vizType? }`), a data source, a time range, and a live flag, it
/// runs the query and renders the chart/table best suited to the data. Used inline in
/// the Assistant chat and as dashboard panels.
struct VisualizationView: View {
    let visualization: SavedVisualization
    let dataSource: DataSource?
    var timeRange: TimeRange = .relative(duration: 3600)
    var isLive: Bool = false
    /// Whether to rewrite the query's time-scope with `timeRange` (dashboards) or run it
    /// verbatim (chat, where the assistant chose the window).
    var applyTimeRange: Bool = false
    /// Optional user/model override; falls back to the stored type, then auto-select.
    var vizTypeOverride: VizType? = nil

    @State private var runner = VisualizationRunner()
    /// Runs the same query one window earlier, to compute a stat delta.
    @State private var prevRunner = VisualizationRunner()

    /// The type actually rendered: explicit override → stored `shape` → auto-selected.
    var effectiveVizType: VizType {
        vizTypeOverride
            ?? visualization.vizType
            ?? VizType.autoSelect(
                shape: runner.shape,
                columns: runner.columns,
                rows: runner.rows,
                roles: runner.columnRoles
            )
    }

    /// The prior-period scalar value (for a stat's delta), when loaded.
    private var previousStatValue: Double? {
        VizFormat.firstNumeric(inFirstRowOf: prevRunner.rows)
    }

    /// The query to execute — time-scope rewritten when `applyTimeRange` is set.
    private var effectiveQuery: String {
        applyTimeRange ? timeRange.applied(to: visualization.seql) : visualization.seql
    }

    /// Restart key — re-runs the query when the query, live flag, or time window change.
    ///
    /// Must be STABLE across re-renders: a relative range keys on its duration (not the
    /// live `bounds`, which advance every render and would restart the query on every
    /// frame — freezing the view), an absolute range on its fixed endpoints.
    private var reloadKey: String {
        let rangeKey: String
        switch timeRange {
        case .relative(let duration):
            rangeKey = "r\(duration)"
        case .absolute(let start, let end):
            rangeKey = "a\(start.timeIntervalSince1970)-\(end.timeIntervalSince1970)"
        }
        return "\(effectiveQuery)|\(isLive)|\(rangeKey)"
    }

    var body: some View {
        Group {
            if let error = runner.errorMessage {
                VizMessage(icon: "exclamationmark.triangle", text: error)
            } else if runner.isLoading && runner.rows.isEmpty {
                ProgressView()
                    .frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if runner.rows.isEmpty && runner.schema != nil {
                VizMessage(icon: "tray", text: "No data")
            } else {
                VizRenderer(
                    vizType: effectiveVizType,
                    columns: runner.columns,
                    rows: runner.rows,
                    columnTypes: runner.columnTypes,
                    columnRoles: runner.columnRoles,
                    previousValue: previousStatValue
                )
            }
        }
        .task(id: reloadKey) {
            guard let dataSource else { return }
            runner.start(dataSource: dataSource, query: effectiveQuery, isLive: isLive)
        }
        .task(id: "\(reloadKey)|stat=\(effectiveVizType == .stat)") {
            // For a stat, run the same query one window earlier for the delta.
            prevRunner.stop()
            guard effectiveVizType == .stat, applyTimeRange, let dataSource else { return }
            let bounds = timeRange.bounds
            let duration = bounds.end.timeIntervalSince(bounds.start)
            guard duration > 0 else { return }
            let prior = TimeRange.absolute(
                start: bounds.start.addingTimeInterval(-duration),
                end: bounds.start
            )
            prevRunner.start(dataSource: dataSource, query: prior.applied(to: visualization.seql), isLive: false)
        }
        .onDisappear {
            runner.stop()
            prevRunner.stop()
        }
    }
}

/// Switches to the concrete chart/table view for a `VizType`. Reuses the Explore leaf
/// views for line/stat/table/trace, and the app-side chart views for the rest.
struct VizRenderer: View {
    let vizType: VizType
    let columns: [String]
    let rows: [[Any?]]
    var columnTypes: [NodeTypeLabel] = []
    var columnRoles: [SeQLColumnRole] = []
    var previousValue: Double?

    /// A chart type that plots measures but has none to plot degrades to a table, so a
    /// result with rows always shows something rather than an empty chart.
    private var effectiveType: VizType {
        if vizType.plotsMeasures, !rows.isEmpty,
           VizFormat.valueColumns(columns: columns, rows: rows, roles: columnRoles).isEmpty {
            return .table
        }
        return vizType
    }

    var body: some View {
        switch effectiveType {
        case .line:
            ExploreTimeSeriesView(columns: columns, rows: rows, columnTypes: columnTypes)
        case .area:
            AreaChartView(columns: columns, rows: rows, columnTypes: columnTypes, columnRoles: columnRoles)
        case .bar:
            BarChartView(columns: columns, rows: rows, stacked: false, columnTypes: columnTypes, columnRoles: columnRoles)
        case .stackedBar:
            BarChartView(columns: columns, rows: rows, stacked: true, columnTypes: columnTypes, columnRoles: columnRoles)
        case .pie:
            PieChartView(columns: columns, rows: rows, columnTypes: columnTypes, columnRoles: columnRoles)
        case .gauge:
            GaugeChartView(columns: columns, rows: rows, columnTypes: columnTypes)
        case .stat:
            ExploreScalarView(
                columnName: columns.first ?? "value",
                rows: rows,
                columnType: columnTypes.first ?? .number,
                previousValue: previousValue
            )
        case .table:
            DataTableView(columns: columns, rows: rows, columnTypes: columnTypes)
        case .heatmap:
            HeatmapChartView(columns: columns, rows: rows, columnTypes: columnTypes)
        case .trace:
            TraceVizView(columns: columns, rows: rows)
        }
    }
}

/// A centered icon + message placeholder (empty/error states).
struct VizMessage: View {
    let icon: String
    let text: String

    var body: some View {
        VStack(spacing: 8) {
            Image(systemName: icon)
                .font(.title2)
                .foregroundStyle(.secondary)
            Text(text)
                .font(.caption)
                .foregroundStyle(.secondary)
                .multilineTextAlignment(.center)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity)
        .padding()
    }
}
