import XCTest
@testable import SequinsData

final class DashboardVizTests: XCTestCase {

    func testDashboardJSONRoundTrip() throws {
        let dashboard = Dashboard(
            id: "d1",
            title: "Errors",
            createdAtNs: 1,
            updatedAtNs: 2,
            rows: [
                DashboardRow(height: 300, panels: [
                    RowPanel(visualization: SavedVisualization(seql: "logs last 1h", title: "Logs", shape: "bar"), weight: 2),
                    RowPanel(visualization: SavedVisualization(seql: "spans last 1h", title: "Spans"), weight: 1),
                ])
            ]
        )

        let encoder = JSONEncoder()
        encoder.keyEncodingStrategy = .convertToSnakeCase
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase

        let data = try encoder.encode(dashboard)
        let json = String(decoding: data, as: UTF8.self)
        XCTAssertTrue(json.contains("created_at_ns"))
        XCTAssertTrue(json.contains("\"rows\""))
        XCTAssertFalse(json.contains("\"x\""), "no legacy grid coordinates")

        let back = try decoder.decode(Dashboard.self, from: data)
        XCTAssertEqual(back.id, "d1")
        XCTAssertEqual(back.rows.count, 1)
        XCTAssertEqual(back.rows.first?.height, 300)
        XCTAssertEqual(back.rows.first?.panels.count, 2)
        XCTAssertEqual(back.rows.first?.panels.first?.weight, 2)
        XCTAssertEqual(back.rows.first?.panels.first?.visualization.vizType, .bar)
        XCTAssertEqual(back.panelCount, 2)
    }

    func testDecodesLegacyRowsAbsentDefaultsToEmpty() throws {
        // A dashboard JSON without `rows` (e.g. a freshly created empty dashboard) decodes.
        let json = #"{"id":"d2","title":"Empty","created_at_ns":0,"updated_at_ns":0}"#
        let decoder = JSONDecoder()
        decoder.keyDecodingStrategy = .convertFromSnakeCase
        let back = try decoder.decode(Dashboard.self, from: Data(json.utf8))
        XCTAssertEqual(back.rows.count, 0)
        XCTAssertEqual(back.panelCount, 0)
    }

    func testSavedVisualizationVizTypeMapping() {
        var v = SavedVisualization(seql: "x", title: "t", shape: nil)
        XCTAssertNil(v.vizType)

        v.vizType = .stackedBar
        XCTAssertEqual(v.shape, "stackedBar")
        XCTAssertEqual(v.vizType, .stackedBar)

        // Legacy ResponseShape strings map onto the VizType superset.
        XCTAssertEqual(SavedVisualization(seql: "x", title: "t", shape: "timeseries").vizType, .line)
        XCTAssertEqual(SavedVisualization(seql: "x", title: "t", shape: "scalar").vizType, .stat)
        XCTAssertEqual(SavedVisualization(seql: "x", title: "t", shape: "trace_tree").vizType, .trace)
    }

    func testVizTypeAutoSelect() {
        XCTAssertEqual(VizType.autoSelect(shape: .timeSeries, columns: ["t", "v"], rows: []), .line)
        XCTAssertEqual(VizType.autoSelect(shape: .scalar, columns: ["c"], rows: [[1]]), .stat)
        XCTAssertEqual(VizType.autoSelect(shape: .heatmap, columns: ["x", "y", "v"], rows: []), .heatmap)
        XCTAssertEqual(VizType.autoSelect(shape: .traceTimeline, columns: [], rows: []), .trace)

        // A compact two-column category+value table auto-selects a bar chart…
        XCTAssertEqual(
            VizType.autoSelect(shape: .table, columns: ["svc", "count"], rows: [["a", 1], ["b", 2]]),
            .bar
        )
        // …but a wider table stays a table.
        XCTAssertEqual(
            VizType.autoSelect(shape: .table, columns: ["a", "b", "c", "d"], rows: [[1, 2, 3, 4]]),
            .table
        )
    }
}
