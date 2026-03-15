import SwiftUI
import Charts

/// Time-series chart for SeQL results where first column is a time value
struct ExploreTimeSeriesView: View {
    let columns: [String]
    let rows: [[Any?]]

    private struct DataPoint: Identifiable {
        let id = UUID()
        let time: Double
        let value: Double
        let series: String
    }

    private var dataPoints: [DataPoint] {
        guard columns.count >= 2 else { return [] }
        var points: [DataPoint] = []
        let seriesColumns = columns.dropFirst()

        for row in rows {
            guard let timeCell = row.first,
                  let timeVal = numericValue(of: timeCell) else { continue }

            for (offset, col) in seriesColumns.enumerated() {
                let colIdx = offset + 1
                guard colIdx < row.count else { continue }
                if let numVal = numericValue(of: row[colIdx]) {
                    points.append(DataPoint(time: timeVal, value: numVal, series: col))
                }
            }
        }
        return points
    }

    var body: some View {
        if dataPoints.isEmpty {
            Text("No time-series data")
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else {
            Chart(dataPoints) { point in
                LineMark(
                    x: .value(columns.first ?? "time", point.time),
                    y: .value(point.series, point.value)
                )
                .foregroundStyle(by: .value("Series", point.series))
            }
            .chartXAxisLabel(columns.first ?? "time")
            .padding()
        }
    }

    private func numericValue(of value: Any?) -> Double? {
        guard let value else { return nil }
        switch value {
        case let n as NSNumber: return n.doubleValue
        case let d as Double: return d
        case let i as Int: return Double(i)
        default: return nil
        }
    }
}
