import SwiftUI

/// Generic table view for SeQL query results with pagination
struct ExploreTableView: View {
    let columns: [String]
    let rows: [[Any?]]
    let pageSize: Int
    @Binding var currentPage: Int

    private var totalPages: Int {
        max(1, Int(ceil(Double(rows.count) / Double(pageSize))))
    }

    private var displayRows: [[Any?]] {
        let start = currentPage * pageSize
        let end = min(start + pageSize, rows.count)
        guard start < rows.count else { return [] }
        return Array(rows[start..<end])
    }

    var body: some View {
        if rows.isEmpty {
            Text("No results")
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else {
            VStack(spacing: 0) {
                ScrollView([.horizontal, .vertical]) {
                    Grid(alignment: .topLeading, horizontalSpacing: 0, verticalSpacing: 0) {
                        // Header row
                        GridRow {
                            ForEach(columns.indices, id: \.self) { idx in
                                Text(columns[idx])
                                    .font(.system(size: 12, weight: .semibold))
                                    .foregroundStyle(.secondary)
                                    .padding(.horizontal, 12)
                                    .padding(.vertical, 6)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .background(Color(nsColor: .controlBackgroundColor))
                                    .overlay(alignment: .bottom) {
                                        Divider()
                                    }
                            }
                        }

                        // Data rows for the current page
                        ForEach(displayRows.indices, id: \.self) { rowIdx in
                            GridRow {
                                ForEach(columns.indices, id: \.self) { colIdx in
                                    let value = colIdx < displayRows[rowIdx].count ? displayRows[rowIdx][colIdx] : nil
                                    let colName = columns[colIdx]
                                    let text = formattedText(for: value, column: colName)
                                    let isId = isIdColumn(colName)

                                    Group {
                                        if isId && value != nil {
                                            CopyableCell(text: text)
                                        } else {
                                            Text(text)
                                                .foregroundStyle(value == nil ? .secondary : .primary)
                                        }
                                    }
                                    .font(.system(size: 12, design: .monospaced))
                                    .padding(.horizontal, 12)
                                    .padding(.vertical, 5)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                                    .background(rowIdx.isMultiple(of: 2) ? Color.clear : Color(nsColor: .controlBackgroundColor).opacity(0.4))
                                    .overlay(alignment: .bottom) {
                                        Divider().opacity(0.5)
                                    }
                                }
                            }
                        }
                    }
                    .frame(maxWidth: .infinity)
                }

                // Pagination bar
                paginationBar
            }
        }
    }

    // MARK: - Pagination Bar

    private var paginationBar: some View {
        HStack(spacing: 12) {
            Button(action: { currentPage = max(0, currentPage - 1) }) {
                Image(systemName: "chevron.left")
            }
            .buttonStyle(.plain)
            .disabled(currentPage == 0)

            Text("Page \(currentPage + 1) of \(totalPages)")
                .font(.caption)
                .foregroundStyle(.secondary)

            Button(action: { currentPage = min(totalPages - 1, currentPage + 1) }) {
                Image(systemName: "chevron.right")
            }
            .buttonStyle(.plain)
            .disabled(currentPage >= totalPages - 1)

            Spacer()

            Text("\(rows.count) row\(rows.count == 1 ? "" : "s") total")
                .font(.caption)
                .foregroundStyle(.tertiary)
        }
        .padding(.horizontal, 12)
        .padding(.vertical, 6)
        .background(Color(nsColor: .controlBackgroundColor))
    }

    // MARK: - Column classification

    private func isIdColumn(_ name: String) -> Bool {
        name.hasSuffix("_id") || name == "id"
    }

    private func isDurationColumn(_ name: String) -> Bool {
        name == "duration" || name.hasSuffix("_duration")
    }

    private func isTimestampColumn(_ name: String) -> Bool {
        name == "timestamp" || name.hasSuffix("_time")
    }

    // MARK: - Cell formatting

    private func formattedText(for value: Any?, column: String) -> String {
        guard let value else { return "null" }
        if isDurationColumn(column), let num = value as? NSNumber {
            return durationText(nanoseconds: num.int64Value)
        }
        if isTimestampColumn(column), let num = value as? NSNumber {
            return timestampText(nanoseconds: num.int64Value)
        }
        return cellText(for: value)
    }

    private func cellText(for value: Any?) -> String {
        guard let value else { return "null" }
        switch value {
        case let s as String: return s
        case let n as NSNumber: return n.stringValue
        case let b as Bool: return b ? "true" : "false"
        default: return String(describing: value)
        }
    }

    private static let timestampFormatter: DateFormatter = {
        let f = DateFormatter()
        f.dateFormat = "yyyy-MM-dd HH:mm:ss.SSS"
        return f
    }()

    private func timestampText(nanoseconds ns: Int64) -> String {
        let date = Date(timeIntervalSince1970: Double(ns) / 1_000_000_000.0)
        return Self.timestampFormatter.string(from: date)
    }

    private func durationText(nanoseconds ns: Int64) -> String {
        switch ns {
        case ..<1_000:
            return "\(ns) ns"
        case ..<1_000_000:
            return String(format: "%.1f µs", Double(ns) / 1_000.0)
        case ..<1_000_000_000:
            return String(format: "%.2f ms", Double(ns) / 1_000_000.0)
        default:
            return String(format: "%.3f s", Double(ns) / 1_000_000_000.0)
        }
    }
}
