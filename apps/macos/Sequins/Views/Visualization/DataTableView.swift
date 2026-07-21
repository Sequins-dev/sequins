import SwiftUI
import SequinsData

/// A columnar, sortable, type-aware table for tabular results. Header click sorts;
/// cells are formatted by their semantic type (timestamps, durations, monospaced ids,
/// grouped numbers) via `VizFormat`. Shows an explicit "showing N of M" when capped.
struct DataTableView: View {
    let columns: [String]
    let rows: [[Any?]]
    var columnTypes: [NodeTypeLabel] = []

    /// Rows rendered before capping (charts stay responsive on huge results).
    private let rowCap = 500

    @State private var sortColumn: Int?
    @State private var ascending = true

    private func type(_ i: Int) -> NodeTypeLabel {
        i < columnTypes.count ? columnTypes[i] : .unknown
    }

    private var sortedRows: [[Any?]] {
        guard let col = sortColumn else { return rows }
        let sorted = rows.sorted { a, b in
            lessThan(a.count > col ? a[col] : nil, b.count > col ? b[col] : nil)
        }
        return ascending ? sorted : sorted.reversed()
    }

    /// a < b, nils last.
    private func lessThan(_ a: Any?, _ b: Any?) -> Bool {
        switch (a, b) {
        case (nil, nil): return false
        case (nil, _): return false
        case (_, nil): return true
        default: break
        }
        if let an = VizFormat.numeric(a), let bn = VizFormat.numeric(b) {
            return an < bn
        }
        return VizFormat.string(a).localizedCaseInsensitiveCompare(VizFormat.string(b)) == .orderedAscending
    }

    var body: some View {
        if rows.isEmpty {
            VizMessage(icon: "tablecells", text: "No results")
        } else {
            VStack(spacing: 0) {
                ScrollView([.horizontal, .vertical]) {
                    Grid(alignment: .leading, horizontalSpacing: 0, verticalSpacing: 0) {
                        headerRow
                        ForEach(Array(sortedRows.prefix(rowCap).enumerated()), id: \.offset) { rowIdx, row in
                            GridRow {
                                ForEach(columns.indices, id: \.self) { col in
                                    cell(row, col)
                                        .background(rowIdx.isMultiple(of: 2) ? Color.clear : Color.secondary.opacity(0.04))
                                }
                            }
                        }
                    }
                }
                if rows.count > rowCap {
                    Text("Showing \(rowCap) of \(rows.count) rows")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                        .frame(maxWidth: .infinity, alignment: .leading)
                        .padding(6)
                        .background(Color(nsColor: .controlBackgroundColor))
                }
            }
        }
    }

    private var headerRow: some View {
        GridRow {
            ForEach(columns.indices, id: \.self) { col in
                Button {
                    if sortColumn == col { ascending.toggle() } else { sortColumn = col; ascending = true }
                } label: {
                    HStack(spacing: 3) {
                        Text(VizFormat.label(columns[col]))
                            .font(.system(size: 12, weight: .semibold))
                            .lineLimit(1)
                        if sortColumn == col {
                            Image(systemName: ascending ? "chevron.up" : "chevron.down")
                                .font(.system(size: 8))
                        }
                        Spacer(minLength: 0)
                    }
                    .foregroundStyle(.secondary)
                    .padding(.horizontal, 12)
                    .padding(.vertical, 6)
                    .frame(minWidth: 90, alignment: .leading)
                    .background(Color(nsColor: .controlBackgroundColor))
                }
                .buttonStyle(.plain)
                .overlay(alignment: .bottom) { Divider() }
            }
        }
    }

    @ViewBuilder
    private func cell(_ row: [Any?], _ col: Int) -> some View {
        let value = col < row.count ? row[col] : nil
        let t = type(col)
        let text = VizFormat.cell(value, type: t, column: columns[col])
        Text(text)
            .font(.system(size: 12, design: (t == .id) ? .monospaced : .default))
            .foregroundStyle(t == .null ? .secondary : .primary)
            .lineLimit(1)
            .truncationMode(.middle)
            .padding(.horizontal, 12)
            .padding(.vertical, 5)
            .frame(minWidth: 90, maxWidth: 320, alignment: .leading)
            .textSelection(.enabled)
    }
}
