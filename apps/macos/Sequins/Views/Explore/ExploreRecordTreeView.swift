import SwiftUI
import SequinsData

/// Container for the tree-form record explorer.
///
/// Renders up to 500 records as expandable key-value trees.
/// Nested List, Struct, and Map columns are fully navigable.
struct ExploreRecordTreeView: View {
    let records: [RecordNode]

    private let cap = 500

    private var displayRecords: [RecordNode] {
        Array(records.prefix(cap))
    }

    var body: some View {
        if records.isEmpty {
            Text("No results")
                .foregroundStyle(.secondary)
                .frame(maxWidth: .infinity, maxHeight: .infinity)
        } else {
            VStack(spacing: 0) {
                ScrollView(.vertical) {
                    LazyVStack(alignment: .leading, spacing: 1) {
                        ForEach(displayRecords) { record in
                            RecordTreeRow(node: record, depth: 0)
                            Divider()
                        }
                    }
                    .padding(.vertical, 4)
                }

                if records.count > cap {
                    HStack {
                        Image(systemName: "exclamationmark.triangle")
                            .foregroundStyle(.orange)
                            .font(.caption)
                        Text("Showing first \(cap) of \(records.count) records")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    .padding(.horizontal, 12)
                    .padding(.vertical, 6)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .background(Color(nsColor: .windowBackgroundColor))
                }
            }
        }
    }
}
