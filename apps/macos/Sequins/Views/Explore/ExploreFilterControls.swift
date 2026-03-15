import SwiftUI
import SequinsData

/// Filter bar controls for the Explore tab
struct ExploreFilterControls: View {
    @Bindable var viewModel: ExploreViewModel

    var body: some View {
        HStack(spacing: 8) {
            // Export results
            ExportButton {
                Button("Export as JSON") { viewModel.exportAsJSON() }
                    .disabled(viewModel.schema == nil)
                Button("Export as CSV") { viewModel.exportAsCSV() }
                    .disabled(viewModel.schema == nil)
            }

            // Visualization override, copy button, rows per page (temporarily hidden — not yet functional)
            if false {
                Menu {
                    Button {
                        viewModel.visualizationOverride = nil
                    } label: {
                        HStack {
                            Text("Auto")
                            if viewModel.visualizationOverride == nil {
                                Image(systemName: "checkmark")
                            }
                        }
                    }

                    Divider()

                    Button {
                        viewModel.visualizationOverride = .table
                    } label: {
                        HStack {
                            Text("Table")
                            if viewModel.visualizationOverride == .table {
                                Image(systemName: "checkmark")
                            }
                        }
                    }

                    Button {
                        viewModel.visualizationOverride = .timeSeries
                    } label: {
                        HStack {
                            Text("Time Series")
                            if viewModel.visualizationOverride == .timeSeries {
                                Image(systemName: "checkmark")
                            }
                        }
                    }

                    Button {
                        viewModel.visualizationOverride = .scalar
                    } label: {
                        HStack {
                            Text("Scalar")
                            if viewModel.visualizationOverride == .scalar {
                                Image(systemName: "checkmark")
                            }
                        }
                    }
                } label: {
                    HStack(spacing: 4) {
                        Image(systemName: "chart.bar")
                            .font(.caption)
                        Text(vizOverrideLabel)
                            .font(.caption)
                        Image(systemName: "chevron.down")
                            .font(.system(size: 8))
                    }
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .background(.quaternary)
                    .cornerRadius(4)
                }
                .menuStyle(.borderlessButton)
                .menuIndicator(.hidden)
                .fixedSize()

                Button(action: { viewModel.copyQueryToClipboard() }) {
                    Image(systemName: "doc.on.doc")
                        .font(.caption)
                }
                .buttonStyle(.plain)
                .padding(.horizontal, 6)
                .padding(.vertical, 4)
                .background(.quaternary)
                .cornerRadius(4)
                .help("Copy query to clipboard")

                Menu {
                    ForEach([100, 500, 1000], id: \.self) { size in
                        Button {
                            viewModel.pageSize = size
                            viewModel.currentPage = 0
                        } label: {
                            HStack {
                                Text("\(size) rows/page")
                                if viewModel.pageSize == size {
                                    Image(systemName: "checkmark")
                                }
                            }
                        }
                    }
                } label: {
                    HStack(spacing: 4) {
                        Image(systemName: "list.number")
                            .font(.caption)
                        Text("\(viewModel.pageSize)/page")
                            .font(.caption)
                        Image(systemName: "chevron.down")
                            .font(.system(size: 8))
                    }
                    .padding(.horizontal, 8)
                    .padding(.vertical, 4)
                    .background(.quaternary)
                    .cornerRadius(4)
                }
                .menuStyle(.borderlessButton)
                .menuIndicator(.hidden)
                .fixedSize()
            }
        }
    }

    private var vizOverrideLabel: String {
        switch viewModel.visualizationOverride {
        case .none: return "Auto"
        case .table: return "Table"
        case .timeSeries: return "Time Series"
        case .scalar: return "Scalar"
        default: return "Auto"
        }
    }
}
