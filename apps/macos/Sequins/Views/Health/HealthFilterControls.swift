import SwiftUI
import SequinsData

/// Filter bar controls for the Health tab
struct HealthFilterControls: View {
    @Bindable var viewModel: HealthViewModel

    var body: some View {
        HStack(spacing: 8) {
            // Export health snapshot
            ExportButton {
                Button("Export as JSON") { viewModel.exportHealthSnapshot() }
                    .disabled(viewModel.feed == nil)
            }

            // Threshold settings shortcut (temporarily hidden — not yet functional)
            if false {
                Button(action: { viewModel.showingHealthRulesSheet = true }) {
                    Image(systemName: "gear")
                        .font(.caption)
                }
                .buttonStyle(.plain)
                .padding(.horizontal, 6)
                .padding(.vertical, 4)
                .background(.quaternary)
                .cornerRadius(4)
                .help("Configure health threshold rules")
            }

            // Comparison period selector (temporarily hidden — not yet functional)
            if false {
                Menu {
                    ForEach(ComparisonPeriod.allCases) { period in
                        Button {
                            viewModel.comparisonPeriod = period
                        } label: {
                            HStack {
                                Text(period.rawValue)
                                if viewModel.comparisonPeriod == period {
                                    Image(systemName: "checkmark")
                                }
                            }
                        }
                    }
                } label: {
                    HStack(spacing: 4) {
                        Image(systemName: "arrow.left.arrow.right")
                            .font(.caption)
                        Text(viewModel.comparisonPeriod == .none ? "Compare" : viewModel.comparisonPeriod.rawValue)
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
}
