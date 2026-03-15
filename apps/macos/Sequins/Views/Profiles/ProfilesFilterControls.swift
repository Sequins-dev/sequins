import SwiftUI

/// Filter controls specific to the Profiles tab
struct ProfilesFilterControls: View {
    @Bindable var viewModel: ProfilesViewModel

    var body: some View {
        HStack(spacing: 12) {
            // Search field
            searchField

            // Value type selector (populated from live data)
            if let feed = viewModel.feed, !feed.availableValueTypes.isEmpty {
                valueTypeMenu(types: feed.availableValueTypes)

                Divider()
                    .frame(height: 20)
            }

            // Export Menu
            ExportButton {
                Button("Export as JSON") {
                    viewModel.exportAsJSON()
                }
            }
        }
    }

    private var searchField: some View {
        HStack(spacing: 4) {
            Image(systemName: "magnifyingglass")
                .font(.caption)
                .foregroundStyle(.secondary)
            TextField("Search frames...", text: $viewModel.searchText)
                .textFieldStyle(.plain)
                .font(.caption)
                .frame(width: 120)
            if !viewModel.searchText.isEmpty {
                Button(action: { viewModel.searchText = "" }) {
                    Image(systemName: "xmark.circle.fill")
                        .foregroundColor(.secondary)
                }
                .buttonStyle(.plain)
            }
        }
        .padding(.horizontal, 8)
        .padding(.vertical, 4)
        .background(.quaternary)
        .cornerRadius(4)
        .fixedSize()
    }

    private func valueTypeMenu(types: [String]) -> some View {
        Menu {
            ForEach(types, id: \.self) { type in
                Button(action: { viewModel.selectedValueType = type }) {
                    HStack {
                        Text(type)
                        if viewModel.selectedValueType == type {
                            Image(systemName: "checkmark")
                        }
                    }
                }
            }
        } label: {
            HStack(spacing: 4) {
                Text(viewModel.selectedValueType ?? types.first ?? "Type")
                    .font(.caption)
            }
            .padding(.horizontal, 8)
            .padding(.vertical, 4)
            .background(.quaternary)
            .cornerRadius(4)
        }
        .menuStyle(.borderlessButton)
        .fixedSize()
    }
}

#Preview("ProfilesFilterControls") {
    struct PreviewWrapper: View {
        @State private var viewModel = ProfilesViewModel()

        var body: some View {
            ProfilesFilterControls(viewModel: viewModel)
                .padding()
        }
    }
    return PreviewWrapper()
}
