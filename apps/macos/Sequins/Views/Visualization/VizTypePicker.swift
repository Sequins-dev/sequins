import SwiftUI
import SequinsData

/// A compact menu for switching the `VizType` of a rendered visualization. `nil`
/// selection means "auto-select from the data".
struct VizTypePicker: View {
    @Binding var selection: VizType?
    /// The types offered; defaults to all.
    var available: [VizType] = VizType.allCases

    var body: some View {
        Menu {
            Button {
                selection = nil
            } label: {
                Label("Auto", systemImage: "wand.and.stars")
            }
            Divider()
            ForEach(available, id: \.self) { type in
                Button {
                    selection = type
                } label: {
                    Label(type.displayName, systemImage: type.systemImage)
                }
            }
        } label: {
            Image(systemName: (selection ?? .table).systemImage)
                .font(.caption)
        }
        .menuStyle(.borderlessButton)
        .menuIndicator(.hidden)
        .fixedSize()
        .help("Change visualization type")
    }
}
