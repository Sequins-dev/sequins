import SwiftUI

/// A reusable export button with dropdown menu
struct ExportButton<Content: View>: View {
    let menuContent: Content

    init(@ViewBuilder menuContent: () -> Content) {
        self.menuContent = menuContent()
    }

    var body: some View {
        Menu {
            menuContent
        } label: {
            Image(systemName: "square.and.arrow.up")
                .font(.caption)
        }
        .menuStyle(.borderlessButton)
        .menuIndicator(.hidden)
        .fixedSize()
        .help("Export data")
    }
}

#Preview("ExportButton") {
    ExportButton {
        Button("Export as JSON") { }
        Button("Export as CSV") { }
        Divider()
        Button("Export as PNG") { }
    }
    .padding()
}

#Preview("ExportButton - Profile Export") {
    ExportButton {
        Button("Export as Speedscope JSON") { }
        Button("Export as Chrome Trace") { }
        Button("Export as Collapsed Stack") { }
    }
    .padding()
}
