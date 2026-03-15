import SwiftUI
import AppKit

/// A text view with a copy-to-clipboard button.
///
/// Used directly in table cells (where chip styling would look wrong),
/// and as the inner content of `AttributeValueChip`.
struct CopyableCell: View {
    let text: String
    /// When true, the copy button is hidden until the user hovers over the cell.
    var hideUntilHover: Bool = true

    @State private var isHovered = false
    @State private var showCopied = false

    var body: some View {
        HStack(spacing: 4) {
            Text(text)
                .foregroundStyle(.primary)
                .textSelection(.enabled)

            Button(action: copy) {
                Image(systemName: showCopied ? "checkmark" : "doc.on.doc")
                    .imageScale(.small)
                    .foregroundStyle(showCopied ? .green : .secondary)
            }
            .buttonStyle(.plain)
            .opacity(hideUntilHover ? (isHovered || showCopied ? 1 : 0) : 1)
            .help("Copy to clipboard")
        }
        .contentShape(Rectangle())
        .onHover { isHovered = $0 }
    }

    private func copy() {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(text, forType: .string)
        withAnimation(.easeInOut(duration: 0.2)) { showCopied = true }
        DispatchQueue.main.asyncAfter(deadline: .now() + 1.5) {
            withAnimation(.easeInOut(duration: 0.2)) { showCopied = false }
        }
    }
}
