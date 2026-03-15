import SwiftUI
import AppKit

/// A single chip/tag displaying an attribute value, with a copy button.
///
/// Uses `CopyableCell` for the copy behaviour; adds chip styling
/// (rounded background, padding) on top.
struct AttributeValueChip: View {
    let value: String
    var monospaced: Bool = true
    var copyable: Bool = true

    var body: some View {
        Group {
            if copyable {
                CopyableCell(text: value, hideUntilHover: false)
            } else {
                Text(value)
                    .foregroundStyle(.primary)
                    .textSelection(.enabled)
            }
        }
        .font(monospaced ? .caption.monospaced() : .caption)
        .padding(.horizontal, 6)
        .padding(.vertical, 2)
        .background(.quaternary.opacity(0.5))
        .clipShape(RoundedRectangle(cornerRadius: 4))
    }
}

#Preview("AttributeValueChip") {
    VStack(spacing: 12) {
        AttributeValueChip(value: "server-1")
        AttributeValueChip(value: "production", monospaced: false)
        AttributeValueChip(value: "1.2.3")
        AttributeValueChip(value: "not-copyable", copyable: false)
    }
    .padding()
}

#Preview("AttributeValueChip - Long Value") {
    AttributeValueChip(value: "https://api.example.com/v1/users/12345")
        .padding()
}
