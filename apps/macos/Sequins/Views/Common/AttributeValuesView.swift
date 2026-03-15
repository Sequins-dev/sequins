import SwiftUI

/// Renders an array of attribute values as a flow of chips
struct AttributeValuesView: View {
    let values: [String]
    var monospaced: Bool = true
    var copyable: Bool = false

    var body: some View {
        FlowLayout(spacing: 4, lineSpacing: 4) {
            ForEach(Array(values.enumerated()), id: \.offset) { _, value in
                AttributeValueChip(value: value, monospaced: monospaced, copyable: copyable)
            }
        }
    }
}

#Preview("AttributeValuesView - Single") {
    AttributeValuesView(values: ["production"])
        .padding()
}

#Preview("AttributeValuesView - Multiple") {
    AttributeValuesView(values: ["server-1", "server-2", "server-3"])
        .padding()
}

#Preview("AttributeValuesView - Many Values") {
    AttributeValuesView(values: [
        "server-1", "server-2", "server-3",
        "server-4", "server-5", "server-6"
    ])
    .frame(width: 300)
    .padding()
}

#Preview("AttributeValuesView - Long Values") {
    AttributeValuesView(values: [
        "https://api.example.com/v1/users",
        "https://api.example.com/v1/products"
    ])
    .frame(width: 400)
    .padding()
}
