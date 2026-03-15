import SwiftUI

/// A layout that arranges views in a flow, wrapping to the next line when needed
struct FlowLayout: Layout {
    var spacing: CGFloat = 4
    var lineSpacing: CGFloat = 4

    func sizeThatFits(proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) -> CGSize {
        let result = layout(proposal: proposal, subviews: subviews)
        return result.size
    }

    func placeSubviews(in bounds: CGRect, proposal: ProposedViewSize, subviews: Subviews, cache: inout ()) {
        let result = layout(proposal: proposal, subviews: subviews)

        for (index, subview) in subviews.enumerated() {
            let position = result.positions[index]
            subview.place(
                at: CGPoint(x: bounds.minX + position.x, y: bounds.minY + position.y),
                proposal: .unspecified
            )
        }
    }

    private func layout(proposal: ProposedViewSize, subviews: Subviews) -> LayoutResult {
        let maxWidth = proposal.width ?? .infinity
        var positions: [CGPoint] = []
        var currentX: CGFloat = 0
        var currentY: CGFloat = 0
        var lineHeight: CGFloat = 0
        var totalWidth: CGFloat = 0

        for subview in subviews {
            let size = subview.sizeThatFits(.unspecified)

            // Check if we need to wrap to next line
            if currentX + size.width > maxWidth && currentX > 0 {
                currentX = 0
                currentY += lineHeight + lineSpacing
                lineHeight = 0
            }

            positions.append(CGPoint(x: currentX, y: currentY))

            currentX += size.width + spacing
            lineHeight = max(lineHeight, size.height)
            totalWidth = max(totalWidth, currentX - spacing)
        }

        let totalHeight = currentY + lineHeight
        return LayoutResult(
            size: CGSize(width: totalWidth, height: totalHeight),
            positions: positions
        )
    }

    private struct LayoutResult {
        let size: CGSize
        let positions: [CGPoint]
    }
}

#Preview("FlowLayout") {
    FlowLayout(spacing: 4, lineSpacing: 4) {
        ForEach(["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"], id: \.self) { item in
            Text(item)
                .padding(.horizontal, 8)
                .padding(.vertical, 4)
                .background(.blue.opacity(0.2))
                .clipShape(RoundedRectangle(cornerRadius: 4))
        }
    }
    .frame(width: 200)
    .padding()
}

#Preview("FlowLayout - Wide") {
    FlowLayout(spacing: 4, lineSpacing: 4) {
        ForEach(["server-1", "server-2", "server-3"], id: \.self) { item in
            Text(item)
                .padding(.horizontal, 8)
                .padding(.vertical, 4)
                .background(.gray.opacity(0.2))
                .clipShape(RoundedRectangle(cornerRadius: 4))
        }
    }
    .frame(width: 400)
    .padding()
}
