import SwiftUI
import AppKit
import SequinsData

/// Canvas-based icicle graph visualization driven by a FlamegraphFeed.
struct CanvasIcicleGraphView: View {
    let feed: FlamegraphFeed
    let width: CGFloat
    @Binding var hoveredNodeId: String?
    @Binding var selectedNodeId: String?
    let searchText: String
    @Binding var zoomedNodeId: String?

    private let rowHeight: CGFloat = 20
    private let minFrameWidth: CGFloat = 2

    // Zoom scale — content width = width * manualZoomScale
    @State private var manualZoomScale: CGFloat = 1.0
    @State private var pinchStartScale: CGFloat = 1.0

    // Programmatic scroll position (bidirectional ScrollView handles bounding automatically)
    @State private var scrollPosition = ScrollPosition()

    // Live scroll offset tracked via onScrollGeometryChange
    @State private var currentScrollOffset: CGPoint = .zero

    // Pinch-from-mouse state: captured at the moment each pinch begins
    @State private var isPinching = false
    @State private var pinchStartScrollX: CGFloat = 0
    @State private var pinchStartContentX: CGFloat = 0  // mouse content-x at pinch start

    // Mouse content-x tracked continuously via hover
    @State private var mouseContentX: CGFloat = 0


    var displayRootId: String { feed.rootNodeId ?? "root" }
    var contentWidth: CGFloat { width * manualZoomScale }

    var body: some View {
        let rows = buildRows(canvasWidth: contentWidth)
        let contentHeight = calculateCanvasHeight(for: rows)

        ScrollView([.horizontal, .vertical], showsIndicators: true) {
            Canvas { context, size in
                let _ = manualZoomScale

                for row in rows {
                    for info in row.frames {
                        drawCanvasFrame(context: context, info: info)
                    }
                }

                if rows.isEmpty {
                    context.draw(
                        Text("No profile data available").font(.caption).foregroundColor(.secondary),
                        in: CGRect(x: size.width / 2 - 100, y: size.height / 2 - 10, width: 200, height: 20)
                    )
                }
            }
            .frame(width: contentWidth, height: contentHeight)
            .background(Color(NSColor.textBackgroundColor))
            .onTapGesture { location in
                handleTap(at: location, in: rows)
            }
            .onHover { isHovering in
                if !isHovering { hoveredNodeId = nil }
            }
            .onContinuousHover { phase in
                switch phase {
                case .active(let location):
                    mouseContentX = location.x
                    updateHoveredNode(at: location, in: rows)
                case .ended:
                    hoveredNodeId = nil
                }
            }
            .simultaneousGesture(
                MagnificationGesture()
                    .onChanged { value in
                        // Capture anchor point on first call of each gesture
                        if !isPinching {
                            isPinching = true
                            pinchStartScrollX = currentScrollOffset.x
                            pinchStartContentX = mouseContentX
                        }

                        let newScale = max(1.0, min(pinchStartScale * value, 50))
                        let scaleRatio = newScale / pinchStartScale

                        // Adjust scroll so the point under the cursor stays fixed:
                        // newScrollX = anchorContentX * (scaleRatio - 1) + anchorScrollX
                        let newScrollX = max(0, pinchStartContentX * (scaleRatio - 1) + pinchStartScrollX)

                        manualZoomScale = newScale
                        scrollPosition.scrollTo(point: CGPoint(x: newScrollX, y: currentScrollOffset.y))
                    }
                    .onEnded { _ in
                        isPinching = false
                        pinchStartScale = manualZoomScale
                    }
            )
            .contextMenu {
                contextMenuItems()
            }
        }
        .scrollPosition($scrollPosition)
        .onScrollGeometryChange(for: CGPoint.self, of: { $0.contentOffset }) { _, newOffset in
            currentScrollOffset = newOffset
        }
        .frame(width: width)
        .onChange(of: zoomedNodeId) { _, newValue in
            // Reset when zoom is cleared externally (e.g., ZoomIndicatorView reset button)
            if newValue == nil {
                manualZoomScale = 1.0
                pinchStartScale = 1.0
                scrollPosition.scrollTo(point: .zero)
            }
        }
    }

    // MARK: - Context menu

    @ViewBuilder
    private func contextMenuItems() -> some View {
        if let hoveredId = hoveredNodeId, let node = getNode(hoveredId) {
            if hoveredId != displayRootId {
                Button("Zoom to '\(node.functionName)'") {
                    animateZoom(to: hoveredId)
                }
            }
            Button("Copy Function Name") {
                NSPasteboard.general.clearContents()
                NSPasteboard.general.setString(node.functionName, forType: .string)
            }
        }
        if zoomedNodeId != nil {
            Button("Reset Zoom") {
                animateZoom(to: nil)
            }
        }
    }

    // MARK: - Layout

    private func buildRows(canvasWidth: CGFloat) -> [CanvasFrameRow] {
        guard let rootNode = getNode(displayRootId) else { return [] }

        var rows: [CanvasFrameRow] = []
        var currentDepth: [(node: FlamegraphNode, x: CGFloat, w: CGFloat)] = [(rootNode, 0, canvasWidth)]
        var depth = 0

        while !currentDepth.isEmpty && depth < 500 {
            var rowFrames: [CanvasFrameInfo] = []
            var nextDepth: [(node: FlamegraphNode, x: CGFloat, w: CGFloat)] = []
            let y = CGFloat(depth) * rowHeight

            for (node, x, w) in currentDepth {
                rowFrames.append(CanvasFrameInfo(node: node, rect: CGRect(x: x, y: y, width: w, height: rowHeight)))

                var childX = x
                for childId in node.childIds {
                    if let child = getNode(childId) {
                        let childW = node.totalValue > 0
                            ? (CGFloat(child.totalValue) / CGFloat(node.totalValue)) * w
                            : 0
                        if childW >= minFrameWidth { nextDepth.append((child, childX, childW)) }
                        childX += childW
                    }
                }
            }

            if !rowFrames.isEmpty { rows.append(CanvasFrameRow(frames: rowFrames)) }
            currentDepth = nextDepth
            depth += 1
        }

        return rows
    }

    private func calculateCanvasHeight(for rows: [CanvasFrameRow]) -> CGFloat {
        guard !rows.isEmpty else { return rowHeight * 10 }
        return CGFloat(rows.count) * rowHeight + 20
    }

    // MARK: - Drawing

    private func drawCanvasFrame(context: GraphicsContext, info: CanvasFrameInfo) {
        let node = info.node
        let rect = info.rect
        let isHovered = hoveredNodeId == node.id
        let isSelected = selectedNodeId == node.id
        let matchesSearch = !searchText.isEmpty && node.functionName.localizedCaseInsensitiveContains(searchText)

        let opacity: Double = isSelected ? 1.0 : isHovered ? 0.85 : 0.75

        let parentValue: Int64
        if let parentId = node.parentId, let parent = getNode(parentId) {
            parentValue = parent.totalValue
        } else {
            parentValue = node.totalValue
        }
        let ratio = parentValue > 0 ? Double(node.totalValue) / Double(parentValue) : 1.0
        let displayColor = ProfileColorScheme().colorForRatio(ratio)

        context.fill(Path(rect), with: .color(displayColor.opacity(opacity)))

        let borderOpacity = isHovered ? 0.6 : 0.2
        context.stroke(Path(rect), with: .color(.black.opacity(borderOpacity)), lineWidth: isHovered ? 1.5 : 0.5)

        if isSelected {
            context.stroke(Path(rect), with: .color(.white), lineWidth: 1)
        } else if matchesSearch {
            context.stroke(Path(rect), with: .color(.yellow), lineWidth: 1)
        }

        if rect.width > 40 {
            context.draw(
                Text(node.functionName).font(.caption2).foregroundColor(.white),
                in: rect.insetBy(dx: 4, dy: 4)
            )
        }
    }

    // MARK: - Interaction

    private func handleTap(at location: CGPoint, in rows: [CanvasFrameRow]) {
        for row in rows {
            for info in row.frames {
                if info.rect.contains(location) {
                    selectedNodeId = info.node.id
                    return
                }
            }
        }
        selectedNodeId = nil
    }

    private func updateHoveredNode(at location: CGPoint, in rows: [CanvasFrameRow]) {
        for row in rows {
            for info in row.frames {
                if info.rect.contains(location) {
                    if hoveredNodeId != info.node.id { hoveredNodeId = info.node.id }
                    return
                }
            }
        }
        hoveredNodeId = nil
    }

    // MARK: - Zoom

    private func animateZoom(to nodeId: String?) {
        zoomedNodeId = nodeId

        if let nodeId, let (origX, origW) = nodeOriginalRect(for: nodeId) {
            let newScale = max(1.0, width / max(origW, 1))
            manualZoomScale = newScale
            pinchStartScale = newScale
            scrollPosition.scrollTo(point: CGPoint(x: origX * newScale, y: 0))
        } else {
            manualZoomScale = 1.0
            pinchStartScale = 1.0
            scrollPosition.scrollTo(point: .zero)
        }
    }

    /// Returns the node's position and width at scale=1 (root spans viewport width).
    private func nodeOriginalRect(for nodeId: String) -> (x: CGFloat, w: CGFloat)? {
        guard let rootNode = getNode(displayRootId), getNode(nodeId) != nil else { return nil }

        var path: [String] = []
        var cur: String? = nodeId
        while let id = cur {
            path.insert(id, at: 0)
            if id == rootNode.id { break }
            cur = getNode(id)?.parentId
        }
        guard path.first == rootNode.id else { return nil }

        var nodeX: CGFloat = 0
        var nodeW: CGFloat = width
        var current = rootNode

        for i in 1 ..< path.count {
            let targetId = path[i]
            var childX = nodeX
            for childId in current.childIds {
                if let child = getNode(childId) {
                    let childW = current.totalValue > 0
                        ? (CGFloat(child.totalValue) / CGFloat(current.totalValue)) * nodeW
                        : 0
                    if childId == targetId { nodeX = childX; nodeW = childW; current = child; break }
                    childX += childW
                }
            }
        }

        return (nodeX, nodeW)
    }

    private func getNode(_ id: String) -> FlamegraphNode? {
        guard let idx = feed.nodeIndex[id], idx < feed.nodes.count else { return nil }
        return feed.nodes[idx]
    }
}

// MARK: - Supporting types

struct CanvasFrameRow: Equatable {
    let frames: [CanvasFrameInfo]
    static func == (lhs: CanvasFrameRow, rhs: CanvasFrameRow) -> Bool { lhs.frames == rhs.frames }
}

struct CanvasFrameInfo: Equatable {
    let node: FlamegraphNode
    let rect: CGRect
    static func == (lhs: CanvasFrameInfo, rhs: CanvasFrameInfo) -> Bool {
        lhs.node.id == rhs.node.id && lhs.rect == rhs.rect
    }
}
