import SwiftUI
import SequinsData
import UniformTypeIdentifiers

// A dragged panel is carried as its stable id (a UUID string) via a plain-text
// `NSItemProvider` (a registered system type, so it round-trips reliably). Drops use a
// `DropDelegate` so we get the live cursor location (`dropUpdated`) and can show an
// insertion bar in the exact gap/edge where the chart will land, rather than
// highlighting a whole target chart. Insertion index `k` (0...panelCount) means "before
// panel k"; `panelCount` is the trailing edge of the row.

/// The single gap a dragged chart would drop into, shared across all rows so exactly one
/// insertion bar shows at a time and it clears globally on drop (per-row `dropExited` is
/// not reliably delivered, which otherwise left stale guides behind after a drop).
private struct DropSlot: Equatable {
    let row: Int
    let gap: Int
}

/// What the chart-editor sheet is currently targeting.
enum ChartEditorTarget: Identifiable {
    case addToRow(Int)
    case newRow
    case edit(row: Int, panel: Int)

    var id: String {
        switch self {
        case .addToRow(let r): return "add-\(r)"
        case .newRow: return "newrow"
        case .edit(let r, let p): return "edit-\(r)-\(p)"
        }
    }
}

/// A dashboard as a vertical stack of full-width rows. Each row splits its width among
/// its panels by weight. Rows are resized by the bars between them; panels within a row
/// are re-proportioned by the bars between them; panels can be added (right-edge slot or
/// a new row) removed, and configured. Panels honor the dashboard's live toggle +
/// time-range controls.
struct DashboardGridView: View {
    @Bindable var viewModel: DashboardsViewModel
    let dataSource: DataSource?
    let timeRange: TimeRange
    let isLive: Bool

    @State private var editorTarget: ChartEditorTarget?
    /// The gap a chart is currently being dragged over (shared across rows), or nil.
    @State private var activeDropSlot: DropSlot?
    /// True only while a chart is mid-drag. `dropEntered` sets it, `performDrop` clears
    /// it — the insertion bar is gated on this, so the stray `dropUpdated` SwiftUI fires
    /// AFTER a drop (which re-sets `activeDropSlot`) can't make the guide reappear.
    @State private var dragActive = false

    private let handleThickness: CGFloat = 10

    var body: some View {
        ScrollView(.vertical) {
            VStack(spacing: 0) {
                if let dashboard = viewModel.selected {
                    ForEach(Array(dashboard.rows.enumerated()), id: \.element.id) { rowIndex, row in
                        DashboardRowView(
                            row: row,
                            rowIndex: rowIndex,
                            viewModel: viewModel,
                            dataSource: dataSource,
                            timeRange: timeRange,
                            isLive: isLive,
                            activeDropSlot: $activeDropSlot,
                            dragActive: $dragActive,
                            onAddChart: { editorTarget = .addToRow(rowIndex) },
                            onEditPanel: { panelIndex in editorTarget = .edit(row: rowIndex, panel: panelIndex) }
                        )
                        .frame(height: row.height)

                        RowResizeHandle(
                            thickness: handleThickness,
                            revealed: dragActive,
                            onChanged: { dy in
                                if let ds = dataSource {
                                    viewModel.setRowHeight(rowIndex, height: row.height + dy, persist: false, dataSource: ds)
                                }
                            },
                            onEnded: { dy in
                                if let ds = dataSource {
                                    viewModel.setRowHeight(rowIndex, height: row.height + dy, persist: true, dataSource: ds)
                                }
                            }
                        )
                    }

                    addRowButton
                }
            }
            .padding(8)
        }
        .sheet(item: $editorTarget) { target in
            ChartEditorSheet(
                target: target,
                viewModel: viewModel,
                dataSource: dataSource,
                timeRange: timeRange
            )
        }
    }

    private var addRowButton: some View {
        Button {
            editorTarget = .newRow
        } label: {
            HStack {
                Image(systemName: "plus")
                Text("Add row")
            }
            .font(.caption)
            .foregroundStyle(.secondary)
            .frame(maxWidth: .infinity)
            .frame(height: 34)
            .background(
                RoundedRectangle(cornerRadius: 8)
                    .strokeBorder(style: StrokeStyle(lineWidth: 1, dash: [4]))
                    .foregroundStyle(.secondary.opacity(0.4))
            )
        }
        .buttonStyle(.plain)
        .padding(.top, 6)
    }
}

// MARK: - Row

private struct DashboardRowView: View {
    let row: DashboardRow
    let rowIndex: Int
    @Bindable var viewModel: DashboardsViewModel
    let dataSource: DataSource?
    let timeRange: TimeRange
    let isLive: Bool
    @Binding var activeDropSlot: DropSlot?
    @Binding var dragActive: Bool
    let onAddChart: () -> Void
    let onEditPanel: (Int) -> Void

    /// Transient weights during a ratio drag (nil when not dragging).
    @State private var dragWeights: [Double]?
    @State private var hoveringAddSlot = false

    private let panelHandleWidth: CGFloat = 8
    private let addSlotCollapsed: CGFloat = 12
    private let addSlotExpanded: CGFloat = 120

    private var weights: [Double] {
        dragWeights ?? row.panels.map { $0.weight }
    }

    /// This row's active insertion gap — only while a drag is actually in progress
    /// (`dragActive`), so a post-drop stray `dropUpdated` can't resurrect the guide.
    private var dropInsertion: Int? {
        guard dragActive else { return nil }
        return activeDropSlot.flatMap { $0.row == rowIndex ? $0.gap : nil }
    }

    /// Set this row's insertion gap into the shared slot; nil clears it (only if the
    /// shared slot currently belongs to this row, so leaving one row can't wipe the
    /// indicator another row just claimed).
    private func setInsertion(_ gap: Int?) {
        if let gap {
            activeDropSlot = DropSlot(row: rowIndex, gap: gap)
        } else if activeDropSlot?.row == rowIndex {
            activeDropSlot = nil
        }
    }

    var body: some View {
        GeometryReader { geo in
            let addSlotWidth = hoveringAddSlot ? addSlotExpanded : addSlotCollapsed
            let handlesWidth = panelHandleWidth * CGFloat(max(0, row.panels.count - 1))
            let panelsWidth = max(0, geo.size.width - handlesWidth - addSlotWidth)
            let total = max(0.0001, weights.reduce(0, +))

            HStack(spacing: 0) {
                ForEach(Array(row.panels.enumerated()), id: \.element.id) { panelIndex, panel in
                    let panelWidth = panelsWidth * CGFloat(weights[panelIndex] / total)
                    DashboardPanelView(
                        panel: panel,
                        dataSource: dataSource,
                        timeRange: timeRange,
                        isLive: isLive,
                        onEdit: { onEditPanel(panelIndex) },
                        onRemove: {
                            if let ds = dataSource {
                                viewModel.removePanel(row: rowIndex, panel: panelIndex, dataSource: ds)
                            }
                        },
                        onVizTypeChange: { newType in
                            updatePanelType(panelIndex, newType)
                        }
                    )
                    .frame(width: panelWidth)
                    // Insertion bar in the gap on this panel's leading edge.
                    .overlay(alignment: .leading) { insertionBar(active: dropInsertion == panelIndex) }
                    // Last panel also shows the bar on its trailing edge (the final gap).
                    .overlay(alignment: .trailing) {
                        insertionBar(active: panelIndex == row.panels.count - 1 && dropInsertion == row.panels.count)
                    }

                    // Ratio handle between this panel and the next.
                    if panelIndex < row.panels.count - 1 {
                        PanelRatioHandle(
                            width: panelHandleWidth,
                            revealed: dragActive,
                            onChanged: { dx in ratioDrag(after: panelIndex, dx: dx, panelsWidth: panelsWidth, commit: false) },
                            onEnded: { dx in ratioDrag(after: panelIndex, dx: dx, panelsWidth: panelsWidth, commit: true) }
                        )
                    }
                }

                addSlot(width: addSlotWidth)
            }
            // ONE contiguous drop target across the whole row — no dead zones (the
            // 8pt resize handles between charts were gaps that swallowed drops and left
            // the guide stuck). The insertion gap is derived from the cursor's x.
            .onDrop(of: [.text], delegate: RowDropDelegate(
                panelWidths: (0..<row.panels.count).map { panelsWidth * CGFloat(weights[$0] / total) },
                handleWidth: panelHandleWidth,
                setDragActive: { dragActive = $0 },
                setInsertion: setInsertion,
                move: { uuid, index in moveDroppedPanel(uuid, to: index) }
            ))
        }
    }

    /// A vertical accent bar marking the exact gap a dropped chart will land in.
    @ViewBuilder
    private func insertionBar(active: Bool) -> some View {
        if active {
            RoundedRectangle(cornerRadius: 2)
                .fill(Color.accentColor)
                .frame(width: 4)
                .padding(.vertical, 4)
                .shadow(color: .accentColor.opacity(0.7), radius: 3)
                .transition(.opacity)
        }
    }

    /// Perform a validated drop: move the dragged panel into this row at `index`.
    private func moveDroppedPanel(_ panelId: UUID, to index: Int) {
        activeDropSlot = nil
        guard let ds = dataSource else { return }
        viewModel.movePanel(panelId, toRow: rowIndex, at: index, dataSource: ds)
    }

    private func addSlot(width: CGFloat) -> some View {
        Button(action: onAddChart) {
            ZStack {
                RoundedRectangle(cornerRadius: 8)
                    .strokeBorder(style: StrokeStyle(lineWidth: 1, dash: [4]))
                    .foregroundStyle(.secondary.opacity(hoveringAddSlot ? 0.6 : 0.25))
                if hoveringAddSlot {
                    VStack(spacing: 4) {
                        Image(systemName: "plus")
                        Text("Add chart").font(.caption2)
                    }
                    .foregroundStyle(.secondary)
                } else {
                    Image(systemName: "plus")
                        .font(.caption2)
                        .foregroundStyle(.secondary.opacity(0.5))
                }
            }
            .frame(width: width)
            .contentShape(Rectangle())
        }
        .buttonStyle(.plain)
        .padding(.leading, 4)
        .onHover { hoveringAddSlot = $0 }
        .animation(.easeOut(duration: 0.12), value: hoveringAddSlot)
    }

    /// Shift weight between panel `i` and `i+1` by a pixel delta.
    private func ratioDrag(after i: Int, dx: CGFloat, panelsWidth: CGFloat, commit: Bool) {
        guard panelsWidth > 0, row.panels.indices.contains(i), row.panels.indices.contains(i + 1) else { return }
        let base = row.panels.map { $0.weight }
        let total = max(0.0001, base.reduce(0, +))
        // Convert pixel delta to a weight delta (weights are in the same units as widths
        // once scaled by total/panelsWidth).
        let deltaWeight = Double(dx) / Double(panelsWidth) * total
        var next = base
        let pairSum = base[i] + base[i + 1]
        next[i] = min(pairSum - 0.05, max(0.05, base[i] + deltaWeight))
        next[i + 1] = pairSum - next[i]

        if commit {
            dragWeights = nil
            if let ds = dataSource {
                viewModel.setPanelWeights(rowIndex, weights: next, persist: true, dataSource: ds)
            }
        } else {
            dragWeights = next
            if let ds = dataSource {
                viewModel.setPanelWeights(rowIndex, weights: next, persist: false, dataSource: ds)
            }
        }
    }

    private func updatePanelType(_ panelIndex: Int, _ newType: VizType?) {
        guard let ds = dataSource, row.panels.indices.contains(panelIndex) else { return }
        var viz = row.panels[panelIndex].visualization
        viz.shape = newType?.rawValue
        viewModel.updatePanel(row: rowIndex, panel: panelIndex, visualization: viz, dataSource: ds)
    }
}

// MARK: - Panel

private struct DashboardPanelView: View {
    let panel: RowPanel
    let dataSource: DataSource?
    let timeRange: TimeRange
    let isLive: Bool
    let onEdit: () -> Void
    let onRemove: () -> Void
    let onVizTypeChange: (VizType?) -> Void

    @State private var hovering = false

    private var vizTypeBinding: Binding<VizType?> {
        Binding(get: { panel.visualization.vizType }, set: { onVizTypeChange($0) })
    }

    var body: some View {
        VStack(spacing: 0) {
            HStack(spacing: 6) {
                HStack(spacing: 4) {
                    Image(systemName: "line.3.horizontal")
                        .font(.caption2)
                        .foregroundStyle(.secondary)
                        .opacity(hovering ? 1 : 0.4)
                    Text(panel.visualization.title)
                        .font(.caption.weight(.semibold))
                        .lineLimit(1)
                    // Fill the rest of the title bar (up to the controls) so the whole
                    // empty area is a drag handle, not just the text + grip.
                    Spacer(minLength: 0)
                }
                .contentShape(Rectangle())
                .onDrag {
                    NSItemProvider(object: panel.id.uuidString as NSString)
                } preview: {
                    Label(panel.visualization.title, systemImage: "chart.bar.doc.horizontal")
                        .font(.caption)
                        .padding(6)
                        .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 6))
                }
                .onHover { inside in
                    if inside { NSCursor.openHand.push() } else { NSCursor.pop() }
                }
                .help("Drag to reorder within the row or move to another row")
                if hovering {
                    VizTypePicker(selection: vizTypeBinding)
                    Button(action: onEdit) { Image(systemName: "pencil") }
                        .buttonStyle(.borderless).help("Edit chart")
                    Button(action: onRemove) { Image(systemName: "xmark.circle") }
                        .buttonStyle(.borderless).help("Remove chart")
                }
            }
            .font(.caption)
            .padding(.horizontal, 8)
            .padding(.vertical, 5)

            Divider()

            VisualizationView(
                visualization: panel.visualization,
                dataSource: dataSource,
                timeRange: timeRange,
                isLive: isLive,
                applyTimeRange: true,
                vizTypeOverride: panel.visualization.vizType
            )
        }
        .background(Color(nsColor: .controlBackgroundColor))
        .clipShape(RoundedRectangle(cornerRadius: 10))
        .overlay(
            RoundedRectangle(cornerRadius: 10)
                .stroke(Color.secondary.opacity(0.2), lineWidth: 1)
        )
        .padding(4)
        .onHover { hovering = $0 }
    }
}

// MARK: - Handles

/// A vertical bar dragged left/right to re-proportion two adjacent panels.
///
/// Its grip is normally invisible so the gap reads as plain margin between charts; it
/// surfaces only when a chart is being dragged (`revealed` — every boundary lights up as
/// a drop/resize area) or while this handle itself is hovered or dragged to resize.
private struct PanelRatioHandle: View {
    let width: CGFloat
    /// A chart drag is in progress — reveal all separators as drop/resize areas.
    let revealed: Bool
    let onChanged: (CGFloat) -> Void
    let onEnded: (CGFloat) -> Void

    @State private var isHovering = false
    @State private var isDragging = false

    private var showGrip: Bool { revealed || isHovering || isDragging }

    var body: some View {
        Rectangle()
            .fill(Color.secondary.opacity(0.001))
            .overlay(
                RoundedRectangle(cornerRadius: 1.5)
                    .fill(Color.secondary.opacity(0.35))
                    .frame(width: 3)
                    .opacity(showGrip ? 1 : 0)
            )
            .frame(width: width)
            .contentShape(Rectangle())
            .onHover { inside in
                isHovering = inside
                if inside { NSCursor.resizeLeftRight.push() } else { NSCursor.pop() }
            }
            .gesture(
                DragGesture()
                    .onChanged { isDragging = true; onChanged($0.translation.width) }
                    .onEnded { isDragging = false; onEnded($0.translation.width) }
            )
            .animation(.easeOut(duration: 0.12), value: showGrip)
    }
}

/// A horizontal bar dragged up/down to resize the row above it. Like ``PanelRatioHandle``
/// its grip is blank margin until a chart is dragged (`revealed`) or the handle is
/// hovered/dragged to resize.
private struct RowResizeHandle: View {
    let thickness: CGFloat
    /// A chart drag is in progress — reveal all separators as drop/resize areas.
    let revealed: Bool
    let onChanged: (CGFloat) -> Void
    let onEnded: (CGFloat) -> Void

    @State private var isHovering = false
    @State private var isDragging = false

    private var showGrip: Bool { revealed || isHovering || isDragging }

    var body: some View {
        Rectangle()
            .fill(Color.secondary.opacity(0.001))
            .overlay(
                RoundedRectangle(cornerRadius: 1.5)
                    .fill(Color.secondary.opacity(0.35))
                    .frame(height: 3)
                    .padding(.horizontal, 40)
                    .opacity(showGrip ? 1 : 0)
            )
            .frame(height: thickness)
            .contentShape(Rectangle())
            .onHover { inside in
                isHovering = inside
                if inside { NSCursor.resizeUpDown.push() } else { NSCursor.pop() }
            }
            .gesture(
                DragGesture()
                    .onChanged { isDragging = true; onChanged($0.translation.height) }
                    .onEnded { isDragging = false; onEnded($0.translation.height) }
            )
            .animation(.easeOut(duration: 0.12), value: showGrip)
    }
}

// MARK: - Drop

/// Drop handling for a whole dashboard row — one contiguous target (no dead zones), so
/// the insertion guide always clears on drop. `dropUpdated` gives the live cursor
/// location (which `.dropDestination`'s `isTargeted` can't), from which the insertion
/// gap is computed: the number of panel centers left of the cursor (0…panelCount).
private struct RowDropDelegate: DropDelegate {
    let panelWidths: [CGFloat]
    let handleWidth: CGFloat
    let setDragActive: (Bool) -> Void
    let setInsertion: (Int?) -> Void
    let move: (UUID, Int) -> Void

    /// Insertion index for a cursor at `x`: count of panels whose center is left of `x`.
    private func gap(at x: CGFloat) -> Int {
        var left: CGFloat = 0
        for (i, w) in panelWidths.enumerated() {
            if x < left + w / 2 { return i }
            left += w + handleWidth
        }
        return panelWidths.count
    }

    func validateDrop(info: DropInfo) -> Bool {
        info.hasItemsConforming(to: [.text])
    }

    func dropEntered(info: DropInfo) {
        setDragActive(true)
        setInsertion(gap(at: info.location.x))
    }

    func dropUpdated(info: DropInfo) -> DropProposal? {
        setInsertion(gap(at: info.location.x))
        return DropProposal(operation: .move)
    }

    func dropExited(info: DropInfo) { setInsertion(nil) }

    func performDrop(info: DropInfo) -> Bool {
        let index = gap(at: info.location.x)
        // End the drag first: the guide is gated on this, so the stray `dropUpdated`
        // SwiftUI fires right after can't bring the bar back.
        setDragActive(false)
        setInsertion(nil)
        guard let provider = info.itemProviders(for: [.text]).first else { return false }
        _ = provider.loadObject(ofClass: NSString.self) { value, _ in
            guard let string = value as? String, let uuid = UUID(uuidString: string) else { return }
            DispatchQueue.main.async { move(uuid, index) }
        }
        return true
    }
}
