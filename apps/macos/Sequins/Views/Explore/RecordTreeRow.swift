import SwiftUI
import SequinsData

/// Recursive row renderer for a single `RecordNode`.
///
/// Depth-0 nodes are top-level records (rows); they get a subtle background.
/// Non-leaf nodes show a chevron toggle for expand/collapse.
/// Leaf nodes display their formatted value with type-appropriate coloring.
struct RecordTreeRow: View {
    let node: RecordNode
    var depth: Int = 0

    @State private var isExpanded: Bool = false

    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            rowContent
            if isExpanded && !node.isLeaf {
                ForEach(node.children) { child in
                    RecordTreeRow(node: child, depth: depth + 1)
                }
            }
        }
    }

    // MARK: - Row header

    private var rowContent: some View {
        HStack(spacing: 4) {
            // Indentation
            if depth > 0 {
                Spacer()
                    .frame(width: CGFloat(depth) * 16)
            }

            // Expand/collapse chevron for non-leaf nodes
            if !node.isLeaf {
                Button(action: { withAnimation(.easeInOut(duration: 0.15)) { isExpanded.toggle() } }) {
                    Image(systemName: isExpanded ? "chevron.down" : "chevron.right")
                        .font(.system(size: 10, weight: .semibold))
                        .foregroundStyle(.secondary)
                        .frame(width: 12)
                }
                .buttonStyle(.plain)
            } else {
                Spacer().frame(width: 16)
            }

            // Field name
            Text(node.name)
                .font(.system(size: 12, design: .monospaced))
                .foregroundStyle(depth == 0 ? .primary : .secondary)
                .lineLimit(1)

            Spacer()

            // Value or container badge
            if node.isLeaf {
                leafValueView
            } else {
                containerBadge
            }
        }
        .padding(.horizontal, 10)
        .padding(.vertical, depth == 0 ? 5 : 3)
        .background(rowBackground)
        .contentShape(Rectangle())
        .onTapGesture {
            guard !node.isLeaf else { return }
            withAnimation(.easeInOut(duration: 0.15)) { isExpanded.toggle() }
        }
    }

    // MARK: - Leaf value

    @ViewBuilder
    private var leafValueView: some View {
        if let value = node.displayValue {
            if node.typeLabel == .id {
                CopyableCell(text: value)
                    .font(.system(size: 11, design: .monospaced))
                    .foregroundStyle(typeColor)
                    .lineLimit(1)
            } else {
                Text(value)
                    .font(.system(size: 11, design: node.typeLabel == .null ? .default : .monospaced))
                    .foregroundStyle(typeColor)
                    .lineLimit(1)
                    .truncationMode(.tail)
            }
        }
    }

    // MARK: - Container badge

    private var containerBadge: some View {
        Text(containerSuffix)
            .font(.system(size: 10))
            .foregroundStyle(.purple)
            .padding(.horizontal, 5)
            .padding(.vertical, 2)
            .background(Color.purple.opacity(0.1))
            .clipShape(RoundedRectangle(cornerRadius: 4))
    }

    private var containerSuffix: String {
        switch node.typeLabel {
        case .list:
            return "[\(node.children.count)]"
        case .map:
            return "{\(node.children.count) entries}"
        case .structType:
            if depth == 0 {
                return node.summary ?? ""
            }
            return "{\(node.children.count) fields}"
        default:
            return "[\(node.children.count)]"
        }
    }

    // MARK: - Styling

    private var typeColor: Color {
        switch node.typeLabel {
        case .string:    return .green
        case .number:    return .teal
        case .boolean:   return .orange
        case .timestamp: return .cyan
        case .duration:  return .indigo
        case .id:        return .blue
        case .binary:    return .gray
        case .null:      return .secondary
        case .list, .map, .structType: return .purple
        case .unknown:   return .primary
        }
    }

    private var rowBackground: some View {
        Group {
            if depth == 0 {
                Color(nsColor: .controlBackgroundColor).opacity(0.5)
            } else {
                Color.clear
            }
        }
    }
}
