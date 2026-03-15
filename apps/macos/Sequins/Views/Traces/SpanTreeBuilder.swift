import Foundation
import SequinsData

/// Represents a span with its tree depth and position info
struct SpanTreeNode {
    let span: Span
    let depth: Int
    let isLastChild: Bool
    let ancestorIsLast: [Bool] // Track which ancestors are last children (for tree lines)
}

/// Builds a tree structure from flat span list based on parent-child relationships
enum SpanTreeBuilder {
    /// Build tree nodes from spans, sorted by start time within each level
    static func buildTree(from spans: [Span]) -> [SpanTreeNode] {
        guard !spans.isEmpty else { return [] }

        // Create lookup by spanId, handling potential duplicates by keeping the first occurrence
        let spanById = Dictionary(spans.map { ($0.spanId, $0) }, uniquingKeysWith: { first, _ in first })

        // Group children by parent
        var childrenByParent: [String: [Span]] = [:]
        var rootSpans: [Span] = []

        for span in spans {
            if let parentId = span.parentSpanId, spanById[parentId] != nil {
                childrenByParent[parentId, default: []].append(span)
            } else {
                rootSpans.append(span)
            }
        }

        // Sort all groups by start time
        rootSpans.sort { $0.startTime < $1.startTime }
        for key in childrenByParent.keys {
            childrenByParent[key]?.sort { $0.startTime < $1.startTime }
        }

        // Build tree recursively
        var result: [SpanTreeNode] = []

        func addSpan(_ span: Span, depth: Int, isLast: Bool, ancestorIsLast: [Bool]) {
            let node = SpanTreeNode(
                span: span,
                depth: depth,
                isLastChild: isLast,
                ancestorIsLast: ancestorIsLast
            )
            result.append(node)

            let children = childrenByParent[span.spanId] ?? []
            for (index, child) in children.enumerated() {
                let childIsLast = index == children.count - 1
                var newAncestorIsLast = ancestorIsLast
                newAncestorIsLast.append(isLast)
                addSpan(child, depth: depth + 1, isLast: childIsLast, ancestorIsLast: newAncestorIsLast)
            }
        }

        for (index, rootSpan) in rootSpans.enumerated() {
            addSpan(rootSpan, depth: 0, isLast: index == rootSpans.count - 1, ancestorIsLast: [])
        }

        return result
    }
}
