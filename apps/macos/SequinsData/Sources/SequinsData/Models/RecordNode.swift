import Foundation

/// Semantic type hint for display coloring and formatting
public enum NodeTypeLabel {
    case string
    case number
    case boolean
    case timestamp
    case duration
    case binary
    case null
    case list
    case map
    case structType
    case id
    case unknown
}

/// A node in a tree representation of a single Arrow record row.
///
/// Top-level nodes represent rows; their children are the column fields.
/// Nested Lists, Structs, and Maps produce their own subtrees.
public struct RecordNode: Identifiable {
    /// Path-based stable ID, e.g. "0/span_id" or "0/logs/2/body"
    public let id: String

    /// Display name: column name, struct field name, or "[N]" for list elements
    public let name: String

    /// Semantic type for color coding
    public let typeLabel: NodeTypeLabel

    /// Formatted leaf value; nil for container nodes (list, struct, map)
    public let displayValue: String?

    /// Child nodes for container types
    public let children: [RecordNode]

    /// Summary line shown in collapsed record header
    public let summary: String?

    public var isLeaf: Bool { children.isEmpty }

    public init(
        id: String,
        name: String,
        typeLabel: NodeTypeLabel,
        displayValue: String? = nil,
        children: [RecordNode] = [],
        summary: String? = nil
    ) {
        self.id = id
        self.name = name
        self.typeLabel = typeLabel
        self.displayValue = displayValue
        self.children = children
        self.summary = summary
    }
}
