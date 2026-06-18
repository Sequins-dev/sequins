//
//  FlamegraphFeed.swift
//  SequinsData
//
//  @Observable flamegraph model driven by entity deltas from a `FlamegraphStrategy` view.
//
//  The Rust FlamegraphStrategy emits:
//    EntityCreated:      descriptor (path_key, frame_id, function_name, system_name,
//                                   filename, line, depth, parent_path_key)
//                        + data (total_value, self_value)
//    EntityDataReplaced: key + data (total_value, self_value)
//    EntityRemoved:      key
//    Ready:              initial snapshot complete, live updates now streaming
//    Heartbeat:          periodic keepalive
//
//  applyViewDeltas(_:) accumulates entity state.
//  rebuildFromEntities() produces the flat [FlamegraphNode] array consumed by the view.
//

import Foundation
import Logging
import Tracing

// MARK: - FlamegraphNode

/// A single frozen node in the flamegraph tree, optimised for display.
public struct FlamegraphNode: Identifiable, Equatable {
    /// Path key from root: "frameId0/frameId1/.../frameIdK" (or "root" for synthetic root).
    public let id: String
    public let frameId: UInt64
    public let functionName: String
    public let systemName: String?
    public let filename: String?
    public let line: Int64?
    public let depth: Int
    public let selfValue: Int64
    public let totalValue: Int64
    public let parentId: String?
    public let childIds: [String]
    public let selfPercentage: Double
    public let totalPercentage: Double

    public init(
        id: String, frameId: UInt64, functionName: String,
        systemName: String?, filename: String?, line: Int64?,
        depth: Int, selfValue: Int64, totalValue: Int64,
        parentId: String?, childIds: [String],
        selfPercentage: Double, totalPercentage: Double
    ) {
        self.id = id
        self.frameId = frameId
        self.functionName = functionName
        self.systemName = systemName
        self.filename = filename
        self.line = line
        self.depth = depth
        self.selfValue = selfValue
        self.totalValue = totalValue
        self.parentId = parentId
        self.childIds = childIds
        self.selfPercentage = selfPercentage
        self.totalPercentage = totalPercentage
    }

    public static func == (lhs: FlamegraphNode, rhs: FlamegraphNode) -> Bool {
        lhs.id == rhs.id
    }
}

// MARK: - FlamegraphFeed

/// An `@Observable` flamegraph populated by entity deltas from a `FlamegraphStrategy` view.
///
/// Created fresh by `ProfilesViewModel` on each load. The view observes `nodes` directly;
/// it is replaced atomically by `rebuildFromEntities()` after each delta batch.
@Observable
public final class FlamegraphFeed {
    private static let logger = Logger(label: "sequins.flamegraph-feed")

    // ── Public state ──────────────────────────────────────────────────────────

    /// Flat node array, ordered by a BFS traversal of the tree.
    public private(set) var nodes: [FlamegraphNode] = []

    /// Fast lookup: nodeId → index in `nodes`.
    public private(set) var nodeIndex: [String: Int] = [:]

    /// The id of the synthetic root node ("root"), or nil before rebuild.
    public private(set) var rootNodeId: String? = nil

    /// Total value summed across all root-level contributions.
    public private(set) var totalValue: Int64 = 0

    /// True until markReady() or markFailed() is called.
    public private(set) var isLoading: Bool = true

    /// Distinct value types — populated from entity descriptors.
    public private(set) var availableValueTypes: [String] = []

    // ── Private entity state ──────────────────────────────────────────────────

    private final class EntityData {
        let id: String         // path_key
        let frameId: UInt64
        let functionName: String
        let systemName: String?
        let filename: String?
        let line: Int64?
        let depth: Int
        let parentId: String?  // parent path_key, or nil for root-level nodes
        var totalValue: Int64
        var selfValue: Int64

        init(
            id: String, frameId: UInt64, functionName: String,
            systemName: String?, filename: String?, line: Int64?,
            depth: Int, parentId: String?,
            totalValue: Int64, selfValue: Int64
        ) {
            self.id = id
            self.frameId = frameId
            self.functionName = functionName
            self.systemName = systemName
            self.filename = filename
            self.line = line
            self.depth = depth
            self.parentId = parentId
            self.totalValue = totalValue
            self.selfValue = selfValue
        }
    }

    private var entities: [String: EntityData] = [:]

    public init() {}

    // MARK: - Lifecycle

    public func markReady() { isLoading = false }
    public func markFailed() { isLoading = false }

    // MARK: - Entity delta application

    /// Apply a batch of view deltas from the FlamegraphStrategy.
    public func applyViewDeltas(_ deltas: [ViewDelta]) {
        withSpan("FlamegraphFeed.applyViewDeltas") { _ in }
        var didChange = false

        for delta in deltas {
            switch delta.type {
            case .entityCreated:
                guard let descBatch = delta.descriptor.first,
                      let dataBatch = delta.data.first,
                      let key = delta.key
                else { continue }

                // Descriptor schema: path_key(0), frame_id(1), function_name(2),
                //                    system_name(3), filename(4), line(5),
                //                    depth(6), parent_path_key(7)
                let descRows = descBatch.toRows()
                guard let row = descRows.first else { continue }
                guard row.count >= 8 else { continue }

                let frameId: UInt64
                if let n = row[1] as? UInt64 { frameId = n }
                else if let n = row[1] as? NSNumber { frameId = n.uint64Value }
                else { continue }

                let functionName = row[2] as? String ?? "???"
                let systemName = row[3] as? String
                let filename = row[4] as? String
                let line: Int64? = (row[5] as? NSNumber)?.int64Value

                let depth: Int
                if let n = row[6] as? UInt32 { depth = Int(n) }
                else if let n = row[6] as? NSNumber { depth = n.intValue }
                else { depth = 0 }

                let parentId = row[7] as? String

                // Data schema: total_value(0), self_value(1)
                let (totalValue, selfValue) = extractDataValues(dataBatch)

                let entity = EntityData(
                    id: key,
                    frameId: frameId,
                    functionName: functionName,
                    systemName: systemName,
                    filename: filename,
                    line: line,
                    depth: depth,
                    parentId: parentId,
                    totalValue: totalValue,
                    selfValue: selfValue
                )
                entities[key] = entity
                didChange = true

            case .entityDataReplaced:
                guard let key = delta.key,
                      let entity = entities[key],
                      let dataBatch = delta.data.first
                else { continue }

                let (totalValue, selfValue) = extractDataValues(dataBatch)
                entity.totalValue = totalValue
                entity.selfValue = selfValue
                didChange = true

            case .entityRemoved:
                guard let key = delta.key else { continue }
                entities.removeValue(forKey: key)
                didChange = true

            case .ready:
                markReady()

            case .heartbeat:
                // No state change needed; rebuild happens below if didChange.
                break

            case .error:
                markFailed()

            default:
                break
            }
        }

        if didChange {
            rebuildFromEntities()
        }
    }

    // MARK: - Tree reconstruction

    /// Build `nodes` from the current `entities` dictionary.
    /// Produces the same `[FlamegraphNode]` output as before but from entity state.
    private func rebuildFromEntities() {
        withSpan("FlamegraphFeed.rebuild") { _ in }
        guard !entities.isEmpty else {
            nodes = []
            nodeIndex = [:]
            rootNodeId = nil
            totalValue = 0
            return
        }

        // Compute grand total: sum of total_value for root-level nodes (depth == 0)
        let grandTotal: Int64 = entities.values.reduce(0) { acc, e in
            e.depth == 0 ? acc + e.totalValue : acc
        }
        guard grandTotal > 0 else { return }

        // Build child lists
        var childIds: [String: [String]] = [:]  // parentId → [childId]
        var rootIds: [String] = []

        for entity in entities.values {
            if let pid = entity.parentId {
                childIds[pid, default: []].append(entity.id)
            } else {
                rootIds.append(entity.id)
            }
        }

        // Sort children by descending total_value
        for key in childIds.keys {
            childIds[key]!.sort { a, b in
                (entities[a]?.totalValue ?? 0) > (entities[b]?.totalValue ?? 0)
            }
        }
        rootIds.sort { a, b in
            (entities[a]?.totalValue ?? 0) > (entities[b]?.totalValue ?? 0)
        }

        var result: [FlamegraphNode] = []
        var index: [String: Int] = [:]

        func visit(_ entityId: String) {
            guard let entity = entities[entityId] else { return }
            let kids = childIds[entityId] ?? []
            let selfPct = Double(entity.selfValue) / Double(grandTotal) * 100
            let totalPct = Double(entity.totalValue) / Double(grandTotal) * 100
            let frozen = FlamegraphNode(
                id: entity.id,
                frameId: entity.frameId,
                functionName: entity.functionName,
                systemName: entity.systemName,
                filename: entity.filename,
                line: entity.line,
                depth: entity.depth,
                selfValue: entity.selfValue,
                totalValue: entity.totalValue,
                parentId: entity.parentId,
                childIds: kids,
                selfPercentage: selfPct,
                totalPercentage: totalPct
            )
            index[entity.id] = result.count
            result.append(frozen)
            for childId in kids {
                visit(childId)
            }
        }

        for rootId in rootIds {
            visit(rootId)
        }

        nodes = result
        nodeIndex = index
        rootNodeId = rootIds.first
        totalValue = grandTotal
    }

    // MARK: - Helpers

    private func extractDataValues(_ batch: RecordBatch) -> (totalValue: Int64, selfValue: Int64) {
        let rows = batch.toRows()
        guard let row = rows.first, row.count >= 2 else { return (0, 0) }
        let total = (row[0] as? NSNumber)?.int64Value ?? 0
        let self_ = (row[1] as? NSNumber)?.int64Value ?? 0
        return (total, self_)
    }
}
