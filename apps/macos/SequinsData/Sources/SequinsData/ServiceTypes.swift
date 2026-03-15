import Foundation
import SequinsFFI

/// A resource attribute with a key and multiple values (aggregated across service instances)
public struct ResourceAttribute: Identifiable, Hashable, Sendable {
    public let id: String
    public let key: String
    public let values: [String]

    public init(key: String, values: [String]) {
        self.id = key
        self.key = key
        self.values = values
    }

    /// Display value as comma-separated string
    public var displayValue: String {
        values.joined(separator: ", ")
    }
}

/// Represents a discovered service from telemetry data
public struct Service: Identifiable, Hashable, Sendable {
    /// Unique identifier (same as name)
    public let id: String

    /// Service name
    public let name: String

    /// Number of spans received from this service
    public let spanCount: Int

    /// Number of logs received from this service
    public let logCount: Int

    /// Resource attributes aggregated from all service instances
    public let resourceAttributes: [ResourceAttribute]

    /// Resource IDs associated with this service (for filtering spans/logs/profiles by resource_id)
    public let resourceIds: [UInt32]

    public init(name: String, spanCount: Int, logCount: Int, resourceAttributes: [ResourceAttribute] = [], resourceIds: [UInt32] = []) {
        self.id = name
        self.name = name
        self.spanCount = spanCount
        self.logCount = logCount
        self.resourceAttributes = resourceAttributes
        self.resourceIds = resourceIds
    }

    /// Initialize from C struct
    init(cService: CService) {
        self.id = String(cString: cService.name)
        self.name = String(cString: cService.name)
        self.spanCount = Int(cService.span_count)
        self.logCount = Int(cService.log_count)

        // Convert resource attributes from C array
        var attrs: [ResourceAttribute] = []
        if cService.resource_attributes.len > 0 && cService.resource_attributes.data != nil {
            for i in 0..<cService.resource_attributes.len {
                let cAttr = cService.resource_attributes.data.advanced(by: Int(i)).pointee
                let key = String(cString: cAttr.key)
                let valuesStr = String(cString: cAttr.values)
                let values = valuesStr.split(separator: ",").map { String($0).trimmingCharacters(in: CharacterSet.whitespaces) }
                attrs.append(ResourceAttribute(key: key, values: values))
            }
        }
        self.resourceAttributes = attrs
        self.resourceIds = []
    }

    // MARK: - Convenience Accessors for Prominent Attributes

    /// Service version (service.version)
    public var version: String? {
        resourceAttributes.first { $0.key == "service.version" }?.displayValue
    }

    /// Deployment environment (deployment.environment)
    public var environment: String? {
        resourceAttributes.first { $0.key == "deployment.environment" }?.displayValue
    }

    /// Service namespace (service.namespace)
    public var namespace: String? {
        resourceAttributes.first { $0.key == "service.namespace" }?.displayValue
    }

    /// Prominent attributes that should always be displayed (environment, namespace - not version)
    public var prominentAttributes: [ResourceAttribute] {
        let prominentKeys = Set(["deployment.environment", "service.namespace"])
        return resourceAttributes.filter { prominentKeys.contains($0.key) }
    }

    /// Other attributes (non-prominent) for expandable section
    public var otherAttributes: [ResourceAttribute] {
        let prominentKeys = Set(["deployment.environment", "service.namespace"])
        return resourceAttributes.filter { !prominentKeys.contains($0.key) }
    }
}
