import Foundation
import SequinsFFI

/// Configuration for the embedded OTLP server
public struct OTLPServerConfig {
    /// gRPC port (0 = disabled, default 4317)
    public let grpcPort: UInt16

    /// HTTP port (0 = disabled, default 4318)
    public let httpPort: UInt16

    public init(grpcPort: UInt16 = 4317, httpPort: UInt16 = 4318) {
        self.grpcPort = grpcPort
        self.httpPort = httpPort
    }

    func toCStruct() -> COtlpServerConfig {
        return COtlpServerConfig(grpc_port: grpcPort, http_port: httpPort)
    }
}

/// Data source for Sequins telemetry data
///
/// Supports both local (embedded storage) and remote (client to sequins-daemon) modes.
public final class DataSource {
    private let pointer: OpaquePointer

    private init(pointer: OpaquePointer) {
        self.pointer = pointer
    }

    deinit {
        sequins_data_source_free(pointer)
    }

    /// Create a local data source with embedded storage
    ///
    /// - Parameters:
    ///   - dbPath: Path to SQLite database file
    ///   - config: OTLP server configuration
    /// - Returns: A new local DataSource
    /// - Throws: SequinsError if creation fails
    public static func local(dbPath: String, config: OTLPServerConfig) throws -> DataSource {
        var errorPtr: UnsafeMutablePointer<CChar>? = nil
        let cConfig = config.toCStruct()

        guard let pointer = sequins_data_source_new_local(dbPath, cConfig, &errorPtr) else {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.nullPointer
        }

        // Start the OTLP server
        errorPtr = nil
        let started = sequins_data_source_start_otlp_server(pointer, cConfig, &errorPtr)
        if !started {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                sequins_data_source_free(pointer) // Clean up
                throw SequinsError.ffiError("Failed to start OTLP server: \(errorMessage)")
            }
            sequins_data_source_free(pointer) // Clean up
            throw SequinsError.ffiError("Failed to start OTLP server")
        }

        return DataSource(pointer: pointer)
    }

    /// Create a remote data source (connects to sequins-daemon)
    ///
    /// - Parameters:
    ///   - queryURL: URL for query API endpoint
    ///   - managementURL: URL for management API endpoint
    /// - Returns: A new remote DataSource
    /// - Throws: SequinsError if creation fails
    public static func remote(queryURL: String, managementURL: String) throws -> DataSource {
        var errorPtr: UnsafeMutablePointer<CChar>? = nil

        guard let pointer = sequins_data_source_new_remote(queryURL, managementURL, &errorPtr) else {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.nullPointer
        }

        return DataSource(pointer: pointer)
    }

    // Internal accessor for other components
    internal var rawPointer: OpaquePointer {
        return pointer
    }

    // MARK: - Health Configuration

    /// Get the current health threshold configuration
    ///
    /// - Returns: The current health threshold configuration
    /// - Throws: SequinsError if the operation fails
    public func getHealthThresholdConfig() throws -> HealthThresholdConfig {
        var errorPtr: UnsafeMutablePointer<CChar>?
        var cConfig = CHealthThresholdConfig()

        let success = sequins_management_get_health_threshold_config(pointer, &cConfig, &errorPtr)
        if !success {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.ffiError("Failed to get health threshold config")
        }

        defer { sequins_health_threshold_config_free(cConfig) }

        // Convert C config to Swift
        var rules: [HealthMetricRule] = []
        for i in 0..<cConfig.rules.len {
            guard let rulePtr = cConfig.rules.data?.advanced(by: Int(i)) else { continue }
            let cRule = rulePtr.pointee

            let metricName = cRule.metric_name.map { String(cString: $0) } ?? ""
            let serviceName = cRule.service_name.map { String(cString: $0) }
            let displayName = cRule.display_name.map { String(cString: $0) } ?? ""

            rules.append(HealthMetricRule(
                metricName: metricName,
                serviceName: serviceName,
                warningThreshold: cRule.warning_threshold,
                errorThreshold: cRule.error_threshold,
                higherIsWorse: cRule.higher_is_worse,
                weight: cRule.weight,
                displayName: displayName
            ))
        }

        return HealthThresholdConfig(rules: rules)
    }

    /// Set the health threshold configuration
    ///
    /// - Parameter config: The new health threshold configuration
    /// - Throws: SequinsError if the operation fails
    public func setHealthThresholdConfig(_ config: HealthThresholdConfig) throws {
        var errorPtr: UnsafeMutablePointer<CChar>?

        // Convert Swift config to C
        var cRules = config.rules.map { rule -> CHealthMetricRule in
            CHealthMetricRule(
                metric_name: strdup(rule.metricName),
                service_name: rule.serviceName.map { strdup($0) },
                warning_threshold: rule.warningThreshold,
                error_threshold: rule.errorThreshold,
                higher_is_worse: rule.higherIsWorse,
                weight: rule.weight,
                display_name: strdup(rule.displayName)
            )
        }

        let success = cRules.withUnsafeMutableBufferPointer { buffer -> Bool in
            var cConfig = CHealthThresholdConfig(
                rules: CHealthMetricRuleArray(data: buffer.baseAddress, len: UInt(config.rules.count))
            )
            return sequins_management_set_health_threshold_config(pointer, &cConfig, &errorPtr)
        }

        // Free the allocated strings
        for i in 0..<cRules.count {
            free(cRules[i].metric_name)
            if let serviceName = cRules[i].service_name {
                free(serviceName)
            }
            free(cRules[i].display_name)
        }

        if !success {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.ffiError("Failed to set health threshold config")
        }
    }

    /// Add a health metric rule
    ///
    /// If a rule with the same metric name and service name already exists, it is replaced.
    ///
    /// - Parameter rule: The health metric rule to add
    /// - Throws: SequinsError if the operation fails
    public func addHealthRule(_ rule: HealthMetricRule) throws {
        var errorPtr: UnsafeMutablePointer<CChar>?

        var cRule = CHealthMetricRule(
            metric_name: strdup(rule.metricName),
            service_name: rule.serviceName.map { strdup($0) },
            warning_threshold: rule.warningThreshold,
            error_threshold: rule.errorThreshold,
            higher_is_worse: rule.higherIsWorse,
            weight: rule.weight,
            display_name: strdup(rule.displayName)
        )

        let success = sequins_management_add_health_rule(pointer, &cRule, &errorPtr)

        // Free the allocated strings
        free(cRule.metric_name)
        if let serviceName = cRule.service_name {
            free(serviceName)
        }
        free(cRule.display_name)

        if !success {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.ffiError("Failed to add health rule")
        }
    }

    /// Remove a health metric rule
    ///
    /// - Parameters:
    ///   - metricName: The metric name of the rule to remove
    ///   - serviceName: The service name filter (nil = all services)
    /// - Throws: SequinsError if the operation fails
    public func removeHealthRule(metricName: String, serviceName: String? = nil) throws {
        var errorPtr: UnsafeMutablePointer<CChar>?

        let success = sequins_management_remove_health_rule(
            pointer,
            metricName,
            serviceName,
            &errorPtr
        )

        if !success {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.ffiError("Failed to remove health rule")
        }
    }

    // MARK: - Test Data Generation

    /// Generate synthetic test data for development/debugging
    ///
    /// Creates test traces with nested spans in the hot tier that can be immediately queried.
    /// This is useful for testing the UI without needing external data generators.
    ///
    /// - Returns: The number of spans created
    /// - Throws: SequinsError if the operation fails
    public func generateTestData() throws -> Int {
        var errorPtr: UnsafeMutablePointer<CChar>?

        let count = sequins_data_source_generate_test_data(pointer, &errorPtr)

        if count == 0 {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.ffiError("Failed to generate test data")
        }

        return Int(count)
    }
}
