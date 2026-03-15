import Foundation
import SequinsFFI

/// Retention policy configuration
public struct RetentionPolicy {
    /// How long to keep span data (seconds)
    public let spansRetention: Int64

    /// How long to keep log data (seconds)
    public let logsRetention: Int64

    /// How long to keep metric data (seconds)
    public let metricsRetention: Int64

    /// How long to keep profile data (seconds)
    public let profilesRetention: Int64

    public init(
        spansRetention: Int64,
        logsRetention: Int64,
        metricsRetention: Int64,
        profilesRetention: Int64
    ) {
        self.spansRetention = spansRetention
        self.logsRetention = logsRetention
        self.metricsRetention = metricsRetention
        self.profilesRetention = profilesRetention
    }

    /// Convert to C struct
    func toCStruct() -> CRetentionPolicy {
        return CRetentionPolicy(
            spans_retention_secs: spansRetention,
            logs_retention_secs: logsRetention,
            metrics_retention_secs: metricsRetention,
            profiles_retention_secs: profilesRetention
        )
    }

    /// Convert from C struct
    init(cPolicy: CRetentionPolicy) {
        self.spansRetention = cPolicy.spans_retention_secs
        self.logsRetention = cPolicy.logs_retention_secs
        self.metricsRetention = cPolicy.metrics_retention_secs
        self.profilesRetention = cPolicy.profiles_retention_secs
    }
}

/// Storage statistics
public struct StorageStats {
    /// Number of spans in storage
    public let spanCount: UInt

    /// Number of logs in storage
    public let logCount: UInt

    /// Number of metrics in storage
    public let metricCount: UInt

    /// Number of profiles in storage
    public let profileCount: UInt

    public init(
        spanCount: UInt,
        logCount: UInt,
        metricCount: UInt,
        profileCount: UInt
    ) {
        self.spanCount = spanCount
        self.logCount = logCount
        self.metricCount = metricCount
        self.profileCount = profileCount
    }

    /// Convert from C struct
    init(cStats: CStorageStats) {
        self.spanCount = cStats.span_count
        self.logCount = cStats.log_count
        self.metricCount = cStats.metric_count
        self.profileCount = cStats.profile_count
    }
}

/// Maintenance statistics
public struct MaintenanceStats {
    /// Number of entries evicted from hot tier
    public let entriesEvicted: UInt

    /// Number of batches flushed to cold tier
    public let batchesFlushed: UInt

    public init(
        entriesEvicted: UInt,
        batchesFlushed: UInt
    ) {
        self.entriesEvicted = entriesEvicted
        self.batchesFlushed = batchesFlushed
    }

    /// Convert from C struct
    init(cStats: CMaintenanceStats) {
        self.entriesEvicted = cStats.entries_evicted
        self.batchesFlushed = cStats.batches_flushed
    }
}

// MARK: - DataSource Management Extension

extension DataSource {
    /// Update retention policy
    ///
    /// - Parameter policy: New retention policy
    /// - Throws: SequinsError if operation fails
    public func updateRetentionPolicy(_ policy: RetentionPolicy) throws {
        var errorPtr: UnsafeMutablePointer<CChar>? = nil
        let cPolicy = policy.toCStruct()

        let success = sequins_management_update_retention_policy(
            rawPointer,
            cPolicy,
            &errorPtr
        )

        if !success {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.nullPointer
        }
    }

    /// Get current retention policy
    ///
    /// - Returns: Current retention policy
    /// - Throws: SequinsError if operation fails
    public func getRetentionPolicy() throws -> RetentionPolicy {
        var errorPtr: UnsafeMutablePointer<CChar>? = nil
        var cPolicy = CRetentionPolicy(
            spans_retention_secs: 0,
            logs_retention_secs: 0,
            metrics_retention_secs: 0,
            profiles_retention_secs: 0
        )

        let success = sequins_management_get_retention_policy(
            rawPointer,
            &cPolicy,
            &errorPtr
        )

        if !success {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.nullPointer
        }

        return RetentionPolicy(cPolicy: cPolicy)
    }

    /// Get storage statistics
    ///
    /// - Returns: Current storage statistics
    /// - Throws: SequinsError if operation fails
    public func getStorageStats() throws -> StorageStats {
        var errorPtr: UnsafeMutablePointer<CChar>? = nil
        var cStats = CStorageStats(
            span_count: 0,
            log_count: 0,
            metric_count: 0,
            profile_count: 0
        )

        let success = sequins_management_get_storage_stats(
            rawPointer,
            &cStats,
            &errorPtr
        )

        if !success {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.nullPointer
        }

        return StorageStats(cStats: cStats)
    }

    /// Run retention cleanup to delete old data
    ///
    /// - Returns: Number of entries deleted
    /// - Throws: SequinsError if operation fails
    public func runRetentionCleanup() throws -> UInt {
        var errorPtr: UnsafeMutablePointer<CChar>? = nil
        var deletedCount: UInt = 0

        let success = sequins_management_run_retention_cleanup(
            rawPointer,
            &deletedCount,
            &errorPtr
        )

        if !success {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.nullPointer
        }

        return deletedCount
    }

    /// Run database maintenance (compaction and optimization)
    ///
    /// - Returns: Maintenance statistics
    /// - Throws: SequinsError if operation fails
    public func runMaintenance() throws -> MaintenanceStats {
        var errorPtr: UnsafeMutablePointer<CChar>? = nil
        var cStats = CMaintenanceStats(
            entries_evicted: 0,
            batches_flushed: 0
        )

        let success = sequins_management_run_maintenance(
            rawPointer,
            &cStats,
            &errorPtr
        )

        if !success {
            if let errorPtr = errorPtr {
                let errorMessage = String(cString: errorPtr)
                sequins_string_free(errorPtr)
                throw SequinsError.ffiError(errorMessage)
            }
            throw SequinsError.nullPointer
        }

        return MaintenanceStats(cStats: cStats)
    }
}
