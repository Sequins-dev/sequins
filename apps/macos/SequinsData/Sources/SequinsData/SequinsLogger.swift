import Foundation
import Logging

// MARK: - File Log Handler

/// A `LogHandler` that appends structured log lines to `~/Library/Logs/Sequins/swift.log`.
///
/// Thread safety is maintained via a serial `DispatchQueue`. Log lines include a
/// timestamp, severity level, source label, and message.
public struct FileLogHandler: LogHandler {
    public var logLevel: Logger.Level = .debug
    public var metadata: Logger.Metadata = [:]

    private let label: String
    private static let queue = DispatchQueue(label: "sequins.file-log-handler", qos: .utility)
    private static var fileHandle: FileHandle? = {
        let logDir = resolveLogDir()
        let logPath = logDir.appendingPathComponent("swift.log")
        let fm = FileManager.default
        // Ensure directory exists
        try? fm.createDirectory(at: logDir, withIntermediateDirectories: true)
        // Create file if missing
        if !fm.fileExists(atPath: logPath.path) {
            fm.createFile(atPath: logPath.path, contents: nil)
        }
        return FileHandle(forWritingAtPath: logPath.path)
    }()

    public init(label: String) {
        self.label = label
    }

    public subscript(metadataKey key: String) -> Logger.Metadata.Value? {
        get { metadata[key] }
        set { metadata[key] = newValue }
    }

    public func log(
        level: Logger.Level,
        message: Logger.Message,
        metadata: Logger.Metadata?,
        source: String,
        file: String,
        function: String,
        line: UInt
    ) {
        let merged = self.metadata.merging(metadata ?? [:]) { _, new in new }
        let metaStr = merged.isEmpty ? "" : " \(merged.map { "\($0.key)=\($0.value)" }.joined(separator: " "))"
        let timestamp = ISO8601DateFormatter().string(from: Date())
        let line = "\(timestamp) [\(level)] \(label): \(message)\(metaStr)\n"
        guard let data = line.data(using: .utf8) else { return }

        FileLogHandler.queue.async {
            FileLogHandler.fileHandle?.seekToEndOfFile()
            FileLogHandler.fileHandle?.write(data)
        }
    }

    private static func resolveLogDir() -> URL {
        if let home = ProcessInfo.processInfo.environment["HOME"] {
            return URL(fileURLWithPath: home)
                .appendingPathComponent("Library/Logs/Sequins")
        }
        return URL(fileURLWithPath: "/tmp/sequins-logs")
    }
}

// MARK: - Bootstrap

/// Call once on app startup to direct all swift-log output to the Sequins log file.
///
/// This bootstraps the global `LoggingSystem` with `FileLogHandler`. After this call,
/// any `Logger(label:)` instance will write to `~/Library/Logs/Sequins/swift.log`.
///
/// Safe to call multiple times — `LoggingSystem.bootstrap` is idempotent after the first call.
public enum SequinsLogging {
    public static func bootstrap() {
        LoggingSystem.bootstrap(FileLogHandler.init)
    }
}
