import Foundation
import Logging
import Metrics
import OTel
import OTLPHTTPExporter

/// Configuration for the Sequins OpenTelemetry SDK.
public struct SequinsConfig {
    /// The name of your service. Defaults to `OTEL_SERVICE_NAME` env var, or `"unknown_service"`.
    public var serviceName: String?
    /// The OTLP HTTP endpoint. Defaults to `OTEL_EXPORTER_OTLP_ENDPOINT` or `"http://localhost:4318"`.
    public var endpoint: String?

    public init(serviceName: String? = nil, endpoint: String? = nil) {
        self.serviceName = serviceName
        self.endpoint = endpoint
    }
}

/// Handle returned by `Sequins.initialize(...)`. Provides access to all configured OTel providers.
public final class SequinsHandle {
    /// The configured tracer provider. Use `tracerProvider.tracer(name:)` for manual instrumentation.
    public let tracerProvider: OTelTracerProvider
    /// Call this before your application exits to flush pending telemetry.
    public let shutdown: () -> Void

    init(tracerProvider: OTelTracerProvider, shutdown: @escaping () -> Void) {
        self.tracerProvider = tracerProvider
        self.shutdown = shutdown
    }
}

/// Sequins OpenTelemetry SDK — zero-config observability for local development.
///
/// Configures OpenTelemetry traces to export to a local Sequins instance via OTLP/HTTP.
///
/// ```swift
/// import SequinsOtel
///
/// let sequins = try Sequins.initialize(serviceName: "my-app")
/// let tracer = sequins.tracerProvider.tracer(name: "my-module")
///
/// // On exit:
/// sequins.shutdown()
/// ```
public enum Sequins {
    private static let defaultEndpoint = "http://localhost:4318"

    /// Initializes OpenTelemetry pre-configured for Sequins.
    ///
    /// - Parameter serviceName: The name of your service.
    /// - Parameter endpoint: The OTLP HTTP endpoint. Defaults to `OTEL_EXPORTER_OTLP_ENDPOINT` or `http://localhost:4318`.
    /// - Returns: A `SequinsHandle` with the configured provider and shutdown function.
    public static func initialize(
        serviceName: String? = nil,
        config: SequinsConfig = SequinsConfig()
    ) throws -> SequinsHandle {
        let resolvedServiceName = config.serviceName
            ?? serviceName
            ?? ProcessInfo.processInfo.environment["OTEL_SERVICE_NAME"]
            ?? "unknown_service"

        let resolvedEndpoint = config.endpoint
            ?? ProcessInfo.processInfo.environment["OTEL_EXPORTER_OTLP_ENDPOINT"]
            ?? defaultEndpoint

        guard let endpointURL = URL(string: resolvedEndpoint) else {
            throw SequinsError.invalidEndpoint(resolvedEndpoint)
        }

        let resource = OTelResource(attributes: [
            "service.name": .string(resolvedServiceName),
        ])

        let exporter = try OTLPHTTPSpanExporter(
            endpoint: endpointURL.appendingPathComponent("v1/traces"),
            headers: ["Content-Type": "application/x-protobuf"]
        )

        let processor = OTelBatchSpanProcessor(
            exporter: exporter,
            configuration: .init()
        )

        let tracerProvider = OTelTracerProvider(
            resource: resource,
            sampler: OTelConstantSampler(isOn: true),
            processor: processor
        )

        OTel.bootstrapTracing(tracerProvider: tracerProvider)

        // --- Logs: bootstrap swift-log to route through OTel ---
        let logExporter = OTLPHTTPLogExporter(
            endpoint: endpointURL.appendingPathComponent("v1/logs"),
            headers: ["Content-Type": "application/x-protobuf"]
        )
        LoggingSystem.bootstrap { label in
            OTelLogHandler(
                label: label,
                exporter: logExporter,
                resource: resource
            )
        }

        // --- Metrics: bootstrap swift-metrics to route through OTel ---
        let metricExporter = OTLPHTTPMetricExporter(
            endpoint: endpointURL.appendingPathComponent("v1/metrics"),
            headers: ["Content-Type": "application/x-protobuf"]
        )
        MetricsSystem.bootstrap(OTelMetricsFactory(
            exporter: metricExporter,
            resource: resource
        ))

        return SequinsHandle(
            tracerProvider: tracerProvider,
            shutdown: {
                try? processor.forceFlush()
            }
        )
    }
}

public enum SequinsError: Error {
    case invalidEndpoint(String)
}
