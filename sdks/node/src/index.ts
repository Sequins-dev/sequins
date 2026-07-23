import * as api from '@opentelemetry/api';
import { logs } from '@opentelemetry/api-logs';
import { getNodeAutoInstrumentations } from '@opentelemetry/auto-instrumentations-node';
import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-grpc';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-grpc';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-grpc';
import { Instrumentation, registerInstrumentations } from '@opentelemetry/instrumentation';
import { Resource } from '@opentelemetry/resources';
import { BatchLogRecordProcessor, LoggerProvider } from '@opentelemetry/sdk-logs';
import { MeterProvider, PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import {
  BatchSpanProcessor,
  NodeTracerProvider,
} from '@opentelemetry/sdk-trace-node';
import { ATTR_SERVICE_NAME } from '@opentelemetry/semantic-conventions';

export interface SequinsConfig {
  /** Service name reported in all telemetry. Defaults to OTEL_SERVICE_NAME env var or 'unknown_service'. */
  serviceName?: string;
  /** OTLP gRPC endpoint. Defaults to OTEL_EXPORTER_OTLP_ENDPOINT or 'http://localhost:4317'. */
  endpoint?: string;
  /** Export interval for metrics in milliseconds. Default: 10000 */
  metricExportIntervalMs?: number;
  /**
   * Auto-instrumentation plugins to register. Defaults to all available Node.js
   * instrumentations from `@opentelemetry/auto-instrumentations-node`.
   * Pass `[]` to disable auto-instrumentation entirely.
   */
  instrumentations?: Instrumentation[];
}

export interface SequinsHandle {
  /** The configured TracerProvider. Use this to create tracers for manual instrumentation. */
  tracerProvider: NodeTracerProvider;
  /** The configured MeterProvider. Use this to create meters for manual instrumentation. */
  meterProvider: MeterProvider;
  /** The configured LoggerProvider. Use this to create loggers for manual instrumentation. */
  loggerProvider: LoggerProvider;
  /** Flushes all pending telemetry and shuts down providers. Call this before process exit. */
  shutdown(): Promise<void>;
}

/**
 * Initializes OpenTelemetry with all signals (traces, metrics, logs) pre-configured to
 * export to a local Sequins instance.
 *
 * @example
 * ```typescript
 * import { init } from '@sequins/otel';
 *
 * const sequins = init({ serviceName: 'my-app' });
 * const tracer = sequins.tracerProvider.getTracer('my-module');
 * const meter = sequins.meterProvider.getMeter('my-module');
 *
 * // On process exit:
 * await sequins.shutdown();
 * ```
 */
export function init(config: SequinsConfig = {}): SequinsHandle {
  const serviceName =
    config.serviceName ??
    process.env.OTEL_SERVICE_NAME ??
    'unknown_service';

  const endpoint =
    config.endpoint ??
    process.env.OTEL_EXPORTER_OTLP_ENDPOINT ??
    'http://localhost:4317';

  const metricExportIntervalMs = config.metricExportIntervalMs ?? 10_000;
  const instrumentations =
    config.instrumentations !== undefined
      ? config.instrumentations
      : getNodeAutoInstrumentations();

  const resource = new Resource({
    [ATTR_SERVICE_NAME]: serviceName,
  });

  // --- Traces ---
  const traceExporter = new OTLPTraceExporter({ url: endpoint });
  const tracerProvider = new NodeTracerProvider({
    resource,
    spanProcessors: [new BatchSpanProcessor(traceExporter)],
  });
  tracerProvider.register();

  // --- Metrics ---
  const metricExporter = new OTLPMetricExporter({ url: endpoint });
  const meterProvider = new MeterProvider({
    resource,
    readers: [
      new PeriodicExportingMetricReader({
        exporter: metricExporter,
        exportIntervalMillis: metricExportIntervalMs,
      }),
    ],
  });
  api.metrics.setGlobalMeterProvider(meterProvider);

  // --- Logs ---
  const logExporter = new OTLPLogExporter({ url: endpoint });
  const loggerProvider = new LoggerProvider({ resource });
  loggerProvider.addLogRecordProcessor(new BatchLogRecordProcessor(logExporter));
  logs.setGlobalLoggerProvider(loggerProvider);

  // --- Auto-instrumentation ---
  registerInstrumentations({ tracerProvider, meterProvider, instrumentations });

  return {
    tracerProvider,
    meterProvider,
    loggerProvider,
    async shutdown() {
      await Promise.allSettled([
        tracerProvider.shutdown(),
        meterProvider.shutdown(),
        loggerProvider.shutdown(),
      ]);
    },
  };
}
