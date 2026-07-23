<?php

declare(strict_types=1);

namespace Sequins\Otel;

use OpenTelemetry\API\Globals;
use OpenTelemetry\Contrib\Otlp\HttpEndpointResolver;
use OpenTelemetry\Contrib\Otlp\OtlpHttpTransportFactory;
use OpenTelemetry\Contrib\Otlp\SpanExporter;
use OpenTelemetry\Contrib\Otlp\LogsExporter;
use OpenTelemetry\Contrib\Otlp\MetricExporter;
use OpenTelemetry\SDK\Common\Attribute\Attributes;
use OpenTelemetry\SDK\Common\Export\Http\PsrTransportFactory;
use OpenTelemetry\SDK\Logs\LoggerProvider;
use OpenTelemetry\SDK\Logs\Processor\BatchLogRecordProcessor;
use OpenTelemetry\SDK\Metrics\MeterProvider;
use OpenTelemetry\SDK\Metrics\MetricReader\ExportingReader;
use OpenTelemetry\SDK\Resource\ResourceInfo;
use OpenTelemetry\SDK\Resource\ResourceInfoFactory;
use OpenTelemetry\SDK\Sdk;
use OpenTelemetry\SDK\Trace\Sampler\AlwaysOnSampler;
use OpenTelemetry\SDK\Trace\SpanProcessor\BatchSpanProcessor;
use OpenTelemetry\SDK\Trace\TracerProvider;
use OpenTelemetry\SemConv\ResourceAttributes;

/**
 * Sequins OpenTelemetry SDK — zero-config observability for local development.
 *
 * Configures traces, metrics, and logs to export to a local Sequins instance
 * via OTLP/HTTP.
 *
 * @example
 * ```php
 * use Sequins\Otel\Sequins;
 *
 * $sequins = Sequins::init('my-app');
 * $tracer = $sequins->getTracerProvider()->getTracer('my-module');
 * $span = $tracer->spanBuilder('operation')->startSpan();
 * $span->end();
 *
 * // On shutdown (also registered automatically):
 * $sequins->shutdown();
 * ```
 */
final class Sequins
{
    private const DEFAULT_ENDPOINT = 'http://localhost:4318';

    /**
     * Initializes OpenTelemetry with all signals pre-configured for Sequins.
     *
     * @param string      $serviceName The name of your service.
     * @param string|null $endpoint    OTLP HTTP endpoint. Defaults to OTEL_EXPORTER_OTLP_ENDPOINT or http://localhost:4318.
     * @param array       $options     Additional options (reserved for future use).
     * @return SequinsHandle A handle providing access to all configured providers.
     */
    public static function init(
        string $serviceName,
        ?string $endpoint = null,
        array $options = []
    ): SequinsHandle {
        $resolvedServiceName = $serviceName
            ?: ($_ENV['OTEL_SERVICE_NAME'] ?? getenv('OTEL_SERVICE_NAME') ?: 'unknown_service');

        $resolvedEndpoint = $endpoint
            ?: ($_ENV['OTEL_EXPORTER_OTLP_ENDPOINT'] ?? getenv('OTEL_EXPORTER_OTLP_ENDPOINT') ?: self::DEFAULT_ENDPOINT);

        $resource = ResourceInfoFactory::defaultResource()->merge(
            ResourceInfo::create(Attributes::create([
                ResourceAttributes::SERVICE_NAME => $resolvedServiceName,
            ]))
        );

        // Transport factory for HTTP
        $transportFactory = new PsrTransportFactory(
            new \GuzzleHttp\Client(),
            new \GuzzleHttp\Psr7\HttpFactory(),
            new \GuzzleHttp\Psr7\HttpFactory()
        );

        // --- Traces ---
        $spanExporter = new SpanExporter(
            $transportFactory->create($resolvedEndpoint . '/v1/traces', 'application/x-protobuf')
        );
        $tracerProvider = new TracerProvider(
            new BatchSpanProcessor($spanExporter),
            new AlwaysOnSampler(),
            $resource
        );

        // --- Metrics ---
        $metricExporter = new MetricExporter(
            $transportFactory->create($resolvedEndpoint . '/v1/metrics', 'application/x-protobuf')
        );
        $metricReader = new ExportingReader($metricExporter);
        $meterProvider = new MeterProvider(
            null,
            $resource,
            \OpenTelemetry\SDK\Common\Time\ClockFactory::getDefault(),
            Attributes::create([]),
            [],
            $metricReader
        );

        // --- Logs ---
        $logExporter = new LogsExporter(
            $transportFactory->create($resolvedEndpoint . '/v1/logs', 'application/x-protobuf')
        );
        $loggerProvider = new LoggerProvider(
            new BatchLogRecordProcessor($logExporter),
            $resource
        );

        // Register globally
        Sdk::builder()
            ->setTracerProvider($tracerProvider)
            ->setMeterProvider($meterProvider)
            ->setLoggerProvider($loggerProvider)
            ->setPropagator(\OpenTelemetry\API\Trace\Propagation\TraceContextPropagator::getInstance())
            ->buildAndRegisterGlobal();

        $handle = new SequinsHandle($tracerProvider, $meterProvider, $loggerProvider);

        // Automatically flush on shutdown
        register_shutdown_function([$handle, 'shutdown']);

        return $handle;
    }
}
