<?php

declare(strict_types=1);

namespace Sequins\Otel;

use OpenTelemetry\API\Logs\LoggerProviderInterface;
use OpenTelemetry\API\Metrics\MeterProviderInterface;
use OpenTelemetry\API\Trace\TracerProviderInterface;
use OpenTelemetry\SDK\Logs\LoggerProvider;
use OpenTelemetry\SDK\Metrics\MeterProvider;
use OpenTelemetry\SDK\Trace\TracerProvider;

/**
 * Handle returned by Sequins::init(). Provides access to all configured OpenTelemetry providers.
 */
final class SequinsHandle
{
    public function __construct(
        private readonly TracerProviderInterface $tracerProvider,
        private readonly MeterProviderInterface $meterProvider,
        private readonly LoggerProviderInterface $loggerProvider,
    ) {
    }

    /**
     * Returns the configured TracerProvider.
     * Use getTracerProvider()->getTracer('name') to create tracers.
     */
    public function getTracerProvider(): TracerProviderInterface
    {
        return $this->tracerProvider;
    }

    /**
     * Returns the configured MeterProvider.
     * Use getMeterProvider()->getMeter('name') to create meters.
     */
    public function getMeterProvider(): MeterProviderInterface
    {
        return $this->meterProvider;
    }

    /**
     * Returns the configured LoggerProvider.
     */
    public function getLoggerProvider(): LoggerProviderInterface
    {
        return $this->loggerProvider;
    }

    /**
     * Flushes all pending telemetry and shuts down all providers.
     * This is registered automatically as a shutdown function.
     */
    public function shutdown(): void
    {
        if ($this->tracerProvider instanceof TracerProvider) {
            $this->tracerProvider->shutdown();
        }
        if ($this->meterProvider instanceof MeterProvider) {
            $this->meterProvider->shutdown();
        }
        if ($this->loggerProvider instanceof LoggerProvider) {
            $this->loggerProvider->shutdown();
        }
    }
}
