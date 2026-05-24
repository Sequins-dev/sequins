"""
sequins_otel — OpenTelemetry distro for Sequins.

Zero-config observability for local development. Configures traces, metrics,
and logs to export to a local Sequins instance via OTLP/gRPC.

Example:
    from sequins_otel import init

    sequins = init(service_name="my-app")
    tracer = sequins.tracer_provider.get_tracer("my-module")
    meter = sequins.meter_provider.get_meter("my-module")

    # On exit:
    sequins.shutdown()
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from importlib.metadata import entry_points
from typing import Optional

from opentelemetry import metrics as metrics_api
from opentelemetry import trace as trace_api
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

_DEFAULT_ENDPOINT = "http://localhost:4317"
_DEFAULT_METRIC_INTERVAL_MS = 10_000


@dataclass
class SequinsHandle:
    """
    Handle returned by :func:`init`. Provides access to all configured OTel providers
    and a :meth:`shutdown` method to flush and close them.
    """

    tracer_provider: TracerProvider
    """The configured TracerProvider. Create tracers via ``tracer_provider.get_tracer("name")``."""

    meter_provider: MeterProvider
    """The configured MeterProvider. Create meters via ``meter_provider.get_meter("name")``."""

    logger_provider: LoggerProvider
    """The configured LoggerProvider. Create loggers via ``logger_provider.get_logger("name")``."""

    def shutdown(self) -> None:
        """Flushes all pending telemetry and shuts down all providers."""
        self.tracer_provider.shutdown()
        self.meter_provider.shutdown()
        self.logger_provider.shutdown()


def _activate_instrumentors(tracer_provider, meter_provider) -> None:
    """Discover and activate all installed OpenTelemetry instrumentors.

    Instrumentors register themselves under the ``opentelemetry_instrumentor``
    entry-points group. Install framework-specific packages to activate them:
    ``pip install opentelemetry-instrumentation-flask``
    ``pip install opentelemetry-instrumentation-requests``
    ``pip install opentelemetry-instrumentation-django``
    """
    try:
        eps = entry_points(group="opentelemetry_instrumentor")
    except Exception:
        return
    for ep in eps:
        try:
            instrumentor_cls = ep.load()
            instrumentor_cls().instrument(
                tracer_provider=tracer_provider,
                meter_provider=meter_provider,
            )
        except Exception:
            pass


def init(
    service_name: Optional[str] = None,
    endpoint: Optional[str] = None,
    metric_export_interval_ms: int = _DEFAULT_METRIC_INTERVAL_MS,
    auto_instrument: bool = False,
) -> SequinsHandle:
    """
    Initialize OpenTelemetry with all signals pre-configured to export to Sequins.

    Args:
        service_name: Name of the service. Defaults to OTEL_SERVICE_NAME env var
                      or ``"unknown_service"``.
        endpoint: OTLP gRPC endpoint. Defaults to OTEL_EXPORTER_OTLP_ENDPOINT env var
                  or ``"http://localhost:4317"``.
        metric_export_interval_ms: How often to export metrics (milliseconds). Default: 10000.
        auto_instrument: When ``True``, automatically activates all installed OTel
                         instrumentors discovered via entry points. Install framework
                         packages first, e.g.
                         ``pip install opentelemetry-instrumentation-flask``.

    Returns:
        A :class:`SequinsHandle` with references to all configured providers.
    """
    resolved_service_name = (
        service_name
        or os.environ.get("OTEL_SERVICE_NAME")
        or "unknown_service"
    )
    resolved_endpoint = (
        endpoint
        or os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
        or _DEFAULT_ENDPOINT
    )

    resource = Resource.create({"service.name": resolved_service_name})

    # --- Traces ---
    trace_exporter = OTLPSpanExporter(endpoint=resolved_endpoint)
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
    trace_api.set_tracer_provider(tracer_provider)

    # --- Metrics ---
    metric_exporter = OTLPMetricExporter(endpoint=resolved_endpoint)
    metric_reader = PeriodicExportingMetricReader(
        metric_exporter,
        export_interval_millis=metric_export_interval_ms,
    )
    meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader])
    metrics_api.set_meter_provider(meter_provider)

    # --- Logs ---
    log_exporter = OTLPLogExporter(endpoint=resolved_endpoint)
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    set_logger_provider(logger_provider)

    if auto_instrument:
        _activate_instrumentors(tracer_provider, meter_provider)

    return SequinsHandle(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        logger_provider=logger_provider,
    )
