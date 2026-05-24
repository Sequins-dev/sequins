"""Tests for sequins_otel."""

import pytest
from sequins_otel import init, SequinsHandle
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk._logs import LoggerProvider


def test_init_returns_handle():
    handle = init(service_name="test-service")
    assert isinstance(handle, SequinsHandle)
    handle.shutdown()


def test_providers_are_correct_types():
    handle = init(service_name="test-types")
    assert isinstance(handle.tracer_provider, TracerProvider)
    assert isinstance(handle.meter_provider, MeterProvider)
    assert isinstance(handle.logger_provider, LoggerProvider)
    handle.shutdown()


def test_tracer_provider_returns_tracer():
    handle = init(service_name="test-tracer")
    tracer = handle.tracer_provider.get_tracer("my-module")
    assert tracer is not None
    handle.shutdown()


def test_meter_provider_returns_meter():
    handle = init(service_name="test-meter")
    meter = handle.meter_provider.get_meter("my-module")
    assert meter is not None
    handle.shutdown()


def test_shutdown_does_not_raise():
    handle = init(service_name="test-shutdown")
    handle.shutdown()  # Should not raise


def test_custom_service_name():
    handle = init(service_name="custom-name")
    resource = handle.tracer_provider.resource
    assert resource.attributes.get("service.name") == "custom-name"
    handle.shutdown()


def test_default_service_name(monkeypatch):
    monkeypatch.delenv("OTEL_SERVICE_NAME", raising=False)
    handle = init()
    resource = handle.tracer_provider.resource
    assert resource.attributes.get("service.name") == "unknown_service"
    handle.shutdown()


def test_service_name_from_env(monkeypatch):
    monkeypatch.setenv("OTEL_SERVICE_NAME", "env-service")
    handle = init()
    resource = handle.tracer_provider.resource
    assert resource.attributes.get("service.name") == "env-service"
    handle.shutdown()
