# frozen_string_literal: true

require "minitest/autorun"
require "sequins_otel"

class SequinsOtelTest < Minitest::Test
  def test_version
    refute_nil SequinsOtel::VERSION
  end

  def test_init_returns_handle
    handle = SequinsOtel.init(service_name: "test-service")
    assert_instance_of SequinsOtel::Handle, handle
    handle.shutdown
  end

  def test_tracer_provider_is_not_nil
    handle = SequinsOtel.init(service_name: "test-tracer")
    refute_nil handle.tracer_provider
    handle.shutdown
  end

  def test_tracer_from_provider
    handle = SequinsOtel.init(service_name: "test-get-tracer")
    tracer = handle.tracer_provider.tracer("my-module")
    refute_nil tracer
    handle.shutdown
  end

  def test_shutdown_does_not_raise
    handle = SequinsOtel.init(service_name: "test-shutdown")
    assert_silent { handle.shutdown }
  end

  def test_default_service_name
    handle = SequinsOtel.init
    refute_nil handle.tracer_provider
    handle.shutdown
  end
end
