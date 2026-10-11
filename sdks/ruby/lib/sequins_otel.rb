# frozen_string_literal: true

require "opentelemetry-sdk"
require "opentelemetry-logs-sdk"
require "opentelemetry-exporter-otlp"
require "opentelemetry-exporter-otlp-logs"
require_relative "sequins_otel/version"

# SequinsOtel — OpenTelemetry distro for Sequins.
#
# Zero-config observability for local development. Configures traces, metrics, and logs
# to export to a local Sequins instance via OTLP/HTTP.
#
# @example Basic usage
#   require "sequins_otel"
#
#   sequins = SequinsOtel.init(service_name: "my-app")
#   tracer = sequins.tracer_provider.tracer("my-module")
#
#   # On exit:
#   sequins.shutdown
#
module SequinsOtel
  DEFAULT_ENDPOINT = "http://localhost:4318"

  # Returned by {SequinsOtel.init}. Provides access to all configured providers.
  class Handle
    attr_reader :tracer_provider, :meter_provider, :logger_provider

    def initialize(tracer_provider:, meter_provider:, logger_provider:)
      @tracer_provider = tracer_provider
      @meter_provider = meter_provider
      @logger_provider = logger_provider
    end

    # Flushes all pending telemetry and shuts down all providers.
    # Always call this before your process exits.
    def shutdown
      @tracer_provider&.shutdown
      @meter_provider&.shutdown
      @logger_provider&.shutdown
    end
  end

  # Initialize OpenTelemetry with all signals pre-configured for Sequins.
  #
  # @param service_name [String] the name of your service
  # @param endpoint [String] the OTLP HTTP endpoint (default: http://localhost:4318)
  # @param metric_export_interval [Integer] metric export interval in milliseconds (default: 10000)
  # @param use_all [Boolean] when true, installs all available OTel instrumentations
  #   (requires the +opentelemetry-instrumentation-all+ gem). Install it and specific
  #   framework gems you want, e.g. +gem "opentelemetry-instrumentation-rack"+.
  # @return [Handle] a handle providing access to all configured providers
  def self.init(
    service_name: nil,
    endpoint: nil,
    metric_export_interval: 10_000,
    use_all: false
  )
    resolved_service_name = service_name ||
      ENV["OTEL_SERVICE_NAME"] ||
      "unknown_service"

    resolved_endpoint = endpoint ||
      ENV["OTEL_EXPORTER_OTLP_ENDPOINT"] ||
      DEFAULT_ENDPOINT

    resource = OpenTelemetry::SDK::Resources::Resource.create(
      "service.name" => resolved_service_name
    )

    exporter = OpenTelemetry::Exporter::OTLP::Exporter.new(
      endpoint: "#{resolved_endpoint}/v1/traces",
      compression: "gzip"
    )

    # Configure the global SDK
    OpenTelemetry::SDK.configure do |c|
      c.resource = resource
      c.add_span_processor(
        OpenTelemetry::SDK::Trace::Export::BatchSpanProcessor.new(exporter)
      )
      if use_all
        begin
          require "opentelemetry-instrumentation-all"
          c.use_all
        rescue LoadError
          # opentelemetry-instrumentation-all not installed — skipping auto-instrumentation.
          # Add it to your Gemfile: gem "opentelemetry-instrumentation-all"
        end
      end
    end

    tracer_provider = OpenTelemetry.tracer_provider
    meter_provider = OpenTelemetry.meter_provider rescue nil

    # --- Logs ---
    log_exporter = OpenTelemetry::Exporter::OTLP::Logs::OtlpHttpLogRecordExporter.new(
      endpoint: "#{resolved_endpoint}/v1/logs",
      compression: "gzip"
    )
    logger_provider = OpenTelemetry::SDK::Logs::LoggerProvider.new(resource: resource)
    logger_provider.add_log_record_processor(
      OpenTelemetry::SDK::Logs::Export::BatchLogRecordProcessor.new(log_exporter)
    )
    OpenTelemetry::Logs.logger_provider = logger_provider

    Handle.new(
      tracer_provider: tracer_provider,
      meter_provider: meter_provider,
      logger_provider: logger_provider
    )
  end
end
