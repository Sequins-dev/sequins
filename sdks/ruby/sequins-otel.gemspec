# frozen_string_literal: true

require_relative "lib/sequins_otel/version"

Gem::Specification.new do |spec|
  spec.name          = "sequins-otel"
  spec.version       = SequinsOtel::VERSION
  spec.authors       = ["Sequins"]
  spec.summary       = "OpenTelemetry distro for Sequins — zero-config observability for local development"
  spec.description   = "Configures OpenTelemetry traces, metrics, and logs to export to a local Sequins instance."
  spec.homepage      = "https://sequins.dev"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.0"

  spec.files         = Dir["lib/**/*", "README.md", "sequins-otel.gemspec"]
  spec.require_paths = ["lib"]

  spec.add_dependency "opentelemetry-sdk", "~> 1.5"
  spec.add_dependency "opentelemetry-logs-sdk", "~> 0.2"
  spec.add_dependency "opentelemetry-exporter-otlp", "~> 0.28"
  spec.add_dependency "opentelemetry-exporter-otlp-logs", "~> 0.3"

  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "rake", "~> 13.0"
end
