defmodule SequinsOtelTest do
  use ExUnit.Case, async: false
  doctest SequinsOtel

  test "configure/1 returns :ok" do
    assert :ok == SequinsOtel.configure(service_name: "test-service")
  end

  test "configure/1 sets service name in app env" do
    SequinsOtel.configure(service_name: "my-configured-service")
    resource = Application.get_env(:opentelemetry, :resource)
    assert resource[:service][:name] == "my-configured-service"
  end

  test "configure/1 uses default endpoint" do
    SequinsOtel.configure(service_name: "endpoint-test")
    endpoint = Application.get_env(:opentelemetry_exporter, :otlp_endpoint)
    assert endpoint == "http://localhost:4317"
  end

  test "configure/1 accepts custom endpoint" do
    SequinsOtel.configure(service_name: "custom-endpoint", endpoint: "http://custom:4317")
    endpoint = Application.get_env(:opentelemetry_exporter, :otlp_endpoint)
    assert endpoint == "http://custom:4317"
  end
end
