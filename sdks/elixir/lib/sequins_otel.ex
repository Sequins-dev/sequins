defmodule SequinsOtel do
  @moduledoc """
  OpenTelemetry distro for Sequins — zero-config observability for local development.

  Configures OpenTelemetry traces, metrics, and logs to export to a local Sequins
  instance via OTLP/gRPC.

  ## Usage

      # In your application.ex:
      def start(_type, _args) do
        SequinsOtel.configure(service_name: "my-app")
        # ... start your supervision tree
      end

  Or use the `init/1` function which also starts the OTel application:

      {:ok, _} = SequinsOtel.init(service_name: "my-app")

  ## Environment variables

  - `OTEL_SERVICE_NAME` — overrides `service_name`
  - `OTEL_EXPORTER_OTLP_ENDPOINT` — overrides `endpoint`
  """

  @default_endpoint "http://localhost:4317"

  @doc """
  Configures OpenTelemetry application environment for Sequins.

  This is the preferred approach — call it before starting the OTel application,
  typically in your `Application.start/2` callback.

  ## Options

    * `:service_name` — service name (default: `OTEL_SERVICE_NAME` env var or `"unknown_service"`)
    * `:endpoint` — OTLP gRPC endpoint (default: `OTEL_EXPORTER_OTLP_ENDPOINT` env var or `"http://localhost:4317"`)
    * `:traces_enabled` — enable trace export (default: `true`)
    * `:metrics_enabled` — enable metrics export (default: `true`)
    * `:logs_enabled` — enable log export (default: `true`)
    * `:auto_instrument` — when `true`, calls `.setup()` on available instrumentation
      packages (`opentelemetry_phoenix`, `opentelemetry_ecto`, `opentelemetry_oban`).
      Each must be added separately to your `mix.exs`. (default: `false`)
  """
  @spec configure(keyword()) :: :ok
  def configure(opts \\ []) do
    service_name =
      Keyword.get(opts, :service_name) ||
        System.get_env("OTEL_SERVICE_NAME") ||
        "unknown_service"

    endpoint =
      Keyword.get(opts, :endpoint) ||
        System.get_env("OTEL_EXPORTER_OTLP_ENDPOINT") ||
        @default_endpoint

    # Configure resource
    Application.put_env(:opentelemetry, :resource, [
      service: [name: service_name]
    ])

    # Configure OTLP exporter
    Application.put_env(:opentelemetry_exporter, :otlp_protocol, :grpc)
    Application.put_env(:opentelemetry_exporter, :otlp_endpoint, endpoint)

    # Configure batch span processor
    Application.put_env(:opentelemetry, :processors, [
      otel_batch_processor: %{
        exporter: {
          :opentelemetry_exporter,
          %{endpoints: [endpoint]}
        }
      }
    ])

    if Keyword.get(opts, :auto_instrument, false) do
      _try_setup(OpentelemetryPhoenix)
      _try_setup(OpentelemetryEcto)
      _try_setup(OpentelemetryOban)
    end

    :ok
  end

  defp _try_setup(module) do
    if Code.ensure_loaded?(module), do: module.setup()
  end

  @doc """
  Initializes OpenTelemetry for Sequins and starts the OTel application.

  Returns `{:ok, sequins}` where `sequins` is a map with the configured settings.
  Call `SequinsOtel.shutdown/1` when your application exits.

  Accepts all options from `configure/1`, including `:auto_instrument`.

  ## Example

      {:ok, sequins} = SequinsOtel.init(service_name: "my-app")
      # Use :opentelemetry_api to create spans:
      # require OpenTelemetry.Tracer
      # OpenTelemetry.Tracer.with_span "my-operation" do ... end

      # On exit:
      SequinsOtel.shutdown(sequins)
  """
  @spec init(keyword()) :: {:ok, map()} | {:error, term()}
  def init(opts \\ []) do
    :ok = configure(opts)

    case Application.ensure_all_started(:opentelemetry) do
      {:ok, _apps} ->
        service_name =
          Keyword.get(opts, :service_name) ||
            System.get_env("OTEL_SERVICE_NAME") ||
            "unknown_service"

        endpoint =
          Keyword.get(opts, :endpoint) ||
            System.get_env("OTEL_EXPORTER_OTLP_ENDPOINT") ||
            @default_endpoint

        {:ok, %{service_name: service_name, endpoint: endpoint}}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Shuts down OpenTelemetry, flushing any pending spans.
  """
  @spec shutdown(map()) :: :ok
  def shutdown(_sequins) do
    :opentelemetry.shutdown_tracer_provider(:global)
    :ok
  end
end
