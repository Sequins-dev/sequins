// Package sequins provides a zero-config OpenTelemetry distro for Sequins.
// It configures traces, metrics, and logs to export to a local Sequins instance
// via OTLP/gRPC, and optionally captures CPU profiles via runtime/pprof.
//
// Example:
//
//	s, err := sequins.Init(context.Background(), sequins.Config{
//	    ServiceName: "my-app",
//	})
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer s.Shutdown(context.Background())
//
//	tracer := s.TracerProvider.Tracer("my-module")
//	meter := s.MeterProvider.Meter("my-module")
package sequins

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	otelmetric "go.opentelemetry.io/otel/metric"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultEndpoint        = "localhost:4317"
	defaultHTTPEndpoint    = "http://localhost:4318"
	defaultMetricInterval  = 10 * time.Second
	defaultProfileInterval = 30 * time.Second
)

// Config holds configuration for the Sequins SDK.
type Config struct {
	// ServiceName is reported in all telemetry signals.
	// Defaults to OTEL_SERVICE_NAME env var, or "unknown_service".
	ServiceName string

	// Endpoint is the OTLP gRPC endpoint (host:port, no scheme).
	// Defaults to OTEL_EXPORTER_OTLP_ENDPOINT env var, or "localhost:4317".
	Endpoint string

	// HTTPEndpoint is used only for profile export.
	// Defaults to "http://localhost:4318".
	HTTPEndpoint string

	// MetricInterval controls how often metrics are exported.
	// Default: 10 seconds.
	MetricInterval time.Duration

	// ProfilesEnabled enables periodic CPU profile capture and export.
	// Default: false.
	ProfilesEnabled bool

	// ProfileInterval controls how often CPU profiles are captured.
	// Default: 30 seconds.
	ProfileInterval time.Duration
}

// Sequins holds references to all configured OpenTelemetry providers.
type Sequins struct {
	// TracerProvider is the configured trace provider.
	// Use TracerProvider.Tracer("name") to create tracers.
	TracerProvider *trace.TracerProvider

	// MeterProvider is the configured metrics provider.
	// Use MeterProvider.Meter("name") to create meters.
	MeterProvider *metric.MeterProvider

	// LoggerProvider is the configured log provider.
	// Use LoggerProvider.Logger("name") to create loggers.
	LoggerProvider *log.LoggerProvider

	profilerStop chan struct{}
}

// Tracer returns a Tracer for the given instrumentation scope.
func (s *Sequins) Tracer(name string, opts ...oteltrace.TracerOption) oteltrace.Tracer {
	return s.TracerProvider.Tracer(name, opts...)
}

// Meter returns a Meter for the given instrumentation scope.
func (s *Sequins) Meter(name string, opts ...otelmetric.MeterOption) otelmetric.Meter {
	return s.MeterProvider.Meter(name, opts...)
}

// Shutdown flushes all pending telemetry and shuts down providers.
// Always call this before your program exits.
func (s *Sequins) Shutdown(ctx context.Context) error {
	if s.profilerStop != nil {
		close(s.profilerStop)
	}
	var errs []error
	if err := s.TracerProvider.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("trace provider shutdown: %w", err))
	}
	if err := s.MeterProvider.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("meter provider shutdown: %w", err))
	}
	if err := s.LoggerProvider.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Errorf("logger provider shutdown: %w", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("sequins shutdown errors: %v", errs)
	}
	return nil
}

// Init initializes OpenTelemetry with all signals pre-configured for Sequins.
// It registers the providers globally so auto-instrumentation libraries work
// without additional wiring.
func Init(ctx context.Context, cfg Config) (*Sequins, error) {
	// Resolve config
	if cfg.ServiceName == "" {
		cfg.ServiceName = os.Getenv("OTEL_SERVICE_NAME")
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "unknown_service"
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultEndpoint
	}
	if cfg.HTTPEndpoint == "" {
		cfg.HTTPEndpoint = defaultHTTPEndpoint
	}
	if cfg.MetricInterval == 0 {
		cfg.MetricInterval = defaultMetricInterval
	}
	if cfg.ProfileInterval == 0 {
		cfg.ProfileInterval = defaultProfileInterval
	}

	// Shared gRPC connection
	conn, err := grpc.NewClient(cfg.Endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("sequins: failed to create gRPC connection: %w", err)
	}

	// Resource
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(cfg.ServiceName)),
		resource.WithProcess(),
		resource.WithOS(),
	)
	if err != nil {
		return nil, fmt.Errorf("sequins: failed to create resource: %w", err)
	}

	// --- Traces ---
	traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("sequins: failed to create trace exporter: %w", err)
	}
	tp := trace.NewTracerProvider(
		trace.WithBatchSpanProcessor(trace.NewBatchSpanProcessor(traceExporter)),
		trace.WithResource(res),
		trace.WithSampler(trace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)

	// --- Metrics ---
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("sequins: failed to create metric exporter: %w", err)
	}
	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(cfg.MetricInterval))),
		metric.WithResource(res),
	)
	otel.SetMeterProvider(mp)

	// --- Logs ---
	logExporter, err := otlploggrpc.New(ctx, otlploggrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("sequins: failed to create log exporter: %w", err)
	}
	lp := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
		log.WithResource(res),
	)
	global.SetLoggerProvider(lp)

	s := &Sequins{
		TracerProvider: tp,
		MeterProvider:  mp,
		LoggerProvider: lp,
	}

	// --- Profiles (optional) ---
	if cfg.ProfilesEnabled {
		stop := make(chan struct{})
		s.profilerStop = stop
		go runProfiler(cfg.ServiceName, cfg.HTTPEndpoint, cfg.ProfileInterval, stop)
	}

	return s, nil
}
