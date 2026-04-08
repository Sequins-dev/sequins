package sequins_test

import (
	"context"
	"testing"

	sequins "github.com/sequins-dev/otel-go"
)

func TestInit_ReturnsHandle(t *testing.T) {
	ctx := context.Background()
	s, err := sequins.Init(ctx, sequins.Config{
		ServiceName: "test-service",
		Endpoint:    "localhost:4317",
	})
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	if s.TracerProvider == nil {
		t.Error("TracerProvider is nil")
	}
	if s.MeterProvider == nil {
		t.Error("MeterProvider is nil")
	}
	if s.LoggerProvider == nil {
		t.Error("LoggerProvider is nil")
	}
	if err := s.Shutdown(ctx); err != nil {
		t.Errorf("Shutdown returned error: %v", err)
	}
}

func TestInit_TracerWorks(t *testing.T) {
	ctx := context.Background()
	s, err := sequins.Init(ctx, sequins.Config{ServiceName: "test-tracer"})
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	tracer := s.Tracer("my-lib")
	if tracer == nil {
		t.Error("Tracer is nil")
	}
	_ = s.Shutdown(ctx)
}

func TestInit_MeterWorks(t *testing.T) {
	ctx := context.Background()
	s, err := sequins.Init(ctx, sequins.Config{ServiceName: "test-meter"})
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	meter := s.Meter("my-lib")
	if meter == nil {
		t.Error("Meter is nil")
	}
	_ = s.Shutdown(ctx)
}

func TestInit_DefaultServiceName(t *testing.T) {
	ctx := context.Background()
	s, err := sequins.Init(ctx, sequins.Config{})
	if err != nil {
		t.Fatalf("Init returned error: %v", err)
	}
	_ = s.Shutdown(ctx)
}
