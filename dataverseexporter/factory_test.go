package dataverseexporter

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestNewFactoryType(t *testing.T) {
	if got := NewFactory().Type(); got != Type {
		t.Errorf("NewFactory().Type() = %q, want %q", got, Type)
	}
}

func TestNewFactoryDefaultConfig(t *testing.T) {
	got := NewFactory().CreateDefaultConfig()
	want := createDefaultConfig()
	if _, ok := got.(*Config); !ok {
		t.Fatalf("CreateDefaultConfig returned %T, want *Config", got)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("CreateDefaultConfig() = %+v, want %+v", got, want)
	}
}

func TestNewFactoryTracesStability(t *testing.T) {
	if got := NewFactory().TracesStability(); got != component.StabilityLevelDevelopment {
		t.Errorf("TracesStability() = %v, want %v", got, component.StabilityLevelDevelopment)
	}
}

// TestCreateTracesExporterOutsideCluster puts the real factory path (the one
// the Collector binary exercises at startup) under test: without in-cluster
// environment variables, exporter creation must fail with a clear message
// rather than panicking or succeeding silently.
func TestCreateTracesExporterOutsideCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	set := exportertest.NewNopSettings(Type)
	_, err := NewFactory().CreateTraces(context.Background(), set, createDefaultConfig())
	if err == nil {
		t.Fatal("expected error outside a cluster")
	}
	if !strings.Contains(err.Error(), "in-cluster credentials") {
		t.Errorf("error %q does not mention in-cluster credentials", err)
	}
}
