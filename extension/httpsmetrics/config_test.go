package httpsmetrics

import (
	"testing"
)

func TestValidateRejectsEmptyEndpoint(t *testing.T) {
	cfg := &Config{Endpoint: "", Upstream: "http://localhost:18888/metrics"}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestValidateRejectsEmptyUpstream(t *testing.T) {
	cfg := &Config{Endpoint: "0.0.0.0:8888", Upstream: ""}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty upstream")
	}
}

func TestValidateRejectsPartialTLS(t *testing.T) {
	cfg := &Config{
		Endpoint:    "0.0.0.0:8888",
		Upstream:    "http://localhost:18888/metrics",
		TLSCertFile: "/tmp/tls.crt",
		TLSKeyFile:  "",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for partial TLS config")
	}
}

func TestValidateAcceptsValidConfig(t *testing.T) {
	cfg := &Config{
		Endpoint: "0.0.0.0:8888",
		Upstream: "http://127.0.0.1:18888/metrics",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateAcceptsValidConfigWithTLS(t *testing.T) {
	cfg := &Config{
		Endpoint:    "0.0.0.0:8888",
		Upstream:    "http://127.0.0.1:18888/metrics",
		TLSCertFile: "/var/run/secrets/tls.crt",
		TLSKeyFile:  "/var/run/secrets/tls.key",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCreateDefaultConfigValues(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if cfg.Endpoint != "0.0.0.0:8888" {
		t.Errorf("expected endpoint 0.0.0.0:8888, got %s", cfg.Endpoint)
	}
	if cfg.Upstream != "http://127.0.0.1:18888/metrics" {
		t.Errorf("expected upstream http://127.0.0.1:18888/metrics, got %s", cfg.Upstream)
	}
}
