package postgresadmin

import (
	"testing"
)

func TestValidateRejectsEmptyEndpoint(t *testing.T) {
	cfg := &Config{
		Endpoint:         "",
		ConnectionString: "postgres://localhost/db",
		Schema:           "templogs",
		LogsTable:        "logs",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty endpoint, got nil")
	}
}

func TestValidateRejectsEmptyConnectionString(t *testing.T) {
	cfg := &Config{
		Endpoint:         "0.0.0.0:8080",
		ConnectionString: "",
		Schema:           "templogs",
		LogsTable:        "logs",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty connection_string, got nil")
	}
}

func TestValidateRejectsInvalidSchema(t *testing.T) {
	cfg := &Config{
		Endpoint:         "0.0.0.0:8080",
		ConnectionString: "postgres://localhost/db",
		Schema:           "bad; DROP TABLE",
		LogsTable:        "logs",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid schema, got nil")
	}
}

func TestValidateRejectsInvalidTableName(t *testing.T) {
	cfg := &Config{
		Endpoint:         "0.0.0.0:8080",
		ConnectionString: "postgres://localhost/db",
		Schema:           "templogs",
		LogsTable:        "1bad_table",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for invalid logs_table, got nil")
	}
}

func TestValidateAcceptsValidConfig(t *testing.T) {
	cfg := &Config{
		Endpoint:         "0.0.0.0:8080",
		ConnectionString: "postgres://localhost/db",
		Schema:           "templogs",
		LogsTable:        "logs",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config, got: %v", err)
	}
}

func TestValidateAcceptsValidConfigWithTLS(t *testing.T) {
	cfg := &Config{
		Endpoint:         "0.0.0.0:8080",
		ConnectionString: "postgres://localhost/db",
		TLSCertFile:      "/path/to/tls.crt",
		TLSKeyFile:       "/path/to/tls.key",
		Schema:           "templogs",
		LogsTable:        "logs",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config with TLS, got: %v", err)
	}
}

func TestValidateRejectsPartialTLSConfig(t *testing.T) {
	cfg := &Config{
		Endpoint:         "0.0.0.0:8080",
		ConnectionString: "postgres://localhost/db",
		TLSCertFile:      "/path/to/tls.crt",
		Schema:           "templogs",
		LogsTable:        "logs",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for partial TLS config (cert without key), got nil")
	}

	cfg = &Config{
		Endpoint:         "0.0.0.0:8080",
		ConnectionString: "postgres://localhost/db",
		TLSKeyFile:       "/path/to/tls.key",
		Schema:           "templogs",
		LogsTable:        "logs",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for partial TLS config (key without cert), got nil")
	}
}

func TestCreateDefaultConfigValues(t *testing.T) {
	raw := createDefaultConfig()
	cfg, ok := raw.(*Config)
	if !ok {
		t.Fatalf("expected *Config, got %T", raw)
	}
	if cfg.Endpoint != "0.0.0.0:8080" {
		t.Errorf("default Endpoint = %q, want %q", cfg.Endpoint, "0.0.0.0:8080")
	}
	if cfg.Schema != "templogs" {
		t.Errorf("default Schema = %q, want %q", cfg.Schema, "templogs")
	}
	if cfg.LogsTable != "logs" {
		t.Errorf("default LogsTable = %q, want %q", cfg.LogsTable, "logs")
	}
}
