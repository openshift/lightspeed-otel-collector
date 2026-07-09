package postgresexporter

import (
	"testing"
)

func TestValidateRejectsEmptyConnectionString(t *testing.T) {
	cfg := &Config{
		ConnectionString: "",
		Schema:           "templogs",
		LogsTable:        "logs",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("expected error for empty connection_string, got nil")
	}
}

func TestValidateRejectsInvalidSchema(t *testing.T) {
	cases := []struct {
		name   string
		schema string
	}{
		{"empty", ""},
		{"has spaces", "my schema"},
		{"starts with number", "1bad"},
		{"has dash", "my-schema"},
		{"sql injection", "foo; DROP TABLE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				ConnectionString: "postgres://localhost/db",
				Schema:           tc.schema,
				LogsTable:        "logs",
			}
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected error for schema %q, got nil", tc.schema)
			}
		})
	}
}

func TestValidateRejectsInvalidTableName(t *testing.T) {
	cases := []struct {
		name  string
		table string
	}{
		{"empty", ""},
		{"has spaces", "my table"},
		{"starts with number", "1logs"},
		{"has semicolon", "logs;"},
		{"sql injection", "logs; DROP TABLE logs"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{
				ConnectionString: "postgres://localhost/db",
				Schema:           "templogs",
				LogsTable:        tc.table,
			}
			if err := cfg.Validate(); err == nil {
				t.Fatalf("expected error for logs_table %q, got nil", tc.table)
			}
		})
	}
}

func TestValidateAcceptsValidConfig(t *testing.T) {
	cfg := &Config{
		ConnectionString: "postgres://user:pass@localhost:5432/oteldb?sslmode=disable",
		Schema:           "templogs",
		LogsTable:        "logs",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid config to pass, got: %v", err)
	}
}

func TestValidateAcceptsUnderscoresAndNumbers(t *testing.T) {
	cfg := &Config{
		ConnectionString: "postgres://localhost/db",
		Schema:           "_internal_schema",
		LogsTable:        "logs_v2_prod",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid identifiers to pass, got: %v", err)
	}
}

func TestQualifiedTable(t *testing.T) {
	cfg := &Config{
		Schema:    "templogs",
		LogsTable: "logs",
	}
	want := "templogs.logs"
	if got := cfg.qualifiedTable(); got != want {
		t.Fatalf("qualifiedTable() = %q, want %q", got, want)
	}
}

func TestCreateDefaultConfigValues(t *testing.T) {
	raw := createDefaultConfig()
	cfg, ok := raw.(*Config)
	if !ok {
		t.Fatalf("expected *Config, got %T", raw)
	}

	if cfg.Schema != "templogs" {
		t.Errorf("default Schema = %q, want %q", cfg.Schema, "templogs")
	}
	if cfg.LogsTable != "logs" {
		t.Errorf("default LogsTable = %q, want %q", cfg.LogsTable, "logs")
	}
	if cfg.ConnectionString != "" {
		t.Errorf("default ConnectionString should be empty, got %q", cfg.ConnectionString)
	}
	if !cfg.RetryConfig.Enabled {
		t.Error("default RetryConfig should be enabled")
	}
	if !cfg.QueueConfig.HasValue() {
		t.Error("default QueueConfig should have a value")
	}
}
