package postgresadmin

import (
	"fmt"
	"regexp"

	"go.opentelemetry.io/collector/component"
)

var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Config holds the user-facing configuration for the postgres_admin extension.
//
// Example config.yaml:
//
//	extensions:
//	  postgres_admin:
//	    endpoint: 0.0.0.0:8080
//	    connection_string: "postgres://user:pass@localhost:5432/oteldb?sslmode=require"
//	    tls_cert_file: /var/run/secrets/serving-cert/tls.crt
//	    tls_key_file: /var/run/secrets/serving-cert/tls.key
//	    schema: templogs
//	    logs_table: logs
type Config struct {
	Endpoint         string `mapstructure:"endpoint"`
	ConnectionString string `mapstructure:"connection_string"`
	TLSCertFile      string `mapstructure:"tls_cert_file"`
	TLSKeyFile       string `mapstructure:"tls_key_file"`
	Schema           string `mapstructure:"schema"`
	LogsTable        string `mapstructure:"logs_table"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) qualifiedTable() string {
	return fmt.Sprintf("%s.%s", c.Schema, c.LogsTable)
}

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint must not be empty")
	}
	if c.ConnectionString == "" {
		return fmt.Errorf("connection_string must not be empty")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return fmt.Errorf("tls_cert_file and tls_key_file must both be set or both be empty")
	}
	if !validIdentifier.MatchString(c.Schema) {
		return fmt.Errorf("schema %q is not a valid SQL identifier", c.Schema)
	}
	if !validIdentifier.MatchString(c.LogsTable) {
		return fmt.Errorf("logs_table %q is not a valid SQL identifier", c.LogsTable)
	}
	return nil
}

func (c *Config) tlsEnabled() bool {
	return c.TLSCertFile != "" && c.TLSKeyFile != ""
}

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint:  "0.0.0.0:8080",
		Schema:    "templogs",
		LogsTable: "logs",
	}
}
