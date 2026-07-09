package postgresexporter

import (
	"fmt"
	"regexp"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

var validTableName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// Config holds the user-facing configuration for the postgres exporter.
//
// Example config.yaml:
//
//	exporters:
//	  postgres:
//	    connection_string: "postgres://user:pass@localhost:5432/oteldb?sslmode=require"
//	    schema: templogs
//	    logs_table: logs
type Config struct {
	ConnectionString string `mapstructure:"connection_string"`
	Schema           string `mapstructure:"schema"`
	LogsTable        string `mapstructure:"logs_table"`

	RetryConfig configretry.BackOffConfig                              `mapstructure:"retry_on_failure"`
	QueueConfig configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
}

func (c *Config) qualifiedTable() string {
	return fmt.Sprintf("%s.%s", c.Schema, c.LogsTable)
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	if c.ConnectionString == "" {
		return fmt.Errorf("connection_string must not be empty")
	}
	if !validTableName.MatchString(c.Schema) {
		return fmt.Errorf("schema %q is not a valid SQL identifier", c.Schema)
	}
	if !validTableName.MatchString(c.LogsTable) {
		return fmt.Errorf("logs_table %q is not a valid SQL identifier", c.LogsTable)
	}
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{
		Schema:      "templogs",
		LogsTable:   "logs",
		RetryConfig: configretry.NewDefaultBackOffConfig(),
		QueueConfig: configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
	}
}
