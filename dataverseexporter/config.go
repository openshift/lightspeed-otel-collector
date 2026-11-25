package dataverseexporter

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	// DefaultIngressEndpoint is the Dataverse ingress upload API.
	DefaultIngressEndpoint = "https://console.redhat.com/api/ingress/v1/upload"

	// DefaultMaxPayloadSize matches the ingress 100 MiB payload cap.
	DefaultMaxPayloadSize = 100 * 1024 * 1024
)

// Config holds the user-facing configuration for the dataverse exporter.
//
//	exporters:
//	  dataverse:
//	    endpoint: https://console.redhat.com/api/ingress/v1/upload
//	    service_id: ols
//	    archive_path_prefix: agentic-v1/
//	    tls:
//	      ca_file: /etc/pki/trusted-ca/tls-ca-bundle.pem
//	    sending_queue:
//	      sizer: bytes
//	      queue_size: 134217728
//	      batch:
//	        sizer: bytes
//	        min_size: 2097152
//	        max_size: 4194304
//	        flush_timeout: 10m
//	    retry_on_failure:
//	      enabled: true
//	      initial_interval: 30s
//	      max_interval: 5m
//	      max_elapsed_time: 1h
type Config struct {
	confighttp.ClientConfig `mapstructure:",squash"` // endpoint, timeout, proxy_url, tls

	ServiceID         string `mapstructure:"service_id"`
	ArchivePathPrefix string `mapstructure:"archive_path_prefix"`
	MaxPayloadSize    int64  `mapstructure:"max_payload_size"`

	RetryConfig configretry.BackOffConfig                                `mapstructure:"retry_on_failure"`
	QueueConfig configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`
}

var _ component.Config = (*Config)(nil)

// validServiceID mirrors postgresexporter's validTableName: an anchored
// allowlist rather than a blocklist, so only known-safe characters pass.
var validServiceID = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_-]*$`)

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return fmt.Errorf("endpoint must not be empty")
	}
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Host == "" {
		return fmt.Errorf("endpoint %q must be an absolute URL", c.Endpoint)
	}
	if u.Scheme != "https" {
		return fmt.Errorf("endpoint %q must use https", c.Endpoint)
	}
	if c.TLS.InsecureSkipVerify {
		return fmt.Errorf("tls.insecure_skip_verify must not be enabled")
	}
	if strings.TrimSpace(c.ServiceID) == "" || !validServiceID.MatchString(c.ServiceID) {
		return fmt.Errorf("service_id %q must match ^[a-zA-Z_][a-zA-Z0-9_-]*$", c.ServiceID)
	}
	if c.ArchivePathPrefix != "" {
		if !strings.HasSuffix(c.ArchivePathPrefix, "/") || strings.HasPrefix(c.ArchivePathPrefix, "/") ||
			strings.Contains(c.ArchivePathPrefix, "..") {
			return fmt.Errorf("archive_path_prefix %q must be a relative path ending in / with no '..' segments", c.ArchivePathPrefix)
		}
	}
	if c.MaxPayloadSize <= 0 {
		return fmt.Errorf("max_payload_size must be positive")
	}
	if c.MaxPayloadSize > DefaultMaxPayloadSize {
		return fmt.Errorf("max_payload_size %d exceeds the ingress cap of %d bytes (100 MiB)", c.MaxPayloadSize, DefaultMaxPayloadSize)
	}
	return nil
}

func createDefaultConfig() component.Config {
	return &Config{
		ClientConfig: confighttp.ClientConfig{
			Endpoint: DefaultIngressEndpoint,
			Timeout:  30 * time.Second,
		},
		ServiceID:         "ols",
		ArchivePathPrefix: "agentic-v1/",
		MaxPayloadSize:    DefaultMaxPayloadSize,
		RetryConfig:       configretry.NewDefaultBackOffConfig(),
		QueueConfig:       configoptional.Some(exporterhelper.NewDefaultQueueConfig()),
	}
}
