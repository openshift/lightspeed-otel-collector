package httpsmetrics

import (
	"errors"

	"go.opentelemetry.io/collector/component"
)

// Config holds the user-facing configuration for the https_metrics extension.
//
// Example config.yaml:
//
//	extensions:
//	  https_metrics:
//	    endpoint: 0.0.0.0:8888
//	    upstream: http://127.0.0.1:18888/metrics
//	    tls_cert_file: /var/run/secrets/serving-cert/tls.crt
//	    tls_key_file: /var/run/secrets/serving-cert/tls.key
type Config struct {
	Endpoint    string `mapstructure:"endpoint"`
	Upstream    string `mapstructure:"upstream"`
	TLSCertFile string `mapstructure:"tls_cert_file"`
	TLSKeyFile  string `mapstructure:"tls_key_file"`
}

var _ component.Config = (*Config)(nil)

func (c *Config) Validate() error {
	if c.Endpoint == "" {
		return errors.New("endpoint must not be empty")
	}
	if c.Upstream == "" {
		return errors.New("upstream must not be empty")
	}
	if (c.TLSCertFile == "") != (c.TLSKeyFile == "") {
		return errors.New("tls_cert_file and tls_key_file must both be set or both empty")
	}
	return nil
}
