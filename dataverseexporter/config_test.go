package dataverseexporter

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/confmap"
)

func TestDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if cfg.Endpoint != DefaultIngressEndpoint {
		t.Errorf("endpoint = %q, want %q", cfg.Endpoint, DefaultIngressEndpoint)
	}
	if cfg.ServiceID != "ols" {
		t.Errorf("service_id = %q, want ols", cfg.ServiceID)
	}
	if cfg.ArchivePathPrefix != "agentic-v1/" {
		t.Errorf("archive_path_prefix = %q, want agentic-v1/", cfg.ArchivePathPrefix)
	}
	if cfg.MaxPayloadSize != DefaultMaxPayloadSize {
		t.Errorf("max_payload_size = %d, want %d", cfg.MaxPayloadSize, DefaultMaxPayloadSize)
	}
	if cfg.Timeout != 30*time.Second {
		t.Errorf("timeout = %v, want 30s", cfg.Timeout)
	}
	if !cfg.QueueConfig.HasValue() {
		t.Error("sending_queue must be present by default")
	}
}

func TestConfigValidate(t *testing.T) {
	valid := func() *Config {
		return createDefaultConfig().(*Config)
	}
	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{"empty endpoint", func(c *Config) { c.Endpoint = "" }},
		{"http endpoint", func(c *Config) { c.Endpoint = "http://example.com/upload" }},
		{"relative endpoint", func(c *Config) { c.Endpoint = "not-a-url" }},
		{"insecure skip verify", func(c *Config) { c.TLS.InsecureSkipVerify = true }},
		{"empty service id", func(c *Config) { c.ServiceID = "" }},
		{"path-unsafe service id", func(c *Config) { c.ServiceID = "a/b" }},
		{"colon service id", func(c *Config) { c.ServiceID = "a:b" }},
		{"dotdot service id", func(c *Config) { c.ServiceID = "a..b" }},
		{"prefix missing slash", func(c *Config) { c.ArchivePathPrefix = "agentic-v1" }},
		{"absolute prefix", func(c *Config) { c.ArchivePathPrefix = "/agentic-v1/" }},
		{"dotdot prefix", func(c *Config) { c.ArchivePathPrefix = "../escape/" }},
		{"embedded dotdot", func(c *Config) { c.ArchivePathPrefix = "agentic-v1/../x/" }},
		{"zero payload size", func(c *Config) { c.MaxPayloadSize = 0 }},
		{"negative payload size", func(c *Config) { c.MaxPayloadSize = -1 }},
		{"payload size above cap", func(c *Config) { c.MaxPayloadSize = DefaultMaxPayloadSize + 1 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := valid()
			tc.mut(c)
			if err := c.Validate(); err == nil {
				t.Errorf("Validate() = nil, want error")
			}
		})
	}
	if err := valid().Validate(); err != nil {
		t.Errorf("valid config Validate() = %v, want nil", err)
	}

	// Additional valid cases: unset prefix is allowed, and both known
	// service IDs must pass the allowlist.
	c := valid()
	c.ArchivePathPrefix = ""
	if err := c.Validate(); err != nil {
		t.Errorf("unset archive_path_prefix Validate() = %v, want nil", err)
	}
	for _, id := range []string{"ols", "rhos-lightspeed"} {
		c := valid()
		c.ServiceID = id
		if err := c.Validate(); err != nil {
			t.Errorf("service_id %q Validate() = %v, want nil", id, err)
		}
	}

	// Documents current behavior: an uppercase-scheme endpoint ("HTTPS://...")
	// is accepted because net/url.Parse normalizes the scheme to lowercase
	// before the https check. If this ever needs to be rejected, Validate must
	// inspect the raw string, not the parsed scheme.
	c = valid()
	c.Endpoint = "HTTPS://example.com/upload"
	if err := c.Validate(); err != nil {
		t.Errorf("uppercase-scheme endpoint Validate() = %v, want nil (url.Parse lowercases the scheme)", err)
	}
}

func TestConfigMapstructureRoundTrip(t *testing.T) {
	in := map[string]any{
		"endpoint":            "https://ingress.example.com/upload",
		"service_id":          "rhos-lightspeed",
		"archive_path_prefix": "agentic-v1/",
		"max_payload_size":    1024,
		"sending_queue":       map[string]any{"queue_size": 100},
		"retry_on_failure":    map[string]any{"enabled": true},
	}
	conf := confmap.NewFromStringMap(in)
	var c Config
	if err := conf.Unmarshal(&c); err != nil {
		t.Fatalf("Unmarshal() = %v, want nil", err)
	}
	if c.Endpoint != "https://ingress.example.com/upload" {
		t.Errorf("endpoint = %q", c.Endpoint)
	}
	if c.ServiceID != "rhos-lightspeed" {
		t.Errorf("service_id = %q", c.ServiceID)
	}
	if c.ArchivePathPrefix != "agentic-v1/" {
		t.Errorf("archive_path_prefix = %q", c.ArchivePathPrefix)
	}
	if c.MaxPayloadSize != 1024 {
		t.Errorf("max_payload_size = %d", c.MaxPayloadSize)
	}
	if !c.QueueConfig.HasValue() {
		t.Error("sending_queue must be present after decode")
	}
	if !c.RetryConfig.Enabled {
		t.Error("retry_on_failure.enabled must decode to true")
	}
	if err := c.Validate(); err != nil {
		t.Errorf("decoded config Validate() = %v, want nil", err)
	}
}
