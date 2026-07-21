package httpsmetrics

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
)

func NewFactory() extension.Factory {
	return extension.NewFactory(
		Type,
		createDefaultConfig,
		createExtension,
		component.StabilityLevelDevelopment,
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Endpoint: "0.0.0.0:8888",
		Upstream: "http://127.0.0.1:18888/metrics",
	}
}

func createExtension(
	_ context.Context,
	set extension.Settings,
	cfg component.Config,
) (extension.Extension, error) {
	return newHTTPSMetrics(set, cfg.(*Config)), nil
}
