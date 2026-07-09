package postgresexporter

import "go.opentelemetry.io/collector/component"

// Type is the unique identifier for this exporter.
// The collector matches this against the component name in config.yaml
// (e.g. "exporters: postgres:"). MustNewType panics on invalid input
// so misconfiguration is caught at init time, not at runtime.
var Type = component.MustNewType("postgres")
