package dataverseexporter

import "go.opentelemetry.io/collector/component"

// Type is the unique identifier for this exporter, matched against the
// component name in config.yaml (e.g. "exporters: dataverse:").
// MustNewType panics on invalid input so misconfiguration is caught at
// init time, not at runtime.
var Type = component.MustNewType("dataverse")
