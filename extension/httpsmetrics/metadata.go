package httpsmetrics

import "go.opentelemetry.io/collector/component"

// Type is the unique identifier for this extension.
// Referenced as "https_metrics" in the collector's config.yaml under extensions:.
var Type = component.MustNewType("https_metrics")
