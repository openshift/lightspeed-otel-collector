package postgresadmin

import "go.opentelemetry.io/collector/component"

// Type is the unique identifier for this extension.
// Referenced as "postgres_admin" in the collector's config.yaml under extensions:.
var Type = component.MustNewType("postgres_admin")
