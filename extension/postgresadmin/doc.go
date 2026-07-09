// Package postgresadmin implements an OpenTelemetry Collector extension
// that exposes an HTTP API for managing log records in PostgreSQL.
// It provides selective deletion by indexed column (trace_id, span_id, etc.)
// so operators can control database growth without relying on TTL.
package postgresadmin
