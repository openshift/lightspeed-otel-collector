# OpenTelemetry Collector — OpenShift Lightspeed

Custom OpenTelemetry Collector distribution for OpenShift Lightspeed.
Receives OTLP logs over TLS and writes them to PostgreSQL.

```
App --OTLP/TLS--> receiver --> batch processor --> postgresexporter --> PostgreSQL (TLS)

App ---------- GET/DELETE /api/v1/logs (HTTPS) --> postgres_admin --> PostgreSQL (TLS)
```

## Project Structure

```
├── builder-config.yaml              # OCB manifest — defines included components
├── Dockerfile                       # Multi-stage UBI9 container build
├── Makefile                         # Build, test, container targets
├── postgresexporter/
│   ├── go.mod                       # Go module (pgx/v5)
│   ├── doc.go                       # Package documentation
│   ├── metadata.go                  # Component type registration ("postgres")
│   ├── config.go                    # Configuration struct + validation
│   ├── factory.go                   # Factory — creates exporter instances
│   ├── exporter.go                  # Core logic — pgx batch inserts
│   ├── telemetry.go                 # Internal metrics (insert duration, pool stats)
│   ├── config_test.go               # Config validation tests
│   └── exporter_test.go             # Exporter logic tests (pgxmock)
└── extension/
    └── postgresadmin/
        ├── go.mod                   # Go module (pgx/v5)
        ├── doc.go                   # Package documentation
        ├── metadata.go              # Component type registration ("postgres_admin")
        ├── config.go                # Extension configuration + validation
        ├── factory.go               # Factory — creates extension instances
        ├── extension.go             # HTTP server + GET/DELETE handlers
        ├── config_test.go           # Config validation tests
        └── extension_test.go        # HTTP handler tests (pgxmock)
```

## Quick Start

```bash
# Prerequisites: Go 1.23+, PostgreSQL

# Build the collector binary
make build

# Run locally
./dist/otelcol-lightspeed --config=config.yaml

# Run tests
make test
```

## Log Record Schema

The exporter writes a simplified 4-column schema optimised for audit log
storage. Table creation is the operator's responsibility.

```sql
CREATE TABLE templogs.logs (
    id         BIGSERIAL PRIMARY KEY,
    trace_id   TEXT NOT NULL,
    timestamp  TIMESTAMPTZ NOT NULL,
    event      TEXT NOT NULL,
    body       JSONB
);

CREATE INDEX idx_logs_trace_id ON templogs.logs (trace_id);
CREATE INDEX idx_logs_timestamp ON templogs.logs (timestamp);
```

| Column    | Type        | Source                                          |
|-----------|-------------|-------------------------------------------------|
| trace_id  | TEXT        | Log record TraceID (32-char hex)                |
| timestamp | TIMESTAMPTZ | TimeUnixNano → ObservedTimestamp → now          |
| event     | TEXT        | Log record attribute `"event"`                  |
| body      | JSONB       | Log record body (serialized)                    |

## Configuration Reference

- [`builder-config.yaml`](builder-config.yaml) — OCB build manifest (which components are compiled in)
- [`config.yaml`](config.yaml) — Runtime config: direct-to-PostgreSQL (simple pipeline)
- [`config-router.yaml`](config-router.yaml) — Runtime config: routing by service name and signal type

## Admin API

### GET /api/v1/logs

Retrieve log records for a trace with cursor-based pagination.

```bash
curl "https://localhost:8080/api/v1/logs?trace_id=abc123&limit=50&after=100"
```

| Parameter  | Required | Default | Description                        |
|------------|----------|---------|------------------------------------|
| `trace_id` | yes      | —       | Filter by trace ID                 |
| `limit`    | no       | 100     | Max records to return (capped 1000)|
| `after`    | no       | 0       | Cursor: return records with id > N |

Response:
```json
{
  "trace_id": "abc123",
  "records": [
    {"id": 1, "timestamp": "2026-07-09T12:00:00Z", "event": "audit.agent.started", "body": {"msg": "hello"}},
    {"id": 2, "timestamp": "2026-07-09T12:00:01Z", "event": "audit.agent.tool.call", "body": {"tool": "bash"}}
  ],
  "has_more": false
}
```

### DELETE /api/v1/logs

Delete all log records for a trace.

```bash
curl -X DELETE "https://localhost:8080/api/v1/logs?trace_id=abc123"
```

Response:
```json
{"deleted": 42, "trace_id": "abc123"}
```

## Container Build

```bash
# Build image (runs tests first)
make docker-build

# Push to registry
make docker-push

# Custom image tag
make docker-build VERSION=0.1.0
```

## Data Durability

The exporter uses **retry with exponential backoff** and a **file-backed
sending queue** (via the `file_storage` extension):

| Failure scenario | What happens |
|---|---|
| **Transient PostgreSQL failure** | Retried automatically with backoff |
| **Pod restart** | Queue persisted to disk — data resumes on restart |
| **Node failure** | Queue volume lost — in-flight data is lost |

## Credentials

Use the collector's environment variable substitution to inject credentials
from a Kubernetes Secret:

```yaml
# In collector config:
connection_string: "${env:POSTGRES_CONNECTION_STRING}"
```

When managed by the **lightspeed-operator**, credential handling is automatic.

## TLS

All communication channels use TLS:

| Channel | Protocol | TLS mechanism |
|---|---|---|
| OTLP ingestion (gRPC :4317) | mTLS-capable | Serving cert via `tls.cert_file` / `tls.key_file` |
| OTLP ingestion (HTTP :4318) | HTTPS | Serving cert via `tls.cert_file` / `tls.key_file` |
| Admin API (:8080) | HTTPS | Serving cert via `tls_cert_file` / `tls_key_file` |
| PostgreSQL connection | TLS | `sslmode=require` (or `verify-full`) in DSN |
| Trace export (OTLP gRPC) | TLS | Default TLS (system CA bundle) |

In OpenShift, the serving certificate is injected by `service-ca` into
`/var/run/secrets/serving-cert/tls.{crt,key}`. For local development, omit
the TLS fields from `postgres_admin` to fall back to plaintext HTTP.
