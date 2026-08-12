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
├── cmd/otelcol-lightspeed/          # Pre-generated Collector source (committed)
│   ├── main.go                      # Generated entry point
│   ├── components.go                # Generated component wiring
│   └── go.mod / go.sum              # Full dependency graph (used by cachi2)
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
    ├── postgresadmin/
    │   ├── go.mod                   # Go module (pgx/v5)
    │   ├── doc.go                   # Package documentation
    │   ├── metadata.go              # Component type registration ("postgres_admin")
    │   ├── config.go                # Extension configuration + validation
    │   ├── factory.go               # Factory — creates extension instances
    │   ├── extension.go             # HTTP server + GET/DELETE handlers
    │   ├── config_test.go           # Config validation tests
    │   └── extension_test.go        # HTTP handler tests (pgxmock)
    └── httpsmetrics/
        ├── go.mod                   # Go module
        ├── doc.go                   # Package documentation
        ├── metadata.go              # Component type registration ("https_metrics")
        ├── config.go                # Extension configuration + validation
        ├── factory.go               # Factory — creates extension instances
        ├── extension.go             # HTTPS reverse proxy for /metrics
        ├── config_test.go           # Config validation tests
        └── extension_test.go        # Proxy + TLS tests
```

## Quick Start

```bash
# Prerequisites: Go 1.23+, PostgreSQL

# Build the collector binary (uses pre-generated source in cmd/otelcol-lightspeed/)
make build

# Run locally
make run

# Run tests
make test

# Regenerate source after changing builder-config.yaml
make generate
```

## Log Record Schema

The exporter writes a 5-column schema optimised for agentic run audit log
storage. The `postgres_admin` extension creates the table automatically on
startup (idempotent `CREATE TABLE IF NOT EXISTS`).

```sql
CREATE TABLE templogs.logs (
    id              BIGSERIAL PRIMARY KEY,
    agentic_run_id  TEXT NOT NULL,
    phase           TEXT NOT NULL DEFAULT '',
    timestamp       TIMESTAMPTZ NOT NULL,
    event           TEXT NOT NULL,
    body            JSONB
);

CREATE INDEX idx_logs_agentic_run_id ON templogs.logs (agentic_run_id);
CREATE INDEX idx_logs_run_phase ON templogs.logs (agentic_run_id, phase);
CREATE INDEX idx_logs_timestamp ON templogs.logs (timestamp);
```

| Column         | Type        | Source                                                     |
|----------------|-------------|------------------------------------------------------------|
| agentic_run_id | TEXT        | Log attribute `"agenticrun.uid"` — standard UUID (with hyphens, e.g. `550e8400-e29b-41d4-a716-446655440000`) |
| phase          | TEXT        | Log attribute `"agenticrun.phase"` (e.g. `planning`, `execution`) |
| timestamp      | TIMESTAMPTZ | TimeUnixNano → ObservedTimestamp → now                     |
| event          | TEXT        | Log attribute `"event"`                                    |
| body           | JSONB       | Log record body (serialized)                               |

## Configuration Reference

- [`builder-config.yaml`](builder-config.yaml) — OCB build manifest (which components are compiled in)
- [`config.yaml`](config.yaml) — Runtime config: direct-to-PostgreSQL (simple pipeline)
- [`config-router.yaml`](config-router.yaml) — Runtime config: routing by service name and signal type

## Admin API

### GET /api/v1/logs

Retrieve log records for an agentic run with cursor-based pagination.

```bash
curl "https://localhost:8080/api/v1/logs?agentic_run_id=550e8400-e29b-41d4-a716-446655440000&limit=50&after=100"

# Filter by phase:
curl "https://localhost:8080/api/v1/logs?agentic_run_id=550e8400-e29b-41d4-a716-446655440000&phase=planning"
```

| Parameter       | Required | Default | Description                                  |
|-----------------|----------|---------|----------------------------------------------|
| `agentic_run_id`| yes      | —       | Agentic run ID (standard UUID with hyphens)  |
| `phase`         | no       | —       | Filter by phase within the run               |
| `limit`         | no       | 100     | Max records to return (capped at 1000)       |
| `after`         | no       | 0       | Cursor: return records with id > N           |
| `format`        | no       | json    | Set to `text` for plain-text output          |

#### JSON response (default)
```json
{
  "agentic_run_id": "550e8400-e29b-41d4-a716-446655440000",
  "phase": "planning",
  "records": [
    {"id": 1, "phase": "planning", "timestamp": "2026-07-09T12:00:00Z", "event": "audit.agent.started", "body": {"msg": "hello"}},
    {"id": 2, "phase": "planning", "timestamp": "2026-07-09T12:00:01Z", "event": "audit.agent.tool.call", "body": {"tool": "bash"}}
  ],
  "has_more": false
}
```

#### Plain-text response (`format=text`)

```bash
curl "https://localhost:8080/api/v1/logs?agentic_run_id=550e8400-e29b-41d4-a716-446655440000&format=text"
```

Returns `text/plain` with a metadata header, blank line, then one `timestamp: body` per line:

```text
agentic_run_id: 550e8400-e29b-41d4-a716-446655440000
records: 3
has_more: false

2026-07-09T12:00:00Z: [agent] Starting query (model=gpt-5.4, provider=openai)
2026-07-09T12:00:01.404171Z: HTTP Request: POST https://api.openai.com/v1/responses "HTTP/1.1 200 OK"
2026-07-09T12:00:02.174204Z: [provider:run] thinking: **Investigating pods in namespace**...
```

### DELETE /api/v1/logs

Delete all log records for an agentic run (all phases).

```bash
curl -X DELETE "https://localhost:8080/api/v1/logs?agentic_run_id=550e8400-e29b-41d4-a716-446655440000"
```

| Parameter       | Required | Description                                  |
|-----------------|----------|----------------------------------------------|
| `agentic_run_id`| yes      | Agentic run ID (standard UUID with hyphens)  |

Response:
```json
{"deleted": 42, "agentic_run_id": "550e8400-e29b-41d4-a716-446655440000"}
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
| Prometheus metrics (:8888) | HTTPS | Serving cert via `tls_cert_file` / `tls_key_file` |
| PostgreSQL connection | TLS | `sslmode=require` (or `verify-full`) in DSN |
| Trace export (OTLP gRPC) | TLS | Default TLS (system CA bundle) |

In OpenShift, the serving certificate is injected by `service-ca` into
`/var/run/secrets/serving-cert/tls.{crt,key}`. For local development, omit
the TLS fields from `postgres_admin` and `https_metrics` to fall back to plaintext HTTP.
