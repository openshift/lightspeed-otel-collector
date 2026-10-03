# OpenTelemetry Collector — OpenShift Lightspeed

Custom OpenTelemetry Collector distribution for OpenShift Lightspeed.
Receives OTLP logs over TLS and writes them to PostgreSQL. The trace data
collection branch also writes selected resources as native OTLP JSONL.

```
OTLP logs ----> receiver --> batch processor --> postgresexporter --> PostgreSQL (TLS)
OTLP traces --> receiver --> existing backend or nop sink
                  \------> routing/data_collection --> traces/data_collection --> FileExporter

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
├── config.yaml                       # Reference direct runtime configuration
├── config-router.yaml                # Reference routing configuration
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
# Prerequisites: Go 1.26.0+ (cmd/otelcol-lightspeed/go.mod), PostgreSQL

# Build the collector binary (uses pre-generated source in cmd/otelcol-lightspeed/)
make build

# Run locally
make run

# Run tests
make test

# Regenerate source after changing builder-config.yaml
make generate
```

## Trace Data Collection

The `routing/data_collection` branch selects resources whose `service.name` is exactly
`lightspeed-agentic-operator` or `lightspeed-agentic-sandbox`. Selection uses
the resource attribute only: it does not require `agenticrun.uid`,
`agenticrun.phase`, specific span or event names, or payload content. Every
span and its attached events in a matched resource are retained in their
original OTLP context. Unmatched resources are not written to this file branch.
The file branch accepts traces only; OTLP logs continue to their existing
PostgreSQL/debug routes and metrics are not written to product files. Existing
trace forwarding or the direct-config `nop` sink remains independent.

The Collector uses the stock contrib FileExporter
`github.com/open-telemetry/opentelemetry-collector-contrib/exporter/fileexporter`
v0.159.0. Trace support is alpha; review output compatibility before upgrading
the component. The reference trial configuration is:

```yaml
file/data_collection:
  path: /var/lib/lightspeed-data/otel/traces.jsonl
  format: json
  create_directory: true
  rotation:
    max_megabytes: 1
    max_backups: 100
    max_days: 1
```

One JSONL object is one complete OTLP trace batch passed to the exporter after
routing, not one span per line. Its native structure is
`resourceSpans[] -> scopeSpans[] -> spans[] -> events[]`; a batch can contain
multiple resources, scopes, and spans, with events nested under their spans.
The batch uses native OTLP JSON, not a Collector-defined record envelope or a
downstream payload schema. The Collector does not redact selected OTLP data,
so protect these files as raw trace data.

FileExporter writes the JSON object and its LF separately. An exact 1 MiB
object has been observed at the end of a closed backup without a trailing LF,
with the separate LF appearing as an empty line in the new active file.
Readers MUST accept a complete final JSON object without LF and ignore empty
lines at rotation boundaries.

`rotation.max_megabytes` is an integer number of MiB: `1` means 1 MiB; a 500 KB
threshold is not representable by this setting. A serialized export batch
larger than 1 MiB is rejected, not split across files or spans. This is
independent of the OTLP receiver's larger request-size limit. Rotation is
size-triggered only: a quiet, below-threshold active file is not moved to a
backup on a timer or at shutdown. In rotation mode, a restart appends to the
existing active file; do not set `append: true` with rotation.

The 100-backup count and one-day age limits are stock retention criteria, not
upload acknowledgements or a hard disk quota. Count cleanup can remove a backup
before one day. Cleanup is asynchronous; age cleanup is housekeeping, not an
exact expiry timer, and neither criterion rotates an idle active file. About
101 MiB is an estimate for the active file plus backups, not a filesystem
quota. Cleanup delays, files held open after unlink, and other data on the
volume can increase space use. FileExporter has no custom queue, retry, or
failure-isolation wrapper: setup or filesystem errors can prevent startup or
fail a trace request, even if a sibling destination already accepted the same
batch. It provides no exactly-once guarantee. Backup filenames are managed by
the stock rotator; list the directory rather than assuming a suffix.

The Collector owns source writes, size rotation, and source retention. A future
sidecar is outside this Collector-only change. If introduced, it must read
closed rotated files from a read-only source mount and must never modify or
delete Collector-owned files. It may convert native OTLP JSON for a future
consumer, but this document specifies no downstream schema or ingestion
behavior. The sidecar owns its own ledger and may prune ledger entries for
absent source files only after a successful, complete directory scan; failed,
incomplete, or unreadable scans must leave the ledger unchanged.

The lightspeed-operator has not been migrated and still generates the legacy
custom `agentic` exporter configuration. That configuration is incompatible
with this image; an image-only operator override can prevent Collector startup.
Until an operator migration exists, the existing
`spec.ols.userDataCollection.transcriptsDisabled: true` opt-out is required
before using the new image with the unchanged operator. When that field is
`false` or absent, the operator emits the old configuration; setting it to
`true` omits that legacy custom exporter configuration but does not enable the new FileExporter.
Operator changes, sidecar deployment, and upload are out of scope. Use the
standalone local procedure below to exercise this Collector-only change.

### Standalone local rotation smoke

This launches the built Collector directly with a loopback-only, no-TLS config;
it does not depend on operator-generated configuration, PostgreSQL, or a
container image. It needs `make`, Go 1.26.0+ (the module directive), `curl`,
`jq`, Bash 4.4+, and GNU coreutils `date` on `PATH` (for `%s%N`). Run from the
repository root:

```bash
set -euo pipefail
make build

work="$(mktemp -d)"
mkdir -p "$work/otel"
cat >"$work/config.yaml" <<YAML
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:4318

connectors:
  routing/data_collection:
    table:
      - context: resource
        condition: attributes["service.name"] == "lightspeed-agentic-operator" or attributes["service.name"] == "lightspeed-agentic-sandbox"
        pipelines: [traces/data_collection]

exporters:
  nop: {}
  file/data_collection:
    path: $work/otel/traces.jsonl
    format: json
    create_directory: true
    rotation:
      max_megabytes: 1
      max_backups: 100
      max_days: 1

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [nop, routing/data_collection]
    traces/data_collection:
      receivers: [routing/data_collection]
      exporters: [file/data_collection]
YAML

./cmd/otelcol-lightspeed/otelcol-lightspeed --config "$work/config.yaml" \
  >"$work/collector.log" 2>&1 &
collector_pid=$!
cleanup() {
  kill "$collector_pid" 2>/dev/null || true
  wait "$collector_pid" 2>/dev/null || true
}
trap cleanup EXIT

ready=
for _ in {1..30}; do
  if curl --silent --max-time 1 --output /dev/null http://127.0.0.1:4318/; then
    ready=yes
    break
  fi
  sleep 1
done
if [ "$ready" != yes ]; then
  cat "$work/collector.log"
  exit 1
fi

payload="$(printf '%48000s' '' | tr ' ' x)"
for i in {1..32}; do
  printf -v trace_id '%032x' "$i"
  printf -v span_id '%016x' "$i"
  timestamp="$(date +%s%N)"
  marker="rotation-$i"
  jq -n -c \
    --arg trace_id "$trace_id" \
    --arg span_id "$span_id" \
    --arg timestamp "$timestamp" \
    --arg marker "$marker" \
    --arg payload "$payload" \
    '{
      resourceSpans: [{
        resource: {attributes: [{
          key: "service.name",
          value: {stringValue: "lightspeed-agentic-operator"}
        }]},
        scopeSpans: [{
          scope: {name: "fileexporter-smoke"},
          spans: [{
            traceId: $trace_id,
            spanId: $span_id,
            name: $marker,
            startTimeUnixNano: $timestamp,
            endTimeUnixNano: $timestamp,
            attributes: [{key: "smoke.payload", value: {stringValue: $payload}}],
            events: [{
              timeUnixNano: $timestamp,
              name: "gen_ai.input",
              attributes: [{key: "smoke.marker", value: {stringValue: $marker}}]
            }]
          }]
        }]
      }]
    }' >"$work/request.json"
  curl --fail --silent --show-error \
    -H 'Content-Type: application/json' \
    --data-binary @"$work/request.json" \
    http://127.0.0.1:4318/v1/traces >/dev/null
done

find "$work/otel" -maxdepth 1 -type f -print
mapfile -d '' backups < <(find "$work/otel" -maxdepth 1 -type f ! -name traces.jsonl -print0)
if [ "${#backups[@]}" -eq 0 ]; then
  cat "$work/collector.log"
  exit 1
fi
for file in "$work/otel/traces.jsonl" "${backups[@]}"; do
  printf '%s\n' "$file"
  jq -c '.resourceSpans[]?.scopeSpans[]?.spans[]? |
    {name, events: [.events[]?.name]}' "$file"
done
```

Each request contains one approximately 48 KB span batch (with a nested event),
so no single request reaches the 1 MiB serialized-batch rejection threshold;
32 writes exceed the size-rotation threshold. The loop deliberately uses one
span per request for compactness; the file format is still one whole OTLP
batch per JSON object. The `jq` output from the active file and all closed
backups should show `rotation-*` span names and nested `gen_ai.input` events.
The Collector process is stopped when the shell exits; an under-threshold
active file is not rotated by that shutdown.

### Local runtime evidence

The controller's smoke against the newly built distribution observed:

- A mixed five-resource trace request wrote only the operator and sandbox
  resources to the file branch, retaining native events, typed attributes,
  IDs, schema URLs, and links. The original five resources reached the
  configured HTTP trace backend.
- OTLP logs were stored in PostgreSQL and read back through the admin API; they
  did not appear in trace files.
- Size rotation produced inspectable JSON backups. A separate retention run
  with `max_backups: 2` removed the oldest backup.
- Restarting below the threshold appended to the existing active file. A quiet
  interval and graceful shutdown did not create a backup.
- At the exact 1 MiB boundary, the complete JSON object was in a closed backup
  without LF and the separate LF was an empty record in the new active file.
  A 1,050,149-byte serialized write returned HTTP 503 with
  `write length 1050149 exceeds maximum file size 1048576`.

This is local Collector runtime evidence only. Kubernetes deployment/mount
behavior, sidecar behavior, and downstream ingestion were not exercised.

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

The retry policy and file-backed `file_storage` sending queue below apply only
to OTLP logs sent to PostgreSQL; they do not protect the Agentic trace-file
branch.

| Failure scenario | What happens |
|---|---|
| **Transient PostgreSQL failure** | Retried automatically with backoff |
| **Pod restart** | PostgreSQL log queue persists when its storage volume survives |
| **Node failure** | Queue volume loss may lose in-flight log data |

The trace FileExporter has no custom queue, retry, or isolation layer. A
configuration or filesystem write failure can fail the trace request even if
another configured trace destination already accepted the same request. See
the native trace-file contract above.

## Credentials

Use the collector's environment variable substitution to inject credentials
from a Kubernetes Secret:

```yaml
# In collector config:
connection_string: "${env:POSTGRES_CONNECTION_STRING}"
```

When managed by the **lightspeed-operator**, credential handling is automatic.

## TLS

The production/reference configuration uses TLS on its external channels:

| Channel | Protocol | TLS mechanism |
|---|---|---|
| OTLP ingestion (gRPC :4317) | mTLS-capable | Serving cert via `tls.cert_file` / `tls.key_file` |
| OTLP ingestion (HTTP :4318) | HTTPS | Serving cert via `tls.cert_file` / `tls.key_file` |
| Admin API (:8080) | HTTPS | Serving cert via `tls_cert_file` / `tls_key_file` |
| Prometheus metrics (:8888) | HTTPS | Serving cert via `tls_cert_file` / `tls_key_file` |
| PostgreSQL connection | TLS | `sslmode=require` (or `verify-full`) in DSN |
| Trace export (OTLP gRPC) | TLS | Default TLS (system CA bundle) |

The standalone local rotation smoke deliberately binds OTLP HTTP to loopback
without TLS; do not expose that test listener outside the local machine.

In OpenShift, the serving certificate is injected by `service-ca` into
`/var/run/secrets/serving-cert/tls.{crt,key}`. For local development, omit
the TLS fields from `postgres_admin` and `https_metrics` to fall back to plaintext HTTP.
