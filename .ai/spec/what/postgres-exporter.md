# PostgreSQL Exporter

Custom OTel Collector exporter that writes OTLP log records to PostgreSQL.

## Behavioral Rules

### Core Functionality

1. The exporter receives batches of OTLP log records from the Collector pipeline.
2. For each log record, the exporter extracts:
   - **`agentic_run_id`** — from log attribute `"agenticrun.uid"`. A standard UUID with hyphens (e.g. `550e8400-e29b-41d4-a716-446655440000`). Stored as-is.
   - **`phase`** — from log attribute `"agenticrun.phase"` (e.g. `planning`, `execution`). Defaults to empty string if not set.
   - **`timestamp`** — from the log record's `TimeUnixNano` field, converted to `TIMESTAMPTZ`. Falls back to `ObservedTimestamp`, then current time.
   - **`event`** — from the log record's attributes (key: `event`). This is the event discriminator (e.g., `audit.agenticrun.received`, `audit.agent.tool.call`).
   - **`body`** — the log record's body, serialized as JSONB. If serialization fails, wrapped as `{"raw": "..."}`.
3. The exporter writes extracted fields into the `templogs.logs` table.
3a. Log records that lack the `"agenticrun.uid"` attribute are skipped (not written) — they are unqueryable without a run ID. The exporter emits a `Warn` log recording how many records were skipped, so the drop is observable (consistent with Constraint 2: no silent data loss).

### Batch Insert

4. The exporter uses a single multi-value `INSERT` statement per batch for efficiency and atomicity:
   ```sql
   INSERT INTO templogs.logs (agentic_run_id, phase, timestamp, event, body)
   VALUES ($1,$2,$3,$4,$5), ($6,$7,$8,$9,$10), ...
   ```
5. Batch size is bounded by what the Collector pipeline delivers per export call (controlled by the `batch` processor).
6. A single `INSERT` statement is inherently atomic in PostgreSQL — if any value fails, the entire statement is rejected and the Collector retries per its retry policy.

### Connection Management

7. The exporter connects to PostgreSQL using a DSN provided via the `connection_string` configuration field. Pool tuning is via DSN parameters (e.g., `?pool_max_conns=10`). The connection uses TLS (`sslmode=require` or `sslmode=verify-full`).
8. The exporter maintains a connection pool using `pgxpool`. Pool size defaults to the number of CPUs (minimum 4).
9. On startup, the exporter pings PostgreSQL to verify connectivity. If the ping fails, the pool is closed and the Collector fails to start.
10. On connection failure during operation, the exporter returns an error to the Collector pipeline. The Collector's built-in retry mechanism handles retries with exponential backoff.

### Error Handling

11. The exporter does not drop log records silently. If a write fails, the export call returns an error.
12. The Collector's retry and queue mechanisms handle transient failures (Postgres restarts, network blips).
13. The `postgres_admin` extension creates the schema, table, and indexes on startup using `IF NOT EXISTS` (idempotent). Extensions start before pipelines in the OTel Collector lifecycle, so the table is guaranteed to exist before the exporter writes its first batch. If the table is missing at insert time (edge case), the exporter surfaces this as an export error and the Collector's `retry_on_failure` mechanism retries with backoff.

### Configuration

14. The exporter accepts the following configuration fields:

| Field | Type | Required | Description |
|---|---|---|---|
| `connection_string` | string | yes | PostgreSQL DSN. Supports env var expansion (`${env:POSTGRES_CONNECTION_STRING}`). |
| `schema` | string | no | PostgreSQL schema name. Default: `templogs`. |
| `logs_table` | string | no | Table name within the schema. Default: `logs`. |
| `retry_on_failure` | object | no | Retry with exponential backoff. Default: enabled. |
| `sending_queue` | object | no | File-backed persistent queue. Default: enabled. |

15. The exporter validates its configuration at startup. Missing or empty `connection_string`, or invalid schema/table identifiers cause the Collector to fail to start.

## Implementation

### Go Package Structure

```
postgresexporter/
├── config.go         # Configuration struct and validation
├── config_test.go    # Config validation tests
├── doc.go            # Package documentation
├── exporter.go       # Pool interface, start/shutdown, ConsumeLogs with multi-value INSERT
├── exporter_test.go  # Exporter logic tests (pgxmock)
├── factory.go        # OTel component factory registration
├── metadata.go       # Component type registration ("postgres")
└── telemetry.go      # Internal metrics (insert duration, batch size, pool stats)
```

### OTel Component Interface

16. The exporter implements the `exporter.Logs` interface from the OTel Collector SDK.
17. The factory function registers the exporter under the type name `postgres`.
18. The exporter's `consumeLogs` method receives `plog.Logs` and writes them to PostgreSQL.

### SQL

19. The insert statement uses parameterized queries to prevent SQL injection:
    ```sql
    INSERT INTO {schema}.{table} (agentic_run_id, phase, timestamp, event, body)
    VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10), ...
    ```
20. The schema and table names are validated at configuration time (alphanumeric + underscore only) and are not parameterizable — they are compiled into the SQL statement.

### Dependencies

21. The exporter uses `pgx/v5` with `pgxpool` for PostgreSQL connectivity. `pgx` provides native connection pooling and batch support.
22. The `postgres_admin` extension creates the schema, table, and indexes on first startup using `IF NOT EXISTS`. The exporter does not use an ORM or migration framework.
23. Tests use `pgxmock/v5` for database interaction testing without a real PostgreSQL instance.

## Cross-References

- `what/collector.md` — OCB build, Collector configuration
