# System Overview

The Lightspeed OTel Collector is a custom OpenTelemetry Collector distribution
for OpenShift Lightspeed. The reference configurations ingest OTLP logs and
traces, route them to their configured destinations, and write native trace
JSONL for resources from the two allowlisted services.

## Behavioral Rules

### System Role

1. The collector is a custom OTel Collector distribution built with the OpenTelemetry Collector Builder (ocb).
2. It includes only the receivers, processors, and exporters needed by the OLS fleet — no unnecessary upstream components.
3. The Collector runs as a single-replica Deployment managed by the lightspeed-operator.

### Deployment Modes

4. **Spoke mode:** collects telemetry from local OLS components (service, agentic operator, sandbox, alerts adapter) and exports to the hub collector. `[PLANNED]`
5. **Hub mode:** receives telemetry from spoke collectors, aggregates fleet-wide data, and exports to the final backend (Prometheus, Jaeger, etc.). `[PLANNED]`
6. The deployment mode is determined by configuration, not by separate binaries. `[PLANNED]`

### Signal Support

7. The collector MUST support metrics (Prometheus scraping and OTLP ingestion).
8. The collector MUST support traces (OTLP ingestion).
9. The collector SHOULD support logs (OTLP ingestion) — initially optional, required when structured logging is adopted across OLS components.

### Trace Data Collection

10. The trace data collection route matches resources whose `service.name` is
    exactly `lightspeed-agentic-operator` or `lightspeed-agentic-sandbox` and
    sends their traces through `routing/data_collection` and `traces/data_collection` to the
    stock FileExporter. It retains native OTLP resource, scope, span, and event
    structure. See `what/data-collection.md` for file settings and
    operational behavior.
    [PLANNED: OLS-4493] The route sends selected traces to the `dataverse`
    exporter instead; see `what/dataverse-exporter.md`.

### Resilience

11. The PostgreSQL log branch is buffered through its configured queue; the
    trace data collection branch writes through FileExporter and uses its configured
    source-file lifecycle.
12. The FileExporter uses size-triggered rotation at the configured
    `rotation.max_megabytes` MiB threshold and applies configured
    `max_backups` and `max_days` retention criteria. These settings are
    independent of consumer upload status and do not impose a hard disk
    quota; write failures may propagate through a trace request even if a
    sibling destination already accepted the same batch. See
    `what/data-collection.md`.
    [PLANNED: OLS-4493] Rules 11-12 for traces are replaced: the trace data
    collection branch is buffered in the `dataverse` exporter's in-memory
    queue with retries. Queue-full and upload failures never fail the
    producer request; see `what/dataverse-exporter.md`.
13. The Collector exposes its standard health check on port 13133 and serves
    internal Prometheus metrics over HTTPS on port 8888.

## Configuration Surface

Configuration follows standard OTel Collector YAML — receivers, processors,
exporters, connectors, and pipelines. See `what/collector.md` for reference
configuration and `what/data-collection.md` for native trace-file
behavior and standalone testing.

## Scope Boundary

The Collector owns the native JSONL source files and their retention.
The downstream interface is closed rotated files on a read-only source mount.
Conversion, upload, and ledger state belong to the consuming component; see
`what/data-collection.md` for this integration contract.

[PLANNED: OLS-4493] The Collector owns trace upload to the Red Hat ingress
through the `dataverse` exporter. No trace files and no downstream consumer
interface remain.
