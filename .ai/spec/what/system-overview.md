# System Overview

The Lightspeed OTel Collector is a custom OpenTelemetry Collector distribution
for OpenShift Lightspeed. The reference configurations ingest OTLP logs and
traces, route them to their configured destinations, and export traces from
the two allowlisted services to the Red Hat Dataverse ingress.

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

10. [PLANNED: OLS-4493] The trace data collection route matches resources
    whose `service.name` is exactly `lightspeed-agentic-operator` or
    `lightspeed-agentic-sandbox` and sends their traces through
    `routing/data_collection` and `traces/data_collection` to the `dataverse`
    exporter. It retains native OTLP resource, scope, span, and event
    structure. See `what/data-collection.md` for the exporter, upload, and
    failure contract.

### Resilience

11. [PLANNED: OLS-4493] The PostgreSQL log branch is buffered through its
    configured queue; the trace data collection branch batches in the
    `dataverse` exporter's memory queue and uploads to the ingress service
    with retry and counted-drop semantics.
12. [PLANNED: OLS-4493] The dataverse exporter classifies upload errors as
    retryable or permanent, swallows queue-full conditions for fan-out
    isolation, resolves credentials lazily over the in-cluster API, and
    bounds memory by queue size. Failures are silent to producers; drops
    are visible in the exporter's drop counters. See
    `what/data-collection.md`.
13. The Collector exposes its standard health check on port 13133 and serves
    internal Prometheus metrics over HTTPS on port 8888.

## Configuration Surface

Configuration follows standard OTel Collector YAML — receivers, processors,
exporters, connectors, and pipelines. See `what/collector.md` for reference
configuration and `what/data-collection.md` for the dataverse exporter
contract and standalone testing.

## Scope Boundary

The Collector owns trace selection, batching, packaging, and upload to the
Red Hat ingress. Dataverse data products own extraction, deduplication, and
analytics; see `what/data-collection.md` for the integration contract.
