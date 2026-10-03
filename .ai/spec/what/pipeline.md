# Pipeline

The data pipeline: how telemetry flows from OLS components through the collector to its configured destinations.

## Behavioral Rules

### Receivers

1. The collector MUST include a Prometheus receiver for scraping OLS component metrics endpoints. `[PLANNED]`
2. The collector MUST include an OTLP receiver (gRPC and HTTP) for ingesting traces and metrics pushed by OLS components.
3. On the hub, the collector MUST include an OTLP receiver to accept forwarded telemetry from spoke collectors. `[PLANNED]`

### Processors

4. The collector MUST add cluster identity labels to all telemetry (cluster name, cluster ID) so fleet-wide data is attributable to its source spoke. `[PLANNED]`
5. The collector MUST support batch processing to reduce export overhead.
6. The collector SHOULD support filtering/sampling processors to control volume in large fleets.

### Exporters

7. In spoke mode, the collector MUST export to the hub collector's OTLP endpoint. `[PLANNED]`
8. In hub mode, the collector MUST export to at least one configurable backend (Prometheus remote-write, OTLP endpoint, or both).
9. The collector MUST support multiple exporters simultaneously (e.g., Prometheus for metrics + Jaeger for traces).

### Pipeline Composition

10. Pipelines (receiver → processor → exporter chains) MUST be defined per signal type (metrics, traces, logs).
11. A misconfigured pipeline MUST fail at startup with a clear error, not at runtime.

### Trace Data Collection

12. In `config.yaml`, the top-level trace pipeline sends received traces to `nop` and `routing/data_collection`. In `config-router.yaml`, it fans out to `routing/traces` and `routing/data_collection`, with `routing/traces` forwarding all traces to the configured backend. `routing/data_collection` matches resources whose `service.name` is exactly `lightspeed-agentic-operator` or `lightspeed-agentic-sandbox` and routes their traces to `traces/data_collection`, which writes native OTLP JSONL with the stock `file/data_collection` FileExporter and preserves resource, scope, span, and event structure. The top-level trace pipelines and `traces/data_collection` are unbatched; in routing mode, only the `traces/lightspeed` backend trace pipeline applies `batch`. See `what/data-collection.md` for FileExporter version, path, rotation, retention, and failure behavior.

