# Pipeline

The data pipeline: how telemetry flows from OLS components through the collector to backends.

## Behavioral Rules

### Receivers

1. The collector MUST include a Prometheus receiver for scraping OLS component metrics endpoints.
2. The collector MUST include an OTLP receiver (gRPC and HTTP) for ingesting traces and metrics pushed by OLS components.
3. On the hub, the collector MUST include an OTLP receiver to accept forwarded telemetry from spoke collectors.

### Processors

4. The collector MUST add cluster identity labels to all telemetry (cluster name, cluster ID) so fleet-wide data is attributable to its source spoke.
5. The collector MUST support batch processing to reduce export overhead.
6. The collector SHOULD support filtering/sampling processors to control volume in large fleets.

### Exporters

7. In spoke mode, the collector MUST export to the hub collector's OTLP endpoint.
8. In hub mode, the collector MUST export to at least one configurable backend (Prometheus remote-write, OTLP endpoint, or both).
9. The collector MUST support multiple exporters simultaneously (e.g., Prometheus for metrics + Jaeger for traces).

### Pipeline Composition

10. Pipelines (receiver → processor → exporter chains) MUST be defined per signal type (metrics, traces, logs).
11. A misconfigured pipeline MUST fail at startup with a clear error, not at runtime.

## Planned Changes

| Ticket | Summary |
|---|---|
| — | All rules are planned — initial design |
