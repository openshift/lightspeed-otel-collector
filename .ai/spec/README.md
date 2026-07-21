# Lightspeed OTel Collector — Specifications

Custom OpenTelemetry collector for OpenShift Lightspeed. Collects, processes, and exports observability data (metrics, traces, logs) across the OLS fleet — from spoke clusters to the hub and to external backends.

## Structure

| Layer | Path | Purpose |
|---|---|---|
| **what/** | `.ai/spec/what/` | Behavioral rules. What the system must do. Implementation-agnostic. |
| **how/** | `.ai/spec/how/` | Codebase navigation. How the code is organized. Implementation-specific. |

## Scope

Covers the custom OTel collector distribution and its configuration. Out of scope: the metrics/traces emitted by OLS components (defined in each component's own spec), hub operator logic (lightspeed-hub), and hub UI (lightspeed-hub-ui).

## Audience

AI agents. Content is optimized for precision and machine consumption.

## Quick Start

| Task | Start here |
|---|---|
| Understand the system | `what/system-overview.md` |
| Understand the data pipeline | `what/pipeline.md` |
| HTTPS Prometheus metrics (OLS-3656) | `what/https-metrics.md` |

## Conventions

- **Rule numbering:** behavioral rules are numbered sequentially within each what/ file.
- **Planned changes:** unimplemented behavior is marked with `[PLANNED]` or `[PLANNED: TICKET-XXXX]` inline next to the rule it affects.
- **Authority:** what/ specs are authoritative for behavior. how/ specs are authoritative for implementation. When they conflict, what/ wins.

## Updating this spec

- **Adding a new component:** create `what/<component>.md` with behavioral rules and `how/<component>.md` with implementation navigation. Add to the quick-start table.
- **Adding rules to an existing component:** append numbered rules to the relevant section in the what/ file. Use `[PLANNED: TICKET]` for unimplemented behavior.
- **After implementation:** remove `[PLANNED]` markers from implemented rules. Update how/ files if code structure changed.
- **When to create a new file vs. extend an existing one:** if the new concern has its own lifecycle, configuration surface, and can be understood independently, it gets its own file. If it's a capability added to an existing component, it goes in that component's file.
