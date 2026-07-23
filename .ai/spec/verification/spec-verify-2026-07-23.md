# Verification Report: lightspeed-otel-collector Spec
Verified: 2026-07-23
Spec root: /Users/xavi/street/github.com/AI/ols/lightspeed-otel-collector/.ai/spec/

## Summary
- 2 constraint violations
- 1 broken or inaccurate internal reference
- 4 internal inconsistencies
- 4 completeness gaps
- 2 cross-repo alignment issues

## Constraint Violations

**CV-1. Constraint 2 violated — silent metrics drop in routing mode.**
`what/collector.md:35` rule 7: "Metrics: no pipeline defined → silently dropped." The actual `config-router.yaml:125` confirms this. This directly violates constraint 2: "The collector MUST NOT drop telemetry data silently. If data cannot be exported, it MUST be buffered... and the failure MUST be observable via the collector's own metrics."

**CV-2. Constraint 3 underspecified — no mTLS for cross-cluster transport.**
`what/pipeline.md` rule 7 states the spoke collector MUST export to the hub's OTLP endpoint. Constraint 3 requires "All cross-cluster telemetry transport MUST use mTLS." No spec file specifies mTLS for the spoke→hub transport path. The TLS section of `what/collector.md` only covers receiver certs, Postgres `sslmode=require`, and system CA for trace export — none of which are mutual TLS for the hub link.

## Reference Issues

**REF-1. `collector.md` embedded config examples are stale.**
- "Direct to PostgreSQL" example (lines 99-147) shows `extensions: [health_check, postgres_admin]`, omitting `file_storage`. Actual `config.yaml:64` has `extensions: [health_check, file_storage, postgres_admin]`. Example also omits `retry_on_failure`, `sending_queue`, and `telemetry` sections.
- "Routing mode" example (lines 149-199) is missing: the `routing/traces` connector, the `traces/lightspeed` pipeline, the `debug` exporter definition, the `file_storage` extension, and the `telemetry` section — all present in actual `config-router.yaml`.

## Internal Inconsistencies

**IC-1. Schema creation responsibility — contradicting claims.**
- `what/postgres-exporter.md:88` rule 22: "No ORM or migration framework. The exporter writes to an existing table — schema creation is the lightspeed-operator's responsibility."
- `what/collector.md:56-57` (Startup Order): "The `postgres_admin` extension bootstraps the database schema, table, and indexes on startup."
- `what/postgres-exporter.md:38` rule 13: "The `postgres_admin` extension creates the schema, table, and indexes on startup using `IF NOT EXISTS`."
Rule 22 contradicts the rest. The `postgres_admin` extension creates the schema; the operator does not.

**IC-2. Hub/spoke deployment modes described but not implemented — missing `[PLANNED]` markers.**
`what/system-overview.md` rules 4-6 and `what/pipeline.md` rules 1, 3, 4, 7 describe hub/spoke deployment modes using MUST/SHOULD language. But `what/collector.md` describes only a single-replica Deployment; the codebase has no hub/spoke configuration, no Prometheus receiver, no cluster identity processors. The footer says "All rules are planned" but the rules lack inline `[PLANNED]` markers, violating README convention: "unimplemented behavior is marked with `[PLANNED]` inline next to the rule it affects."

**IC-3. Prometheus receiver — claimed in `pipeline.md` but absent from spec and code.**
`what/pipeline.md:9` rule 1: "The collector MUST include a Prometheus receiver for scraping OLS component metrics endpoints."
`what/collector.md:11` rule 2 lists only `otlpreceiver`.
`builder-config.yaml:30-32` confirms only the OTLP receiver is compiled in. This rule should be marked `[PLANNED]`.

**IC-4. Traces routing spec incomplete in `collector.md`.**
`what/collector.md:34` rule 7 says traces are "forwarded to a configurable tracing backend." The actual `config-router.yaml` uses a `routing/traces` connector with a dedicated `traces/lightspeed` sub-pipeline — more complex than the spec describes. The `routing/traces` connector is not mentioned in `collector.md` at all.

## Completeness Gaps

**CG-1. No dedicated spec for `postgres_admin` extension.**
The extension has its own package (`extension/postgresadmin/`), config surface, and lifecycle (bootstraps DB schema; serves HTTPS API for GET/DELETE by trace_id). Per README guidance, it warrants a `what/postgres-admin.md`. Currently only mentioned briefly in `collector.md` (rules 12, 22-23) and `postgres-exporter.md` (rule 13).

**CG-2. No `how/` directory.**
README describes `how/` as a first-class spec layer for codebase navigation and implementation content. No how/ files exist.

**CG-3. No `glossary.md`.**
Terms like "OCB," "spoke mode," "hub mode," "templogs," "OTTL" are used without definition.

**CG-4. `system-overview.md` Configuration Surface is empty.**
Lines 33-35 have a single placeholder row: "Specific fields TBD." Meanwhile `what/collector.md` and `what/postgres-exporter.md` have detailed config tables. Should be populated or point to the detailed specs.

## Cross-Repo Alignment Issues

**XR-1. Operator references HTTPS metrics on port 8888 — collector spec doesn't mention it.**
`lightspeed-operator/.ai/spec/what/observability.md` rule 2a: "The OTEL Collector ServiceMonitor scrapes HTTPS `:8888` `/metrics`." The collector spec documents ports 4317 (gRPC), 4318 (HTTP), 13133 (health), and 8080 (admin API) — port 8888 is never mentioned. This is the OTel Collector's built-in telemetry endpoint; it should be documented in the collector spec since the operator depends on it.

**XR-2. Operator references `https_metrics` TLS posture — not described in collector spec.**
`lightspeed-operator/.ai/spec/what/observability.md` rule 2a says the collector ServiceMonitor uses "server TLS only" matching the "https_metrics / admin API TLS posture." The collector spec's TLS section does not mention the collector's own metrics endpoint TLS configuration. The operator treats the collector's metrics endpoint as HTTPS with server-side TLS via service-ca, but this contract is undocumented on the collector side.

## Files Checked

| File | Status |
|---|---|
| `.ai/spec/README.md` | Read |
| `.ai/spec/constraints.md` | Read |
| `.ai/spec/what/collector.md` | Read |
| `.ai/spec/what/pipeline.md` | Read |
| `.ai/spec/what/postgres-exporter.md` | Read |
| `.ai/spec/what/system-overview.md` | Read |
| `.ai/spec/glossary.md` | Does not exist |
| `.ai/spec/how/` | Does not exist |
| `builder-config.yaml` | Cross-checked |
| `config.yaml` | Cross-checked |
| `config-router.yaml` | Cross-checked |
| `lightspeed-operator/.ai/spec/what/observability.md` | Read for cross-repo alignment |
