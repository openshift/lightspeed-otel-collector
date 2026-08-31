# Spec health report

Last evaluated: 2026-08-31
Trigger: post-milestone: spec drift audit (OLS-3696 templog schema, OLS-3745 e2e, Konflux onboarding)
Layout: software (.ai/spec/)

## Stale

- **`what/collector.md` rule 20** — "The image is built and shipped via Konflux (pipeline definition is a separate ticket)." The Konflux pipeline definitions now exist in `.tekton/` (`lightspeed-otel-collector-pull-request.yaml`, `lightspeed-otel-collector-push.yaml`) plus integration-tests. The "separate ticket" framing is stale. *(fixed)*
- **`what/https-metrics.md` "Why" #1** — cites "Upstream otelcol **0.155**". `builder-config.yaml` now pins `otelcol_version: 0.159.0`. The underlying limitation (otelconf prometheus pull reader has no TLS fields) still holds — the `https_metrics` extension is still compiled in — but the version number is stale. *(fixed → 0.159.0)*
- **`README.md` Quick Start table** — lists only `system-overview.md`, `pipeline.md`, `https-metrics.md`. Omits `collector.md` and `postgres-exporter.md`, which both exist and are core what/ files. *(fixed)*
- **`what/collector.md` Repository Contents table** — omits `extension/httpsmetrics/` (which the same file documents in rules 2/13 and TLS section), `cmd/`, `.tekton/`, and `test/e2e/`. All exist in the repo. *(fixed)*

## Missing

- **`what/postgres-exporter.md` rule 2** — the exporter skips log records that lack the `agenticrun.uid` attribute and emits a `Warn` log ("skipped log records missing agenticrun.uid attribute"); such records are not written to PostgreSQL. This is shipped, observable behavior (`postgresexporter/exporter.go`) not captured by the spec. *(fixed — added as rule 2a)*
- No `how/` files exist. The `README.md` conventions ("how/ specs are authoritative for implementation", "create `how/<component>.md`") describe a what/how split that is not realized — all implementation navigation currently lives inline in `what/collector.md` ("Implementation map", "Repository Contents") and `what/postgres-exporter.md` ("Implementation", "Go Package Structure"). Not fixed (see Structural concerns).

## Structural concerns

- **what/how separation not observed.** `what/collector.md` and `what/postgres-exporter.md` embed code-navigation content (Go package layout, repository contents, implementation maps) that the README convention assigns to `how/` files. Whether to introduce a `how/` layer or update the README to describe the actual single-layer structure is a design decision left to a human — not changed here to avoid restructuring or removing intended future structure.
- `README.md` "Structure" table advertises only the **what/** layer, while the tree also contains `constraints.md`, `decisions/`, and `verification/`. Left as-is (the table documents the primary behavioral layer, not every directory).

## Findability issues

- None material. The Quick Start and Cross-References additions (this pass) close the main gap: `collector.md` and `postgres-exporter.md` were reachable only via cross-links from other files, not from the README entry point.

## No issues (verified current)

- Templog schema (OLS-3696): `agentic_run_id`, `phase`, `event`, `body` columns and the `agentic_run_id` / `(agentic_run_id, phase)` indexes in `extension/postgresadmin/extension.go` match `what/postgres-exporter.md` and `what/collector.md`. The `trace_id → agentic_run_id` rename is fully reflected in the specs.
- Admin API plain-text format (OLS-3515): `format=text` behavior, `records` / `has_more` header lines match `what/collector.md` rule 12.
- `https_metrics` extension (OLS-3656): ports (8888 HTTPS / 18888 localhost pull), TLS paths, `without_type_suffix`/`without_units`, reference-config wiring match code and both `config.yaml`/`config-router.yaml`. `[IMPLEMENTED: OLS-3656]` marker is accurate.
- Metrics-drop `[KNOWN VIOLATION of Constraint 2]` / `[PLANNED]` in `what/collector.md` rule 7 is still accurate: neither `config.yaml` nor `config-router.yaml` defines a metrics pipeline.
- Builder components (`what/collector.md` rule 2) match `builder-config.yaml` exactly (otlp receiver; postgres/otlp/otlphttp/debug/nop exporters; batch processor; routing connector; healthcheck/filestorage/postgresadmin/httpsmetrics extensions).
- Remaining `[PLANNED]` markers (hub/spoke modes, prometheus receiver, cluster-identity processor) correctly describe unimplemented behavior — no code exists for them yet.
