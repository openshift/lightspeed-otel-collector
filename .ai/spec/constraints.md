# Constraints

Project-wide invariants. If an agent violates any of these, the system is wrong.

1. The collector MUST be built as a custom OpenTelemetry Collector distribution using the OTel Collector Builder (ocb). No forking the upstream collector.
2. Delivery follows the configured signal pipelines and exporters. The reference PostgreSQL log pipeline uses a file-backed sending queue and retries; the trace data collection pipeline uses FileExporter's write, rotation, and retention behavior. See `what/collector.md` and `what/data-collection.md`.
3. All cross-cluster telemetry transport MUST use mTLS.
   _(Implementation status: mTLS for spoke→hub transport is not yet specified in the pipeline spec. See `[PLANNED]` markers in `what/pipeline.md` rule 7.)_
4. The collector MUST NOT require spoke-side configuration changes when new metrics/traces are added to OLS components — it should collect what's available.
5. The collector's resource footprint MUST be bounded and configurable. It runs as a sidecar or standalone pod and must not consume unbounded memory or CPU.
6. Commit messages and PR titles MUST start with `OLS-XXXX` (Jira ticket reference).
7. Fork-based git workflow: push to your fork, PR against `origin/main`.
