# Constraints

Project-wide invariants. If an agent violates any of these, the system is wrong.

1. The collector MUST be built as a custom OpenTelemetry Collector distribution using the OTel Collector Builder (ocb). No forking the upstream collector.
2. The collector MUST NOT drop telemetry data silently. If data cannot be exported, it MUST be buffered (with bounded memory) and the failure MUST be observable via the collector's own metrics.
3. All cross-cluster telemetry transport MUST use mTLS.
4. The collector MUST NOT require spoke-side configuration changes when new metrics/traces are added to OLS components — it should collect what's available.
5. The collector's resource footprint MUST be bounded and configurable. It runs as a sidecar or standalone pod and must not consume unbounded memory or CPU.
6. Commit messages and PR titles MUST start with `OLS-XXXX` (Jira ticket reference).
7. Fork-based git workflow: push to your fork, PR against `origin/main`.
