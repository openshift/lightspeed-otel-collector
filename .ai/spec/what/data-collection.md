# Trace Data Collection

This file defines the Collector contract for exporting selected OTLP traces
to the Red Hat Dataverse ingress service through the `dataverse` exporter.
It covers service-based resource selection, queueing and batching, upload
behavior, credentials, and failure isolation. The contract ends at the
ingress HTTP 202 acknowledgement; downstream Dataverse processing belongs to
the data product.

This supersedes the FileExporter JSONL file contract under OLS-4493
(decision 0052 in the parent `openshift/ols` spec); the parent
[agentic data collection contract](https://github.com/openshift/ols/blob/main/.ai/spec/what/agentic-data-collection.md)
is authoritative for cross-repo rules 24-37, and this file is the
Collector-internal view. All rules are
[PLANNED: OLS-4493] unless marked otherwise.

## Resource Selection and Pipeline

1. The product branch MUST consume OTLP traces only. OTLP logs and metrics
   MUST NOT enter the branch; existing PostgreSQL log handling, admin APIs,
   internal metrics, and configured trace forwarding or `nop` behavior
   remain separate.
2. The `routing/data_collection` connector MUST evaluate the resource
   attribute `service.name` and route only exact matches for
   `lightspeed-agentic-operator` or `lightspeed-agentic-sandbox` to
   `traces/data_collection`. It MUST have no default product pipeline.
3. Resources with missing or non-matching `service.name` values are excluded
   from product export.
4. For a matched resource, the branch MUST retain every span and attached
   event in its original OTLP context, including resource and scope
   metadata, attributes, IDs, timestamps, and links.
5. The trace input MUST fan out to the product-selection connector and the
   configured all-trace backend route or direct-config `nop` sink. Each
   destination receives its trace data intact. The `traces/data_collection`
   pipeline MUST remain unbatched at the pipeline level; batching belongs to
   the `dataverse` exporter's own sending queue, where byte-based splitting
   is bounded by `batch.max_size` rather than vulnerable to count-based
   combination above a write limit.
6. Internal telemetry uses the standard Collector and component
   instrumentation configured for the distribution. The exporter adds
   `otelcol_dataverse_exporter_dropped_batches` and
   `otelcol_dataverse_exporter_dropped_spans` counters with a `reason`
   attribute (`credentials`, `http_<status>`, `payload_too_large`,
   `queue_full`); the standard `sent_spans` metric counts dropped batches
   as sent for drops inside the push function, not queue refusals. Retry
   exhaustion is reported by `otelcol_exporter_send_failed_spans`, not the
   custom drop counters.

## The dataverse Exporter

7. The distribution MUST provide the `dataverse` exporter from
   `github.com/openshift/lightspeed-otel-collector/dataverseexporter`,
   registered in `builder-config.yaml` as component type `dataverse`,
   traces signal only, built on `exporterhelper`. The exporter replaces
   `file/data_collection` as the product-branch terminus; no product trace
   files are written or read.
8. The exporter uses the stock `exporterhelper` sending queue and retry:
   memory-only queue with `sizer: bytes`, `block_on_overflow: false`, and
   an explicit `batch:` section (which activates queue batching without the
   exporterhelper feature gate) providing size-bounded batches and a
   `flush_timeout` that guarantees delivery at any volume, including quiet
   clusters. Queue, batch, and retry values are operator-owned deployment
   configuration; the parent contract records the target values and the
   measured protobuf-sizer/JSON-expansion basis for them.
9. The push function serializes each flushed batch with the OTLP JSON
   marshaller, wraps it as one top-level JSON array, and uploads it as the
   sole member of a gzip TAR under the configured `archive_path_prefix`.
   A packaged payload above `max_payload_size` MUST be a counted permanent
   drop, never truncated. The multipart wire values, headers, and 202
   acknowledgement are frozen by the parent contract (rule 26) and MUST
   remain byte-compatible with the retired Python sidecar.
10. The exporter's HTTP client comes from the standard `confighttp`
    client config: egress honors the cluster-wide proxy environment
    variables, TLS settings follow `configtls`, and `insecure_skip_verify`
    MUST be rejected by config validation. The exporter MUST NOT embed
    assumptions that break on IPv4-only or IPv6-only clusters.

## Credentials and Failure Behavior

11. Credentials resolve lazily on the first upload attempt, never at
    Collector startup: the pull-secret token and ClusterVersion cluster ID
    are read over the in-cluster Kubernetes API with the pod's projected
    ServiceAccount token, using a minimal REST client (no client-go) that
    is never proxied and trusts only the ServiceAccount CA. Structural
    in-cluster prerequisites (env vars, ServiceAccount CA) fail fast at
    exporter creation. The token is cached and re-read only on an ingress
    401, rate-limited to at most one re-read per minute.
12. Upload errors classify as retryable (network, timeout, HTTP
    408/429/5xx, 401 via the refresh path, API-server availability) with
    the configured retry budget, or permanent counted drops (credential
    shape errors, other 4xx, oversized payloads) via
    `consumererror.NewPermanent` converted to counted drops.
13. When the queue refuses a batch, the wrapper MUST count the drop
    (`queue_full`), rate-limit the warning, and return success to the caller.
    The exporter's queue or upload problems MUST NOT fail a producer request
    or cause another trace consumer to receive duplicates. Shutdown drains the
    queue within the process termination window; `ConsumeTraces` after
    `Shutdown` is silently dropped by exporterhelper v0.159.0 (pinned by
    test).
14. Collector image and generated configuration MUST be rolled out
    together: an image without the `dataverse` exporter cannot run a
    configuration that references it.

For runnable standalone build and fail-fast smoke steps, see
[`README.md`](../../../README.md).

## Verification

Rule-to-verification map. Unit tests live in `dataverseexporter/` (run by
`make test`); e2e tests live in `test/e2e/` (build tag `e2e`, run in the
Konflux IntegrationTestScenario via `make test-e2e`). Parent rules refer
to `openshift/ols` `.ai/spec/what/agentic-data-collection.md`.

| Rule | Verified by |
|---|---|
| 1 | Unit: `postgresexporter` suite + `dataverseexporter` factory tests keep log/trace paths separate. E2e: dataverse collector configs route only the `traces` signal. |
| 2, 3 | E2e: `TestDataverseQuietClusterSelectionAndFlush` (allowlisted service uploads; `something-else` never does). Unit (pipeline shape): e2e config mirrors `routing/data_collection` table. |
| 4 | Unit: `TestBuildPayload` (full OTLP JSON round-trip). E2e: `TestDataverseQuietClusterSelectionAndFlush` asserts the TAR member's JSON array carries the span content. |
| 5 | E2e: `TestDataverseQuietClusterSelectionAndFlush` (flush_timeout-only delivery, no pipeline-level batch processor in the dataverse configs). Unit: `TestQueueFlushTimeoutCoversQuietClusters`. |
| 6 | Unit: `TestConsumeTracesDropsPermanentUploadErrors`, `TestWrapperSwallowsQueueFullAndCountsDrop`, `TestBuildPayloadTooLarge`. E2e: drop reasons asserted from collector logs in `TestDataverseCredentialDegradationAndSelfHeal` (`credentials`) and `TestDataversePermanentDropIsCounted` (`http_403`); metric scraping is out of e2e scope. |
| 7 | Unit: `TestNewFactoryType`, `TestCreateTracesExporterOutsideCluster`. E2e: every dataverse test runs a collector image whose config references the `dataverse` exporter. |
| 8 | E2e: `TestDataverseQuietClusterSelectionAndFlush` (flush_timeout delivery), `TestDataverseRetryThenRecover` (retry budget), `TestDataverseQueueFullIsolation` (queue/batch bounds). Unit: `TestQueueFlushTimeoutCoversQuietClusters`, `TestShutdownDrainsQueuedBatch`. |
| 9 | E2e: `TestDataverseQuietClusterSelectionAndFlush` asserts the full wire shape (multipart field/filename/part content type, single gzip TAR member `agentic-v1/traces-*.json`, one-element JSON array, 202). Unit: `TestBuildPayload`, `TestUploadSuccess`, `TestBuildPayloadCompressibleBatch`, `TestBuildPayloadTooLarge` (oversize → counted drop, never truncated). |
| 10 | Unit: `TestConfigValidate` (https-only, `insecure_skip_verify` rejected), `TestConfigMapstructureRoundTrip`. E2e: all dataverse configs use `https` + `tls.ca_file` against the fake ingress. |
| 11 | E2e: `TestDataverseCredentialDegradationAndSelfHeal` (availability path: RBAC-revoked credential read is retryable, no crashloop, same batch delivered after re-grant with no restart; non-empty bearer token and real cluster ID on the wire — the token value is not asserted because hosted-cluster pull-secrets are controller-managed). Unit: `TestStartIsLazyAboutCredentials`, `TestUploadToken*`, `TestClusterID*`, `TestNewClusterAuth*`. |
| 12 | E2e: `TestDataverseRetryThenRecover` (5xx retryable, late delivery), `TestDataversePermanentDropIsCounted` (403 permanent, one attempt), credential shape permanent drop is unit-only (hosted-cluster pull-secrets are controller-managed, so the no-entry state cannot be created at cluster level). Unit: `TestUploadPermanentHTTPStatuses`, `TestUploadRetryableHTTPStatuses`, `TestUploadCredentialShapeErrorsArePermanent`, `TestUploadCredentialAvailabilityErrorsAreRetryable`, `TestConsumeTracesRetriesUploadErrors`. |
| 13 | E2e: `TestDataverseQueueFullIsolation` (queue full: producer-side OTLP exports keep succeeding; warning logged). Unit: `TestWrapperSwallowsQueueFullAndCountsDrop`, `TestQueueFullErrorIsSwallowedEndToEnd`, `TestConsumeAfterShutdownIsDropped`. |
| 14 | Verified only by inspection/review: the image and generated configuration are rolled out together by the operator/deployment flow; no automated test exercises the rollout coupling. |
