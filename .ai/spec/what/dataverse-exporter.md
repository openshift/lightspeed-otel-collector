# Dataverse Exporter

[PLANNED: OLS-4493] Custom OTel Collector exporter that batches selected
Agentic OTLP traces in memory and uploads them to the Red Hat ingress service
for Dataverse. It replaces the FileExporter trace branch in
[`data-collection.md`](data-collection.md). All rules in this file are planned.
The cross-repository contract is `.ai/spec/what/agentic-data-collection.md`
in the `openshift/ols` workspace repo; the decision is its ADR 0052.

## Behavioral Rules

### Component

1. The exporter is a Go module `dataverseexporter/` in this repository,
   registered in `builder-config.yaml`. Its component type is `dataverse`. It
   supports the traces signal only.
2. The exporter receives traces only from `traces/data_collection`. Resource
   selection by `routing/data_collection` is unchanged; see
   [`data-collection.md`](data-collection.md) rules 1-6.
3. The configuration embeds the standard `confighttp.ClientConfig`
   (`endpoint`, `timeout`, `tls`, `proxy_url`), the `exporterhelper` queue and
   batch settings, and `configretry`. These keys behave as in other exporters.
4. Exporter-specific keys are `service_id` (required, `ols` or
   `rhos-lightspeed`), `archive_path_prefix`, and `pull_secret`
   (`namespace`, `name`, `registry`).
5. The reference configuration is:

   ```yaml
   dataverse:
     endpoint: https://console.redhat.com/api/ingress/v1/upload
     timeout: 60s
     tls:
       min_version: "1.2"
       ca_file: /etc/certs/trusted-ca/ca-bundle.crt
     service_id: ols
     archive_path_prefix: agentic-v1/
     pull_secret:
       namespace: openshift-config
       name: pull-secret
       registry: cloud.openshift.com
     sending_queue:
       enabled: true
       sizer: bytes
       queue_size: 33554432
       num_consumers: 1
       batch:
         flush_timeout: 5m
         min_size: 4194304
         max_size: 4194304
     retry_on_failure:
       enabled: true
       initial_interval: 30s
       max_interval: 5m
       max_elapsed_time: 1h
   ```

   Defaults equal these values, except `service_id`. These are reference
   values; the operator owns the deployed configuration.

### Packaging and Upload

6. Each batch is encoded with `ptrace.JSONMarshaler` as one native OTLP
   trace-batch object. The object keeps the `resourceSpans[] -> scopeSpans[]
   -> spans[] -> events[]` nesting.
7. The exporter wraps the object in a one-element JSON array. It stores the
   array as the only member of an in-memory gzip TAR. The member name is
   `<archive_path_prefix>traces-<UTC yyyy-mm-ddThh-mm-ss.mmm>-<8 hex>.json`.
8. A TAR larger than 100 MiB is a permanent error.
9. The upload is `POST` to `endpoint` with multipart field `file`, filename
   `lightspeed-assistant.tgz`, and part content type
   `application/vnd.redhat.<service_id>.periodic+tar`.
10. The request headers are `Authorization: Bearer <registry auth>` and
    `User-Agent: openshift-lightspeed-operator/user-data-collection cluster/<clusterID>`.
11. HTTP 202 is success and acknowledges the whole batch. A 202 without a
    parsable `request_id` is still success.

### Credentials

12. The exporter reads the projected ServiceAccount token and CA from
    `/var/run/secrets/kubernetes.io/serviceaccount/`. It calls the Kubernetes
    API with plain `net/http`.
13. It reads the auth of `pull_secret.registry` from `.dockerconfigjson` of
    the configured Secret. It reads `spec.clusterID` from ClusterVersion
    `version`.
14. The exporter loads credentials lazily on the first upload attempt, never
    in `Start`. Missing credentials or an unreachable API server MUST NOT stop
    the Collector.
15. The exporter caches the cluster ID after the first success. It re-reads
    the pull-secret auth before every upload attempt.
16. The Kubernetes API client trusts only the ServiceAccount `ca.crt`. It sets
    no proxy and connects to `KUBERNETES_SERVICE_HOST` directly.
17. The exporter MUST NOT log the token or the registry auth. Errors name the
    Secret and the missing key only.

### TLS and Proxy

18. Every TLS connection the exporter opens uses the configured
    `tls.min_version` and `tls.cipher_suites`. This includes the Kubernetes
    API client.
19. With `proxy_url` unset, uploads follow `https_proxy` and `no_proxy` from
    the process environment.
20. The upload client trusts the CAs in `tls.ca_file`.

### Errors and Isolation

21. Network errors, timeouts, HTTP 408, 429, 5xx, and Kubernetes API errors
    while loading credentials are retryable.
22. A missing registry auth, malformed `.dockerconfigjson`, HTTP 401, 403,
    other 4xx, and an oversized TAR are permanent errors
    (`consumererror.NewPermanent`). The exporter drops the batch.
23. When the queue is full, the exporter drops the data, counts the dropped
    spans, logs a rate-limited warning, and returns `nil`. Producers and
    sibling trace consumers MUST NOT receive an error caused by this exporter.
24. The in-memory queue and the open batch are lost when the Collector process
    stops, except data drained within the termination grace period.

### Telemetry and Logging

25. Standard `exporterhelper` metrics report sent spans, failed spans, and
    queue usage.
26. The exporter counts dropped spans with attribute `reason` =
    `queue_full` or `permanent_error`, unless an existing `exporterhelper`
    metric reports the same count.
27. Each successful upload logs one info line, `Uploaded Agentic trace batch`,
    with fields `request_id`, `archive_member`, `spans`, `payload_bytes`, and
    `duration`.
28. A failed attempt logs the HTTP status or network error, `archive_member`,
    and a truncated response body. The level is warning when the attempt
    will be retried and error when the batch is dropped.
29. Messages carry no component prefix. The Collector logger adds
    `otelcol.component.id`.
