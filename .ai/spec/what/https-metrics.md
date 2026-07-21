# HTTPS Metrics Endpoint

[IMPLEMENTED: OLS-3656]

Expose the Collector’s internal Prometheus metrics to cluster Prometheus over **HTTPS**, using the same service-ca serving certificate as OTLP and `postgres_admin`.

Companion work (out of scope here): lightspeed-operator Service, ServiceMonitor (`scheme: https`), and NetworkPolicy for `openshift-monitoring` → `:8888`.

## Why

1. Upstream otelcol **0.155** `service.telemetry.metrics.readers[].pull.exporter.prometheus` only starts a plain HTTP server (`host` / `port`). The otelconf `Prometheus` config has **no TLS fields**.
2. Default telemetry bind is effectively localhost-oriented; cluster scrapers need a cluster-facing endpoint.
3. This distribution’s TLS rule (see `collector.md`) requires TLS for external channels. Metrics scraped by Prometheus from another namespace are external.

## Design

Two listeners:

| Port | Bind | Protocol | Role |
|------|------|----------|------|
| **18888** | `127.0.0.1` | HTTP | Stock otelcol Prometheus pull (`/metrics`) — not cluster-reachable |
| **8888** | `0.0.0.0` | HTTPS | Cluster-facing `/metrics` via new extension (reverse proxy) |

Keep **8888** as the public metrics port so the operator can wire ServiceMonitor without renumbering.

Server TLS only for OLS-3656 (encrypt + identity via service-ca). App-server-style mTLS + Bearer scrape auth is **out of scope** (follow-up if required).

## Behavioral Rules

1. The distribution includes a custom extension (`https_metrics`) that serves Prometheus metrics over HTTPS.
2. The extension listens on a configurable `endpoint` (production default: `0.0.0.0:8888`).
3. The extension reverse-proxies `GET /metrics` to a configurable upstream URL (production default: `http://127.0.0.1:18888/metrics`).
4. When `tls_cert_file` and `tls_key_file` are set, the extension uses `http.Server.ServeTLS` with those paths (same serving-cert mount as admin/OTLP: `/var/run/secrets/serving-cert/tls.{crt,key}`).
5. TLS file validation matches `postgres_admin`: both cert and key must be set together, or both empty. Local-dev behavior without TLS matches `postgres_admin` (plain HTTP allowed when both empty).
6. Stock collector telemetry must expose the Prometheus pull reader on **localhost only** so metrics are not reachable unencrypted from the cluster network:
   ```yaml
   service:
     telemetry:
       metrics:
         readers:
           - pull:
               exporter:
                 prometheus:
                   host: '127.0.0.1'
                   port: 18888
                   without_type_suffix: true
                   without_units: true
   ```
7. `without_type_suffix: true` and `without_units: true` MUST be set when configuring custom readers (upstream defaults drop these flags and change metric names).
8. The extension is registered in `builder-config.yaml` and enabled in reference configs (`config.yaml`, `config-router.yaml`) and in operator-generated runtime config (operator follow-up).
9. No new OCB pipeline components (`prometheusexporter` / `prometheusreceiver`) are required for internal metrics; those metrics come from service telemetry. The extension only fronts the existing pull endpoint over HTTPS.
10. Unit tests cover config validation and proxy/handler behavior; TLS listen smoke tests where practical.

## Production config shape (operator will generate later)

```yaml
extensions:
  https_metrics:
    endpoint: 0.0.0.0:8888
    upstream: http://127.0.0.1:18888/metrics
    tls_cert_file: /var/run/secrets/serving-cert/tls.crt
    tls_key_file: /var/run/secrets/serving-cert/tls.key

service:
  extensions: [health_check, file_storage, postgres_admin, https_metrics]
  telemetry:
    logs:
      level: info
    metrics:
      readers:
        - pull:
            exporter:
              prometheus:
                host: '127.0.0.1'
                port: 18888
                without_type_suffix: true
                without_units: true
```

Final extension type name and mapstructure field names may match repo conventions; keep the ports and TLS paths stable unless documented otherwise.

## Implementation map (in-repo patterns)

| Copy from | For |
|-----------|-----|
| `extension/postgresadmin/` | Extension layout: `config.go`, `factory.go`, `extension.go`, Validate, Start/Shutdown, TLS |
| `builder-config.yaml` | Register custom extension gomod + `path:` |
| `config.yaml` | Wire extension + serving-cert paths |

## Acceptance checklist (this repo)

- [ ] Binary includes the new extension
- [ ] With TLS files set, `https://<host>:8888/metrics` returns collector internal metrics
- [ ] Localhost HTTP pull on `18888` still works (upstream of the proxy)
- [ ] Config validation and unit tests pass (`make test`)
- [ ] Reference configs and `.ai/spec` updated; remove `[PLANNED]` markers when done
- [ ] Image builds via existing Dockerfile/Makefile

## Out of scope

- lightspeed-operator ServiceMonitor / NetworkPolicy / Deployment ports / `related_images` bump
- Upstream otelconf TLS support for the prometheus pull exporter
- mTLS or Bearer authentication on the scrape endpoint

## Related

- Jira: [OLS-3656](https://redhat.atlassian.net/browse/OLS-3656)
- Spec: `what/collector.md` (TLS, ports, extensions)
- Operator companion: expose Service port `metrics`/:8888, ServiceMonitor HTTPS + service-ca, NP for Prometheus in `openshift-monitoring`
