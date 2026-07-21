// Package httpsmetrics implements an OTel Collector extension that serves
// the Collector's internal Prometheus metrics over HTTPS. It reverse-proxies
// GET /metrics to the stock Prometheus pull endpoint (localhost-only) and
// terminates TLS using the same service-ca serving certificate as OTLP and
// postgres_admin.
package httpsmetrics
