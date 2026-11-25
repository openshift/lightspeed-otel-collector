// Package dataverseexporter implements an OpenTelemetry Collector exporter
// that pushes trace batches to the Red Hat Dataverse ingress API, replacing
// the FileExporter spool and Python sidecar uploader (OLS-4493 Phase 2.1).
//
// Permanent upload failures and oversized archives return success to exporterhelper,
// so sent_spans includes these drops. The Dataverse dropped_batches and dropped_spans
// counters distinguish them from successful uploads and also count queue refusals.
package dataverseexporter
