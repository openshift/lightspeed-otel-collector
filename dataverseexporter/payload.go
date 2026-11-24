package dataverseexporter

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

var memberSeq atomic.Uint64

// memberName returns a unique TAR member name for one flushed batch:
// <prefix>traces-<timestamp>-<seq>.json. The sequence suffix guarantees
// uniqueness even for flushes within the same nanosecond.
func memberName(prefix string, now time.Time) string {
	seq := memberSeq.Add(1)
	return fmt.Sprintf("%straces-%s-%06d.json",
		prefix, now.UTC().Format("2006-01-02T15-04-05.000000000"), seq)
}

// errPayloadTooLarge marks a batch that exceeds the ingress payload cap.
// It is counted and dropped, never retried: identical bytes cannot succeed.
type errPayloadTooLarge struct {
	size  int64
	limit int64
}

func (e *errPayloadTooLarge) Error() string {
	return fmt.Sprintf("packaged payload %d bytes exceeds limit %d bytes", e.size, e.limit)
}

// buildPayload converts one flushed trace batch into the Dataverse wire
// format: a single-element JSON array holding the OTLP JSON serialization
// of the batch (the same object the FileExporter wrote per JSONL line),
// wrapped as the sole member of a gzip TAR. This matches the array/TAR
// framing the Python sidecar produced for rotated backups.
func buildPayload(td ptrace.Traces, prefix string, maxPayloadSize int64) (*bytes.Reader, string, error) {
	batchJSON, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		return nil, "", fmt.Errorf("marshaling traces to JSON: %w", err)
	}
	name := memberName(prefix, time.Now())

	// Wire format: a single-element JSON array holding the OTLP JSON.
	// Write the framing bytes directly to avoid copying batchJSON.
	memberSize := int64(len(batchJSON)) + 2

	var tarball bytes.Buffer
	gz := gzip.NewWriter(&tarball)
	tw := tar.NewWriter(gz)
	if err := tw.WriteHeader(&tar.Header{
		Name:   name,
		Mode:   0o600,
		Size:   memberSize,
		Format: tar.FormatPAX,
	}); err != nil {
		return nil, name, fmt.Errorf("writing tar header: %w", err)
	}
	if _, err := tw.Write([]byte{'['}); err != nil {
		return nil, name, fmt.Errorf("writing tar member: %w", err)
	}
	if _, err := tw.Write(batchJSON); err != nil {
		return nil, name, fmt.Errorf("writing tar member: %w", err)
	}
	if _, err := tw.Write([]byte{']'}); err != nil {
		return nil, name, fmt.Errorf("writing tar member: %w", err)
	}
	if err := tw.Close(); err != nil {
		return nil, name, fmt.Errorf("closing tar: %w", err)
	}
	if err := gz.Close(); err != nil {
		return nil, name, fmt.Errorf("closing gzip: %w", err)
	}

	if int64(tarball.Len()) > maxPayloadSize {
		return nil, name, &errPayloadTooLarge{size: int64(tarball.Len()), limit: maxPayloadSize}
	}
	return bytes.NewReader(tarball.Bytes()), name, nil
}
