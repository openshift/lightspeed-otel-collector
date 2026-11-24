package dataverseexporter

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/ptrace"
)

func testTraces() ptrace.Traces {
	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "lightspeed-agentic-sandbox")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetName("execute_tool")
	span.SetTraceID([16]byte{1})
	span.SetSpanID([8]byte{2})
	return td
}

func readTarMember(t *testing.T, raw []byte) (name string, content []byte) {
	t.Helper()
	gz, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("gzip: %v", err)
	}
	defer gz.Close()
	tr := tar.NewReader(gz)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("tar next: %v", err)
	}
	content, err = io.ReadAll(tr)
	if err != nil {
		t.Fatalf("tar read: %v", err)
	}
	if _, err := tr.Next(); err != io.EOF {
		t.Error("expected exactly one tar member")
	}
	return hdr.Name, content
}

func TestBuildPayload(t *testing.T) {
	td := testTraces()
	payload, _, err := buildPayload(td, "agentic-v1/", DefaultMaxPayloadSize)
	if err != nil {
		t.Fatalf("buildPayload: %v", err)
	}
	raw, err := io.ReadAll(payload)
	if err != nil {
		t.Fatal(err)
	}
	gotName, content := readTarMember(t, raw)

	wantJSON, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{'['}, wantJSON...), ']')
	if !bytes.Equal(content, want) {
		t.Errorf("member content mismatch:\n got: %s\nwant: %s", content, want)
	}
	const wantPrefix, wantSuffix = "agentic-v1/traces-", ".json"
	if len(gotName) < len(wantPrefix)+len(wantSuffix) ||
		gotName[:len(wantPrefix)] != wantPrefix ||
		gotName[len(gotName)-len(wantSuffix):] != wantSuffix {
		t.Errorf("member name = %q, want %s<timestamp>-<seq>%s", gotName, wantPrefix, wantSuffix)
	}
}

func TestBuildPayloadMemberNameUniqueness(t *testing.T) {
	td := testTraces()
	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		_, name, err := buildPayload(td, "agentic-v1/", DefaultMaxPayloadSize)
		if err != nil {
			t.Fatal(err)
		}
		if seen[name] {
			t.Fatalf("duplicate member name %q", name)
		}
		seen[name] = true
	}
}

func TestBuildPayloadEmptyTraces(t *testing.T) {
	td := ptrace.NewTraces()
	payload, _, err := buildPayload(td, "", DefaultMaxPayloadSize)
	if err != nil {
		t.Fatalf("buildPayload: %v", err)
	}
	raw, err := io.ReadAll(payload)
	if err != nil {
		t.Fatal(err)
	}
	gotName, content := readTarMember(t, raw)

	wantJSON, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte{'['}, wantJSON...), ']')
	if !bytes.Equal(content, want) {
		t.Errorf("member content mismatch:\n got: %s\nwant: %s", content, want)
	}
	const wantPrefix, wantSuffix = "traces-", ".json"
	if len(gotName) < len(wantPrefix)+len(wantSuffix) ||
		gotName[:len(wantPrefix)] != wantPrefix ||
		gotName[len(gotName)-len(wantSuffix):] != wantSuffix {
		t.Errorf("member name = %q, want %s<timestamp>-<seq>%s", gotName, wantPrefix, wantSuffix)
	}
}

func TestBuildPayloadCompressibleBatch(t *testing.T) {
	td := testTraces()
	td.ResourceSpans().At(0).Resource().Attributes().PutStr("text", strings.Repeat("a", 8192))
	payload, _, err := buildPayload(td, "agentic-v1/", 1024)
	if err != nil || payload.Size() > 1024 {
		t.Fatalf("compressible archive must fit: %v", err)
	}
}

func TestBuildPayloadTooLarge(t *testing.T) {
	td := testTraces()
	payload, _, err := buildPayload(td, "agentic-v1/", 16)
	var tooLarge *errPayloadTooLarge
	if !errors.As(err, &tooLarge) {
		t.Fatalf("err = %v, want errPayloadTooLarge", err)
	}
	if payload != nil {
		t.Error("expected nil reader on error")
	}
}
