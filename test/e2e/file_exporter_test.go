//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

const (
	activeTraceFileName      = "traces.jsonl"
	nativeTraceWaitTimeout   = 45 * time.Second
	nativeTracePollInterval  = 500 * time.Millisecond
	rotationPayloadBytes     = 384 * 1024
	rotationWriteLimit       = 12
	resourceSchemaURL        = "https://example.test/otel/resource/v1"
	scopeSchemaURL           = "https://example.test/otel/scope/v1"
	requestMarkerAttribute   = "e2e.request_marker"
	rotationMarkerPrefix     = "e2e-rotation"
	rotationPayloadAttribute = "e2e.rotation_payload"
)

func TestFileExporterRuntime(t *testing.T) {
	testStarted := time.Now().Add(-time.Second)
	marker := fmt.Sprintf("native-file-%d", time.Now().UnixNano())
	request := newMixedResourceTraceRequest(marker)

	sendFileTraceRequest(t, request)
	batches := waitForNativeTraceBatches(t, marker)
	assertMixedResourceTraceOutput(t, request, batches, marker)
	waitForDebugTrace(t, testStarted, 10*time.Second)
	assertCollectorHealthy(t)
}

func TestFileExporterRotationRetention(t *testing.T) {
	rotationID := fmt.Sprintf("%s-%d", rotationMarkerPrefix, time.Now().UnixNano())
	markers := make([]string, 0, rotationWriteLimit)
	for index := range 3 {
		marker := fmt.Sprintf("%s-%02d", rotationID, index)
		sendRotationTrace(t, marker, byte(index+1))
		waitForNativeTraceBatches(t, marker)
		markers = append(markers, marker)
	}

	oldestBackup := waitForNativeTraceBackup(t, markers[0])
	assertRotationPayloadInBackup(t, oldestBackup, markers[0])

	retentionObserved := false
	for index := 3; index < rotationWriteLimit; index++ {
		marker := fmt.Sprintf("%s-%02d", rotationID, index)
		sendRotationTrace(t, marker, byte(index+1))
		waitForNativeTraceBatches(t, marker)
		markers = append(markers, marker)

		files, complete := readNativeTraceFiles(t)
		if complete && !nativeTraceFilesContainSpanName(files, markers[0]) {
			retentionObserved = true
			break
		}
	}
	if !retentionObserved {
		t.Fatalf("oldest rotated trace marker %q remained after %d separate writes", markers[0], len(markers))
	}

	files := waitForNativeTraceRetention(t, markers[0])
	backups := closedTraceFiles(files)
	if len(backups) != 2 {
		t.Fatalf("retained closed trace files = %d, want 2", len(backups))
	}
	if nativeTraceFilesContainSpanName(files, markers[0]) {
		t.Fatalf("oldest rotated trace marker %q remained after retention", markers[0])
	}
	var retainedNewerSpan bool
	for _, backup := range backups {
		for _, batch := range backup.batches {
			for _, resourceSpans := range batch.ResourceSpans {
				for _, scopeSpans := range resourceSpans.ScopeSpans {
					for _, span := range scopeSpans.Spans {
						if strings.HasPrefix(span.Name, rotationID+"-") && span.Name != markers[0] {
							retainedNewerSpan = true
						}
					}
				}
			}
		}
	}
	if !retainedNewerSpan {
		t.Fatal("retained closed trace files did not contain a newer rotation span")
	}
}

func sendFileTraceRequest(t *testing.T, request *collectortracepb.ExportTraceServiceRequest) {
	t.Helper()
	connection, err := grpc.NewClient(
		env.Endpoints.OTLPgRPC,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial collector: %v", err)
	}
	defer func() { _ = connection.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if _, err := collectortracepb.NewTraceServiceClient(connection).Export(ctx, request); err != nil {
		t.Fatalf("export trace request: %v", err)
	}
}

func waitForNativeTraceBatches(
	t *testing.T,
	marker string,
) []*collectortracepb.ExportTraceServiceRequest {
	t.Helper()
	deadline := time.Now().Add(nativeTraceWaitTimeout)
	for time.Now().Before(deadline) {
		files, complete := readNativeTraceFiles(t)
		if complete {
			batches := flattenNativeTraceFiles(files)
			if nativeTraceBatchesContainSpanName(batches, marker) {
				return batches
			}
		}
		time.Sleep(nativeTracePollInterval)
	}
	t.Fatalf("timed out waiting for a native trace span name containing %q", marker)
	return nil
}

func newMixedResourceTraceRequest(marker string) *collectortracepb.ExportTraceServiceRequest {
	now := uint64(time.Now().UnixNano())
	resources := []struct {
		service string
		label   string
	}{
		{service: "lightspeed-agentic-operator", label: "operator"},
		{service: "lightspeed-agentic-sandbox", label: "sandbox"},
		{service: "lightspeed-service", label: "classic"},
		{service: "e2e-unrecognized-service", label: "unknown"},
		{label: "missing-service"},
	}

	request := &collectortracepb.ExportTraceServiceRequest{}
	for index, item := range resources {
		resourceAttributes := []*commonpb.KeyValue{
			stringAttribute(requestMarkerAttribute, marker),
			stringAttribute("e2e.resource_kind", item.label),
			intAttribute("e2e.resource_ordinal", int64(index)),
			boolAttribute("e2e.resource_enabled", true),
			doubleAttribute("e2e.resource_sample_rate", 0.875),
			bytesAttribute("e2e.resource_token", []byte{0x00, byte(index), 0xfe}),
			keyValue("e2e.resource_context", keyValueListValue(
				stringAttribute("environment", "cluster-e2e"),
				intAttribute("revision", 42),
			)),
		}
		if item.service != "" {
			resourceAttributes = append(resourceAttributes, stringAttribute("service.name", item.service))
		}
		resource := &resourcepb.Resource{
			Attributes:             resourceAttributes,
			DroppedAttributesCount: 1,
		}

		start := now + uint64(index)*1000
		span := &tracepb.Span{
			TraceId:           traceID(byte(index + 1)),
			SpanId:            spanID(byte(index + 1)),
			ParentSpanId:      spanID(byte(index + 16)),
			Name:              marker + "/" + item.label,
			Kind:              tracepb.Span_SPAN_KIND_SERVER,
			StartTimeUnixNano: start,
			EndTimeUnixNano:   start + uint64(time.Millisecond),
			Attributes: []*commonpb.KeyValue{
				stringAttribute(requestMarkerAttribute, marker),
				stringAttribute("e2e.span_kind", item.label),
				intAttribute("e2e.retry_count", int64(index+2)),
				boolAttribute("e2e.span_sampled", true),
				doubleAttribute("e2e.span_ratio", 0.25),
				bytesAttribute("e2e.span_raw", []byte{0x01, 0x7f, 0xff}),
				keyValue("e2e.span_labels", stringArrayValue("native", "otlp", item.label)),
			},
			DroppedAttributesCount: 2,
			Events: []*tracepb.Span_Event{
				{
					TimeUnixNano: start + 1,
					Name:         "gen_ai.input",
					Attributes: []*commonpb.KeyValue{
						stringAttribute("gen_ai.input.prompt", "prompt-"+item.label),
						keyValue("gen_ai.input.context", keyValueListValue(
							stringAttribute("conversation_id", marker),
							intAttribute("turn", 4),
							boolAttribute("streaming", false),
						)),
						keyValue("gen_ai.input.labels", stringArrayValue("system", "user")),
					},
					DroppedAttributesCount: 1,
				},
				{
					TimeUnixNano: start + 2,
					Name:         "tool.output",
					Attributes: []*commonpb.KeyValue{
						stringAttribute("tool.name", "lookup"),
						boolAttribute("tool.success", true),
						keyValue("tool.output.payload", keyValueListValue(
							stringAttribute("result", "result-"+item.label),
							intAttribute("item_count", 3),
						)),
					},
				},
			},
			DroppedEventsCount: 1,
			Links: []*tracepb.Span_Link{
				{
					TraceId:    traceID(byte(index + 32)),
					SpanId:     spanID(byte(index + 32)),
					TraceState: "vendor=value",
					Attributes: []*commonpb.KeyValue{
						stringAttribute("link.relationship", "causal"),
						intAttribute("link.ordinal", int64(index)),
					},
					DroppedAttributesCount: 1,
				},
			},
			DroppedLinksCount: 1,
			Status: &tracepb.Status{
				Message: "completed",
				Code:    tracepb.Status_STATUS_CODE_OK,
			},
		}
		scope := &commonpb.InstrumentationScope{
			Name:    "native-file-e2e",
			Version: "1.2.3",
			Attributes: []*commonpb.KeyValue{
				stringAttribute("scope.owner", "lightspeed"),
				intAttribute("scope.revision", 17),
			},
			DroppedAttributesCount: 1,
		}
		request.ResourceSpans = append(request.ResourceSpans, &tracepb.ResourceSpans{
			Resource:  resource,
			SchemaUrl: resourceSchemaURL,
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope:     scope,
				SchemaUrl: scopeSchemaURL,
				Spans:     []*tracepb.Span{span},
			}},
		})
	}
	return request
}

type expectedNativeSpan struct {
	resource       *resourcepb.Resource
	scope          *commonpb.InstrumentationScope
	resourceSchema string
	scopeSchema    string
	span           *tracepb.Span
	service        string
	include        bool
}

func assertMixedResourceTraceOutput(
	t *testing.T,
	request *collectortracepb.ExportTraceServiceRequest,
	batches []*collectortracepb.ExportTraceServiceRequest,
	marker string,
) {
	t.Helper()
	wantByName := make(map[string]expectedNativeSpan)
	for _, resourceSpans := range request.ResourceSpans {
		service := resourceServiceName(resourceSpans.Resource)
		include := service == "lightspeed-agentic-operator" || service == "lightspeed-agentic-sandbox"
		for _, scopeSpans := range resourceSpans.ScopeSpans {
			for _, span := range scopeSpans.Spans {
				wantByName[span.Name] = expectedNativeSpan{
					resource:       resourceSpans.Resource,
					scope:          scopeSpans.Scope,
					resourceSchema: resourceSpans.SchemaUrl,
					scopeSchema:    scopeSpans.SchemaUrl,
					span:           span,
					service:        service,
					include:        include,
				}
			}
		}
	}

	matched := make(map[string]int, 2)
	for _, batch := range batches {
		for _, resourceSpans := range batch.ResourceSpans {
			service := resourceServiceName(resourceSpans.Resource)
			for _, scopeSpans := range resourceSpans.ScopeSpans {
				for _, span := range scopeSpans.Spans {
					if !strings.Contains(span.Name, marker) {
						continue
					}
					want, ok := wantByName[span.Name]
					if !ok {
						t.Fatalf("unexpected stored trace span %q with service %q", span.Name, service)
					}
					if !want.include {
						t.Fatalf("unmatched service %q was stored as span %q", want.service, span.Name)
					}
					if service != want.service {
						t.Fatalf("stored service.name = %q for span %q, want %q", service, span.Name, want.service)
					}
					if !proto.Equal(resourceSpans.Resource, want.resource) || resourceSpans.SchemaUrl != want.resourceSchema {
						t.Fatalf("resource metadata was not preserved for span %q", span.Name)
					}
					if !proto.Equal(scopeSpans.Scope, want.scope) || scopeSpans.SchemaUrl != want.scopeSchema {
						t.Fatalf("scope metadata was not preserved for span %q", span.Name)
					}
					if !proto.Equal(span, want.span) {
						t.Fatalf("native span fields, typed attributes, links, status, or events changed for %q", span.Name)
					}
					if hasAttribute(resourceSpans.Resource.Attributes, "agenticrun.uid") ||
						hasAttribute(resourceSpans.Resource.Attributes, "agenticrun.phase") ||
						hasAttribute(span.Attributes, "agenticrun.uid") ||
						hasAttribute(span.Attributes, "agenticrun.phase") {
						t.Fatalf("stored span %q unexpectedly depends on run or phase attributes", span.Name)
					}
					if len(span.Events) != 2 || span.Events[0].Name != "gen_ai.input" || span.Events[1].Name != "tool.output" {
						t.Fatalf("nested event order changed for span %q: %v", span.Name, eventNames(span.Events))
					}
					matched[span.Name]++
				}
			}
		}
	}

	for name, want := range wantByName {
		count := matched[name]
		if want.include && count != 1 {
			t.Errorf("allowlisted span %q stored %d times, want exactly once", name, count)
		}
		if !want.include && count != 0 {
			t.Errorf("unmatched span %q stored %d times, want none", name, count)
		}
	}
}

func sendRotationTrace(t *testing.T, marker string, seed byte) {
	t.Helper()
	start := uint64(time.Now().UnixNano())
	payload := marker + "|" + strings.Repeat("x", rotationPayloadBytes-len(marker)-1)
	request := &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				stringAttribute("service.name", "lightspeed-agentic-operator"),
				stringAttribute(requestMarkerAttribute, marker),
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: &commonpb.InstrumentationScope{Name: "file-rotation-e2e", Version: "1.0"},
				Spans: []*tracepb.Span{{
					TraceId:           traceID(seed),
					SpanId:            spanID(seed),
					Name:              marker,
					StartTimeUnixNano: start,
					EndTimeUnixNano:   start + uint64(time.Millisecond),
					Attributes: []*commonpb.KeyValue{
						stringAttribute(rotationPayloadAttribute, payload),
					},
				}},
			}},
		}},
	}
	sendFileTraceRequest(t, request)
}

type nativeTraceFile struct {
	name    string
	batches []*collectortracepb.ExportTraceServiceRequest
}

func readNativeTraceFiles(t *testing.T) ([]nativeTraceFile, bool) {
	t.Helper()
	names := listCollectorTraceFiles(t)
	files := make([]nativeTraceFile, 0, len(names))
	for _, name := range names {
		content, ok := readCollectorTraceFile(t, name)
		if !ok {
			return nil, false
		}
		batches, err := decodeNativeTraceJSONL(name, content)
		if err != nil {
			t.Fatalf("decode native OTLP JSONL file %q: %v", name, err)
		}
		files = append(files, nativeTraceFile{name: name, batches: batches})
	}
	return files, true
}

// The decoder accepts long batches, blank rollover lines, and complete final records without LF.
func decodeNativeTraceJSONL(
	name string,
	content string,
) ([]*collectortracepb.ExportTraceServiceRequest, error) {
	decoder := json.NewDecoder(strings.NewReader(content))
	var batches []*collectortracepb.ExportTraceServiceRequest
	for {
		var raw json.RawMessage
		err := decoder.Decode(&raw)
		if errors.Is(err, io.EOF) {
			return batches, nil
		}
		if err != nil {
			if name == activeTraceFileName && isIncompleteJSON(err) {
				return batches, nil
			}
			return nil, fmt.Errorf("read complete JSON object: %w", err)
		}
		otlpRequest := ptraceotlp.NewExportRequest()
		if err := otlpRequest.UnmarshalJSON([]byte(raw)); err != nil {
			return nil, fmt.Errorf("decode native OTLP JSON: %w", err)
		}
		protobufBytes, err := otlpRequest.MarshalProto()
		if err != nil {
			return nil, fmt.Errorf("marshal native OTLP trace batch: %w", err)
		}
		batch := &collectortracepb.ExportTraceServiceRequest{}
		if err := proto.Unmarshal(protobufBytes, batch); err != nil {
			return nil, fmt.Errorf("decode native OTLP protobuf batch: %w", err)
		}
		batches = append(batches, batch)
	}
}

func isIncompleteJSON(err error) bool {
	return errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "unexpected end of JSON input")
}

func waitForNativeTraceBackup(t *testing.T, marker string) nativeTraceFile {
	t.Helper()
	deadline := time.Now().Add(nativeTraceWaitTimeout)
	for time.Now().Before(deadline) {
		files, complete := readNativeTraceFiles(t)
		if complete {
			for _, file := range files {
				if file.name != activeTraceFileName && nativeTraceBatchesContainSpanName(file.batches, marker) {
					return file
				}
			}
		}
		time.Sleep(nativeTracePollInterval)
	}
	t.Fatalf("timed out waiting for a closed backup containing trace span %q", marker)
	return nativeTraceFile{}
}

func waitForNativeTraceRetention(t *testing.T, oldestMarker string) []nativeTraceFile {
	t.Helper()
	deadline := time.Now().Add(nativeTraceWaitTimeout)
	for time.Now().Before(deadline) {
		files, complete := readNativeTraceFiles(t)
		if complete && !nativeTraceFilesContainSpanName(files, oldestMarker) && len(closedTraceFiles(files)) == 2 {
			return files
		}
		time.Sleep(nativeTracePollInterval)
	}
	t.Fatalf("timed out waiting for retention to remove %q and keep two closed backups", oldestMarker)
	return nil
}

func assertRotationPayloadInBackup(t *testing.T, file nativeTraceFile, marker string) {
	t.Helper()
	for _, batch := range file.batches {
		for _, resourceSpans := range batch.ResourceSpans {
			for _, scopeSpans := range resourceSpans.ScopeSpans {
				for _, span := range scopeSpans.Spans {
					if !strings.Contains(span.Name, marker) {
						continue
					}
					payload, ok := stringAttributeValue(span.Attributes, rotationPayloadAttribute)
					if !ok || len(payload) != rotationPayloadBytes || !strings.HasPrefix(payload, marker+"|") {
						t.Fatalf("closed backup did not preserve the complete %d-byte payload for %q", rotationPayloadBytes, marker)
					}
					return
				}
			}
		}
	}
	t.Fatalf("closed backup %q did not contain trace span %q", file.name, marker)
}

func flattenNativeTraceFiles(files []nativeTraceFile) []*collectortracepb.ExportTraceServiceRequest {
	var batches []*collectortracepb.ExportTraceServiceRequest
	for _, file := range files {
		batches = append(batches, file.batches...)
	}
	return batches
}

func closedTraceFiles(files []nativeTraceFile) []nativeTraceFile {
	var backups []nativeTraceFile
	for _, file := range files {
		if file.name != activeTraceFileName {
			backups = append(backups, file)
		}
	}
	return backups
}

func nativeTraceFilesContainSpanName(files []nativeTraceFile, marker string) bool {
	return nativeTraceBatchesContainSpanName(flattenNativeTraceFiles(files), marker)
}

func nativeTraceBatchesContainSpanName(
	batches []*collectortracepb.ExportTraceServiceRequest,
	marker string,
) bool {
	for _, batch := range batches {
		for _, resourceSpans := range batch.ResourceSpans {
			for _, scopeSpans := range resourceSpans.ScopeSpans {
				for _, span := range scopeSpans.Spans {
					if strings.Contains(span.Name, marker) {
						return true
					}
				}
			}
		}
	}
	return false
}

func resourceServiceName(resource *resourcepb.Resource) string {
	if resource == nil {
		return ""
	}
	service, _ := stringAttributeValue(resource.Attributes, "service.name")
	return service
}

func stringAttributeValue(attributes []*commonpb.KeyValue, key string) (string, bool) {
	for _, attribute := range attributes {
		if attribute.Key == key && attribute.Value != nil {
			value, ok := attribute.Value.Value.(*commonpb.AnyValue_StringValue)
			if ok {
				return value.StringValue, true
			}
		}
	}
	return "", false
}

func hasAttribute(attributes []*commonpb.KeyValue, key string) bool {
	for _, attribute := range attributes {
		if attribute.Key == key {
			return true
		}
	}
	return false
}

func stringAttribute(key, value string) *commonpb.KeyValue {
	return keyValue(key, stringValue(value))
}

func intAttribute(key string, value int64) *commonpb.KeyValue {
	return keyValue(key, &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: value}})
}

func boolAttribute(key string, value bool) *commonpb.KeyValue {
	return keyValue(key, &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: value}})
}

func doubleAttribute(key string, value float64) *commonpb.KeyValue {
	return keyValue(key, &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: value}})
}

func bytesAttribute(key string, value []byte) *commonpb.KeyValue {
	return keyValue(key, &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: value}})
}

func keyValue(key string, value *commonpb.AnyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: value}
}

func stringValue(value string) *commonpb.AnyValue {
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}
}

func stringArrayValue(values ...string) *commonpb.AnyValue {
	array := &commonpb.ArrayValue{Values: make([]*commonpb.AnyValue, 0, len(values))}
	for _, value := range values {
		array.Values = append(array.Values, stringValue(value))
	}
	return &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: array}}
}

func keyValueListValue(values ...*commonpb.KeyValue) *commonpb.AnyValue {
	return &commonpb.AnyValue{
		Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: values}},
	}
}

func traceID(seed byte) []byte {
	id := make([]byte, 16)
	for index := range id {
		id[index] = seed + byte(index)
	}
	return id
}

func spanID(seed byte) []byte {
	id := make([]byte, 8)
	for index := range id {
		id[index] = seed + byte(index)
	}
	return id
}

func eventNames(events []*tracepb.Span_Event) []string {
	names := make([]string, 0, len(events))
	for _, event := range events {
		names = append(names, event.Name)
	}
	return names
}

func waitForDebugTrace(t *testing.T, since time.Time, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(collectorLogsSince(t, since), "Traces") {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatal("debug exporter did not report the trace fan-out")
}

func assertCollectorHealthy(t *testing.T) {
	t.Helper()
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Get("http://" + env.Endpoints.Health)
	if err != nil {
		t.Fatalf("collector health request: %v", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("collector health status = %d, want 200", response.StatusCode)
	}
}
