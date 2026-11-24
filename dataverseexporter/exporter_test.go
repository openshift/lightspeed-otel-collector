package dataverseexporter

import (
	"context"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

// newTestExporterConfig builds a Config via confmap so queue/batch internals
// (internal sizer types) are set exactly like real config loading does.
func newTestExporterConfig(t *testing.T, endpoint string, flushTimeout string) *Config {
	t.Helper()
	raw := map[string]any{
		"endpoint":            endpoint,
		"service_id":          "ols",
		"archive_path_prefix": "agentic-v1/",
		"sending_queue": map[string]any{
			"sizer":             "bytes",
			"queue_size":        1024 * 1024,
			"num_consumers":     1,
			"block_on_overflow": false,
			"batch": map[string]any{
				"sizer":         "bytes",
				"min_size":      64 * 1024 * 1024, // unreachable: flush must come from timeout
				"flush_timeout": flushTimeout,
			},
		},
	}
	cm := confmap.NewFromStringMap(raw)
	cfg := createDefaultConfig().(*Config)
	if err := cm.Unmarshal(cfg); err != nil {
		t.Fatalf("unmarshal test config: %v", err)
	}
	return cfg
}

func TestUploadHonorsConfiguredHTTPTimeout(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, "cid")
	srv := fakeIngress(t, func(int) int { time.Sleep(5500 * time.Millisecond); return http.StatusAccepted }, &recordedUpload{})
	dcfg := newTestExporterConfig(t, srv.URL, "1m")
	dcfg.Timeout = 15 * time.Second
	dcfg.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	dcfg.RetryConfig.Enabled = false
	exp, err := newTracesExporter(t.Context(), exportertest.NewNopSettings(Type), dcfg, auth)
	require.NoError(t, err)
	require.NoError(t, exp.Start(t.Context(), componenttest.NewNopHost()))
	defer exp.Shutdown(context.Background())
	require.NoError(t, exp.ConsumeTraces(t.Context(), testTraces()))
}

func TestConsumeTracesUploads(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, "cid")
	var rec recordedUpload
	srv := fakeIngress(t, func(int) int { return http.StatusAccepted }, &rec)

	dcfg := newTestExporterConfig(t, srv.URL, "1m")
	e := &dataverseExporter{
		config: dcfg,
		client: &ingressClient{
			client: srv.Client(), endpoint: srv.URL,
			serviceID: dcfg.ServiceID, auth: auth, logger: zap.NewNop(),
		},
		logger:    zap.NewNop(),
		telemetry: newNopTelemetry(),
	}

	if err := e.consumeTraces(context.Background(), testTraces()); err != nil {
		t.Fatalf("consumeTraces: %v", err)
	}
	if len(rec.body) == 0 {
		t.Error("expected an upload")
	}
}

func TestConsumeTracesDropsOversizedWithoutRetry(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, "cid")
	var rec recordedUpload
	srv := fakeIngress(t, func(int) int { return http.StatusAccepted }, &rec)

	dcfg := newTestExporterConfig(t, srv.URL, "1m")
	dcfg.MaxPayloadSize = 16 // smaller than any real payload

	e := &dataverseExporter{
		config: dcfg,
		client: &ingressClient{
			client: srv.Client(), endpoint: srv.URL,
			serviceID: dcfg.ServiceID, auth: auth, logger: zap.NewNop(),
		},
		logger: zap.NewNop(), telemetry: newNopTelemetry(),
	}

	// Oversized batches are counted drops, not retryable errors.
	if err := e.consumeTraces(context.Background(), testTraces()); err != nil {
		t.Fatalf("consumeTraces returned %v; oversized must be dropped, not errored", err)
	}
	if rec.attempts != 0 {
		t.Errorf("ingress attempts = %d; want 0 for oversized drop", rec.attempts)
	}
}

func TestConsumeTracesRetriesUploadErrors(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, "cid")
	var rec recordedUpload
	srv := fakeIngress(t, func(int) int { return http.StatusInternalServerError }, &rec)

	dcfg := newTestExporterConfig(t, srv.URL, "1m")
	e := &dataverseExporter{
		config: dcfg,
		client: &ingressClient{
			client: srv.Client(), endpoint: srv.URL,
			serviceID: dcfg.ServiceID, auth: auth, logger: zap.NewNop(),
		},
		logger: zap.NewNop(), telemetry: newNopTelemetry(),
	}

	if err := e.consumeTraces(context.Background(), testTraces()); err == nil {
		t.Fatal("upload errors must be returned for exporterhelper retry")
	}
}

// TestQueueFlushTimeoutCoversQuietClusters is the OLS-4493 core scenario:
// with volume far below min_size, the queue's flush_timeout must still
// produce an upload. This also proves explicit `batch:` config activates
// queue batching without the exporterhelper feature gate.
func TestQueueFlushTimeoutCoversQuietClusters(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, "cid")

	// fakeIngress's recorder isn't concurrency-safe; use our own counting
	// handler instead.
	var mu sync.Mutex
	uploads := 0
	srv := fakeIngress(t, func(int) int { return http.StatusAccepted }, &recordedUpload{})
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		uploads++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"request_id":"r"}`))
	})

	dcfg := newTestExporterConfig(t, srv.URL, "300ms")

	set := exportertest.NewNopSettings(Type)
	exp, err := newTracesExporter(context.Background(), set, dcfg, auth)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	if err := exp.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start: %v", err) // resolves credentials against the fake K8s API
	}
	t.Cleanup(func() { exp.Shutdown(context.Background()) })

	if err := exp.ConsumeTraces(context.Background(), testTraces()); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		done := uploads >= 1
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no upload within 5s — flush_timeout did not cover the quiet-cluster case")
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestShutdownDrainsQueuedBatch(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, "cid")

	var mu sync.Mutex
	uploads := 0
	srv := fakeIngress(t, func(int) int { return http.StatusAccepted }, &recordedUpload{})
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		uploads++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"request_id":"r"}`))
	})

	// Long flush timeout: the upload must come from shutdown drain, not timeout.
	dcfg := newTestExporterConfig(t, srv.URL, "10m")
	set := exportertest.NewNopSettings(Type)
	exp, err := newTracesExporter(context.Background(), set, dcfg, auth)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	if err := exp.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := exp.ConsumeTraces(context.Background(), testTraces()); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if err := exp.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		done := uploads >= 1
		mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no upload within 5s — shutdown did not drain the queued batch")
		}
		time.Sleep(25 * time.Millisecond)
	}

}

func TestConsumeAfterShutdownIsDropped(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, "cid")

	var n atomic.Int64
	srv := fakeIngress(t, func(int) int { return http.StatusAccepted }, &recordedUpload{})
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"request_id":"r"}`))
	})

	dcfg := newTestExporterConfig(t, srv.URL, "10m")
	set := exportertest.NewNopSettings(Type)
	exp, err := newTracesExporter(context.Background(), set, dcfg, auth)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	if err := exp.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := exp.Shutdown(context.Background()); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	// NOTE: exporterhelper v0.159.0's memory queue has no stopped-check in
	// Offer, so ConsumeTraces after Shutdown returns nil and silently drops
	// the batch (no upload, nothing left to drain). In the running service the
	// pipeline is torn down before exporters, so this is unreachable in
	// production; we pin the framework behavior so an upgrade that starts
	// returning an error (or, worse, uploading) is noticed.
	err = exp.ConsumeTraces(context.Background(), testTraces())
	if err != nil {
		t.Fatalf("ConsumeTraces after Shutdown returned %v; want nil (framework drops silently)", err)
	}
	if uploads := n.Load(); uploads != 0 {
		t.Errorf("uploads after Shutdown = %d; want 0", uploads)
	}
}

// TestStartIsLazyAboutCredentials is the amendment to the original
// fail-fast behavior: the Collector process is shared with other
// pipelines, so a credential problem must not stop the Collector. Start
// must succeed even when the pull-secret lacks the cloud.openshift.com
// entry; the failure surfaces as a permanent counted drop on first upload.
func TestStartIsLazyAboutCredentials(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(`{"auths":{"quay.io":{"auth":"x"}}}`)) // no cloud entry
	auth := newTestAuth(t, &cfg, "cid")
	dcfg := newTestExporterConfig(t, "https://example.invalid", "1m")
	// Disable queue and retry so the first ConsumeTraces surfaces the
	// drop synchronously instead of absorbing it in the background.
	dcfg.RetryConfig.Enabled = false
	dcfg.QueueConfig = configoptional.None[exporterhelper.QueueBatchConfig]()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { mp.Shutdown(context.Background()) })
	set := exportertest.NewNopSettings(Type)
	set.TelemetrySettings.MeterProvider = mp

	exp, err := newTracesExporter(context.Background(), set, dcfg, auth)
	if err != nil {
		t.Fatalf("newTracesExporter: %v", err)
	}
	if err := exp.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		t.Fatalf("start must not resolve credentials: %v", err)
	}
	t.Cleanup(func() { exp.Shutdown(context.Background()) })

	// A pull-secret without the cloud entry is a credential SHAPE error:
	// permanent, so the batch is a counted drop and ConsumeTraces reports
	// success (nothing left to retry).
	if err := exp.ConsumeTraces(context.Background(), testTraces()); err != nil {
		t.Fatalf("first ConsumeTraces must drop the batch, not error: %v", err)
	}
	batches, spans := droppedForReason(t, reader, "credentials")
	if batches != 1 || spans != 1 {
		t.Errorf("credentials drop counters = (%d batches, %d spans); want (1, 1)", batches, spans)
	}
}

// newDataverseExporterForTest wires a dataverseExporter against a fake
// ingress with observed metrics, for drop-path assertions.
func newDataverseExporterForTest(t *testing.T, status int, tel *telemetry) (*dataverseExporter, *recordedUpload) {
	t.Helper()
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, "cid")
	var rec recordedUpload
	srv := fakeIngress(t, func(int) int { return status }, &rec)
	dcfg := newTestExporterConfig(t, srv.URL, "1m")
	return &dataverseExporter{
		config: dcfg,
		client: &ingressClient{
			client: srv.Client(), endpoint: srv.URL,
			serviceID: dcfg.ServiceID, auth: auth, logger: zap.NewNop(),
		},
		logger:    zap.NewNop(),
		telemetry: tel,
	}, &rec
}

// TestConsumeTracesDropsPermanentUploadErrors: a permanent upload error
// (e.g. HTTP 403) must be a counted drop — consumeTraces returns nil so
// exporterhelper never retries it.
func TestConsumeTracesDropsPermanentUploadErrors(t *testing.T) {
	tel, reader := newObservedTelemetry(t)
	e, _ := newDataverseExporterForTest(t, http.StatusForbidden, tel)

	require.NoError(t, e.consumeTraces(context.Background(), testTraces()),
		"permanent upload errors must be dropped, not surfaced for retry")

	batches, spans := droppedForReason(t, reader, "http_403")
	require.Equal(t, int64(1), batches, "permanent-drop batch counter")
	require.Equal(t, int64(1), spans, "permanent-drop span counter")
}

// TestConsumeTracesReturnsRetryableUploadErrors: retryable errors must be
// returned unchanged (surfaced to retry_on_failure) with no drop counted.
func TestConsumeTracesReturnsRetryableUploadErrors(t *testing.T) {
	tel, reader := newObservedTelemetry(t)
	e, _ := newDataverseExporterForTest(t, http.StatusInternalServerError, tel)

	err := e.consumeTraces(context.Background(), testTraces())
	require.Error(t, err, "retryable upload errors must be returned for exporterhelper retry")
	require.False(t, consumererror.IsPermanent(err))

	batches, _ := droppedForReason(t, reader, "http_500")
	require.Zero(t, batches, "no drop must be counted for retryable errors")
}
