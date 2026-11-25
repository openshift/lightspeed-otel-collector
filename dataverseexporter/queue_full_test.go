package dataverseexporter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// stubTraces is a hand-rolled inner exporter.Traces whose ConsumeTraces (and
// optionally Shutdown) behavior is controlled by the test.
type stubTraces struct {
	exporter.Traces // embedded nil: only overridden methods may be called
	consume         func(context.Context, ptrace.Traces) error
	shutdown        func(context.Context) error
}

func (s *stubTraces) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	return s.consume(ctx, td)
}

func (s *stubTraces) Shutdown(ctx context.Context) error {
	if s.shutdown != nil {
		return s.shutdown(ctx)
	}
	return s.Traces.Shutdown(ctx)
}

// newObservedTelemetry returns a telemetry wired to a manual metrics reader
// plus the reader, so tests can assert on the drop counters.
func newObservedTelemetry(t *testing.T) (*telemetry, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { mp.Shutdown(context.Background()) })
	ts := componenttest.NewNopTelemetrySettings()
	ts.MeterProvider = mp
	tel, err := newTelemetry(ts)
	require.NoError(t, err)
	return tel, reader
}

// droppedForReason sums the drop counters for the given reason.
func droppedForReason(t *testing.T, reader *sdkmetric.ManualReader, reason string) (batches, spans int64) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	want := attribute.String("reason", reason)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			data, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				continue
			}
			for _, dp := range data.DataPoints {
				for _, a := range dp.Attributes.ToSlice() {
					if a.Key == "reason" && a.Value == want.Value {
						switch m.Name {
						case droppedBatchesMetric:
							batches = dp.Value
						case droppedSpansMetric:
							spans = dp.Value
						}
					}
				}
			}
		}
	}
	return batches, spans
}

func TestWrapperSwallowsQueueFullAndCountsDrop(t *testing.T) {
	tel, reader := newObservedTelemetry(t)
	td := testTraces() // 1 span
	innerErr := fmt.Errorf("wrap: %w", exporterhelper.ErrQueueIsFull)
	inner := &stubTraces{consume: func(context.Context, ptrace.Traces) error { return innerErr }}
	w := newQueueFullWrapper(inner, zap.NewNop(), tel)

	require.NoError(t, w.ConsumeTraces(context.Background(), td))

	batches, spans := droppedForReason(t, reader, "queue_full")
	require.Equal(t, int64(1), batches, "queue_full drop batch counter")
	require.Equal(t, int64(1), spans, "queue_full drop span counter")
}

func TestWrapperPassesPlainErrorThrough(t *testing.T) {
	tel, reader := newObservedTelemetry(t)
	plainErr := errors.New("boom")
	inner := &stubTraces{consume: func(context.Context, ptrace.Traces) error { return plainErr }}
	w := newQueueFullWrapper(inner, zap.NewNop(), tel)

	err := w.ConsumeTraces(context.Background(), testTraces())
	require.ErrorIs(t, err, plainErr)

	batches, _ := droppedForReason(t, reader, "queue_full")
	require.Zero(t, batches, "no drop must be counted for non-queue errors")
}

func TestWrapperPassesSuccessThrough(t *testing.T) {
	tel, _ := newObservedTelemetry(t)
	inner := &stubTraces{consume: func(context.Context, ptrace.Traces) error { return nil }}
	w := newQueueFullWrapper(inner, zap.NewNop(), tel)
	require.NoError(t, w.ConsumeTraces(context.Background(), testTraces()))
}

func TestWrapperShutdownErrorPassesThrough(t *testing.T) {
	tel, _ := newObservedTelemetry(t)
	shutdownErr := errors.New("shutdown boom")
	inner := &stubTraces{
		consume:  func(context.Context, ptrace.Traces) error { return nil },
		shutdown: func(context.Context) error { return shutdownErr },
	}
	w := newQueueFullWrapper(inner, zap.NewNop(), tel)
	require.ErrorIs(t, w.Shutdown(context.Background()), shutdownErr)
}

func TestWrapperRateLimitsQueueFullWarnLog(t *testing.T) {
	tel, _ := newObservedTelemetry(t)
	core, logs := observer.New(zapcore.WarnLevel)
	logger := zap.New(core)
	inner := &stubTraces{
		consume: func(context.Context, ptrace.Traces) error {
			return exporterhelper.ErrQueueIsFull
		},
	}
	w := newQueueFullWrapper(inner, logger, tel)

	for i := 0; i < 5; i++ {
		require.NoError(t, w.ConsumeTraces(context.Background(), testTraces()))
	}
	// Five queue-full drops within the rate-limit window, but only one warn.
	require.Equal(t, 1, logs.FilterLevelExact(zapcore.WarnLevel).Len(),
		"queue-full warn must be rate limited to once per minute")
	require.Contains(t, logs.All()[0].Message, "queue")
}

// TestQueueFullErrorIsSwallowedEndToEnd is the fan-out isolation scenario:
// tiny queue, no flush (10 m timeout), a single consumer. The queue fills
// because the byte-sizer batcher holds enqueued items pending flush —
// capacity is not released until a flush occurs (verified: the queue
// fills with zero server requests; the stalled handler only affects the
// shutdown path). Once the queue is full, ConsumeTraces must return nil
// (not ErrQueueIsFull) and the drop must be counted, so the Collector's
// fan-out never sees the dataverse exporter's backpressure.
func TestQueueFullErrorIsSwallowedEndToEnd(t *testing.T) {
	cfg := &atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, cfg, "cid")

	release := make(chan struct{})
	var mu sync.Mutex
	inflight := 0
	srv := fakeIngress(t, func(int) int { return http.StatusAccepted }, &recordedUpload{})
	srv.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		inflight++
		mu.Unlock()
		<-release // stall: never respond while the test holds the gate
		w.WriteHeader(http.StatusAccepted)
	})
	t.Cleanup(func() { close(release) })

	// Tiny queue, one consumer, non-blocking overflow. The queue fills after
	// a handful of batches because the batcher holds them pending the 10 m
	// flush_timeout — this premise depends on the batch config staying
	// present; removing it would let items flush through and never fill.
	// Keep the batch block in newTestExporterConfig for this test.
	dcfg := newTestExporterConfig(t, srv.URL, "10m")
	dcfg.QueueConfig.Get().QueueSize = 4096

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { mp.Shutdown(context.Background()) })

	set := exportertest.NewNopSettings(Type)
	set.TelemetrySettings.MeterProvider = mp
	exp, err := newTracesExporter(context.Background(), set, dcfg, auth)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { exp.Shutdown(context.Background()) })

	// Pump until the queue is full (observable via the queue_full drop
	// counter) and verify the swallowed error looks like success to callers.
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for {
		lastErr = exp.ConsumeTraces(context.Background(), testTraces())
		require.NoError(t, lastErr, "queue-full must be swallowed, not surfaced")
		batches, spans := droppedForReason(t, reader, "queue_full")
		if batches > 0 && spans > 0 {
			return // queue filled and drop was counted
		}
		if time.Now().After(deadline) {
			t.Fatal("queue never filled within 10s of pumping")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
