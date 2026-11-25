package dataverseexporter

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// queueFullWarnInterval bounds how often the queue-full warn is emitted.
// Without it, a stalled ingress would flood the log on every refused batch.
const queueFullWarnInterval = time.Minute

// queueFullWrapper decorates the exporter.Traces built by exporterhelper so
// that queue-full errors stay inside the dataverse exporter (fan-out
// isolation). Left alone, exporterhelper.ErrQueueIsFull would propagate up
// the Collector's fan-out: the OTLP receiver would answer the producer with
// UNAVAILABLE/503, the producer would retry the whole request, and every
// OTHER trace consumer would receive the duplicate. Instead the refused
// batch is dropped here, counted, and reported to callers as success.
type queueFullWrapper struct {
	exporter.Traces
	logger    *zap.Logger
	telemetry *telemetry

	mu       sync.Mutex
	lastWarn time.Time
}

func newQueueFullWrapper(inner exporter.Traces, logger *zap.Logger, tel *telemetry) exporter.Traces {
	return &queueFullWrapper{Traces: inner, logger: logger, telemetry: tel}
}

// ConsumeTraces delegates to the inner exporter. If the queue refused the
// batch (non-blocking overflow), the batch is a counted drop and nil is
// returned; all other outcomes pass through unchanged.
func (w *queueFullWrapper) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	err := w.Traces.ConsumeTraces(ctx, td)
	if !errors.Is(err, exporterhelper.ErrQueueIsFull) {
		return err
	}
	w.telemetry.recordDroppedBatch(ctx, "queue_full", td.SpanCount())
	if w.warnAllowed(time.Now()) {
		w.logger.Warn("dropping trace batch: dataverse exporter queue full; "+
			"backpressure is contained to this exporter (further warnings suppressed for a minute)",
			zap.Int("spans", td.SpanCount()))
	}
	return nil
}

// warnAllowed rate-limits the queue-full warn to at most one per
// queueFullWarnInterval, guarded for concurrent ConsumeTraces callers.
func (w *queueFullWrapper) warnAllowed(now time.Time) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if now.Sub(w.lastWarn) < queueFullWarnInterval {
		return false
	}
	w.lastWarn = now
	return true
}
