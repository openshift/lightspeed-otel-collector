package dataverseexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const (
	meterScope = "github.com/openshift/lightspeed-otel-collector/dataverseexporter"

	droppedBatchesMetric = "otelcol_dataverse_exporter_dropped_batches"
	droppedSpansMetric   = "otelcol_dataverse_exporter_dropped_spans"
)

type telemetry struct {
	droppedBatches metric.Int64Counter
	droppedSpans   metric.Int64Counter
}

func newTelemetry(ts component.TelemetrySettings) (*telemetry, error) {
	meter := ts.MeterProvider.Meter(meterScope)
	batches, err := meter.Int64Counter(droppedBatchesMetric,
		metric.WithDescription("Trace batches dropped by the dataverse exporter"))
	if err != nil {
		return nil, err
	}
	spans, err := meter.Int64Counter(droppedSpansMetric,
		metric.WithDescription("Spans in batches dropped by the dataverse exporter"))
	if err != nil {
		return nil, err
	}
	return &telemetry{droppedBatches: batches, droppedSpans: spans}, nil
}

func (t *telemetry) recordDroppedBatch(ctx context.Context, reason string, spanCount int) {
	// Use a detached context so drops during shutdown drain are still counted.
	ctx = context.WithoutCancel(ctx)
	opts := metric.WithAttributes(attribute.String("reason", reason))
	t.droppedBatches.Add(ctx, 1, opts)
	t.droppedSpans.Add(ctx, int64(spanCount), opts)
}

// newNopTelemetry is used by tests that do not assert on metrics.
func newNopTelemetry() *telemetry {
	meter := noop.NewMeterProvider().Meter(meterScope)
	batches, _ := meter.Int64Counter(droppedBatchesMetric)
	spans, _ := meter.Int64Counter(droppedSpansMetric)
	return &telemetry{droppedBatches: batches, droppedSpans: spans}
}
