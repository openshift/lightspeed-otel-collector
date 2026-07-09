package postgresexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/otel/metric"
)

const meterScope = "github.com/openshift/lightspeed-otel-collector/postgresexporter"

type telemetry struct {
	insertDuration metric.Float64Histogram
	batchSize      metric.Int64Histogram
}

func newTelemetry(ts component.TelemetrySettings) (*telemetry, error) {
	meter := ts.MeterProvider.Meter(meterScope)

	insertDuration, err := meter.Float64Histogram(
		"otelcol_postgres_exporter_insert_duration",
		metric.WithDescription("Time spent inserting a batch into PostgreSQL (seconds)."),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, err
	}

	batchSize, err := meter.Int64Histogram(
		"otelcol_postgres_exporter_batch_size",
		metric.WithDescription("Number of log records per consumeLogs call."),
		metric.WithUnit("{records}"),
	)
	if err != nil {
		return nil, err
	}

	return &telemetry{
		insertDuration: insertDuration,
		batchSize:      batchSize,
	}, nil
}

func (t *telemetry) registerPoolMetrics(ts component.TelemetrySettings, p pool) error {
	meter := ts.MeterProvider.Meter(meterScope)

	_, err := meter.Int64ObservableGauge(
		"otelcol_postgres_exporter_db_pool_total",
		metric.WithDescription("Total number of connections in the pool."),
		metric.WithUnit("{connections}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(p.Stat().TotalConns()))
			return nil
		}),
	)
	if err != nil {
		return err
	}

	_, err = meter.Int64ObservableGauge(
		"otelcol_postgres_exporter_db_pool_idle",
		metric.WithDescription("Number of idle connections in the pool."),
		metric.WithUnit("{connections}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(p.Stat().IdleConns()))
			return nil
		}),
	)
	if err != nil {
		return err
	}

	_, err = meter.Int64ObservableGauge(
		"otelcol_postgres_exporter_db_pool_in_use",
		metric.WithDescription("Number of connections currently in use."),
		metric.WithUnit("{connections}"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(p.Stat().AcquiredConns()))
			return nil
		}),
	)
	return err
}

func (t *telemetry) recordInsert(ctx context.Context, start time.Time) {
	t.insertDuration.Record(ctx, time.Since(start).Seconds())
}

func (t *telemetry) recordBatchSize(ctx context.Context, count int) {
	t.batchSize.Record(ctx, int64(count))
}
