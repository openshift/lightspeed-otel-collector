package postgresexporter

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, component.StabilityLevelDevelopment),
	)
}

func createLogsExporter(
	ctx context.Context,
	set exporter.Settings,
	cfg component.Config,
) (exporter.Logs, error) {
	pcfg := cfg.(*Config)

	pool, err := pgxpool.New(ctx, pcfg.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	tel, err := newTelemetry(set.TelemetrySettings)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to create telemetry: %w", err)
	}

	e := &postgresExporter{
		config:    pcfg,
		pool:      pool,
		logger:    set.Logger,
		telemetry: tel,
		ts:        set.TelemetrySettings,
	}

	exp, err := exporterhelper.NewLogs(
		ctx,
		set,
		cfg,
		e.consumeLogs,
		exporterhelper.WithStart(e.start),
		exporterhelper.WithShutdown(e.shutdown),
		exporterhelper.WithRetry(pcfg.RetryConfig),
		exporterhelper.WithQueue(pcfg.QueueConfig),
	)
	if err != nil {
		pool.Close()
		return nil, err
	}
	return exp, nil
}
