package postgresexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

// pool abstracts pgxpool.Pool for testability.
type pool interface {
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
	Ping(ctx context.Context) error
	Stat() *pgxpool.Stat
	Close()
}

type postgresExporter struct {
	config    *Config
	pool      pool
	logger    *zap.Logger
	telemetry *telemetry
	ts        component.TelemetrySettings
}

func (e *postgresExporter) start(ctx context.Context, _ component.Host) error {
	if err := e.pool.Ping(ctx); err != nil {
		e.pool.Close()
		return fmt.Errorf("failed to ping postgres: %w", err)
	}

	stat := e.pool.Stat()
	e.logger.Info("connected to postgres",
		zap.Int32("max_conns", stat.MaxConns()),
	)

	if err := e.telemetry.registerPoolMetrics(e.ts, e.pool); err != nil {
		return fmt.Errorf("failed to register pool metrics: %w", err)
	}
	return nil
}

func (e *postgresExporter) shutdown(_ context.Context) error {
	if e.pool != nil {
		e.pool.Close()
	}
	return nil
}

// consumeLogs is the hot path — called by the Collector for every batch of
// logs that flows through the pipeline. Each batch is written in a single
// transaction so either the entire batch is committed or none of it is.
//
// Per log record, extracts:
//   - trace_id: from the log record's TraceID field (32-char hex)
//   - timestamp: from TimeUnixNano (falls back to ObservedTimestamp, then now)
//   - event: from log record attributes (key: "event")
//   - body: log record body serialized as JSONB
func (e *postgresExporter) consumeLogs(ctx context.Context, ld plog.Logs) error {
	recordCount := ld.LogRecordCount()
	if recordCount == 0 {
		return nil
	}

	e.telemetry.recordBatchSize(ctx, recordCount)
	insertStart := time.Now()

	// Collect all row values into a flat args slice and build a multi-value
	// INSERT: INSERT INTO t (cols) VALUES ($1,$2,$3,$4), ($5,$6,$7,$8), ...
	args := make([]interface{}, 0, recordCount*4)
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)

				ts := lr.Timestamp().AsTime()
				if ts.IsZero() {
					ts = lr.ObservedTimestamp().AsTime()
				}
				if ts.IsZero() {
					ts = time.Now()
				}

				traceID := lr.TraceID().String()

				event := ""
				if v, ok := lr.Attributes().Get("event"); ok {
					event = v.AsString()
				}

			body, err := json.Marshal(lr.Body().AsRaw())
			if err != nil {
				raw := lr.Body().AsString()
				body, _ = json.Marshal(map[string]string{"raw": raw})
			}

				args = append(args, traceID, ts, event, body)
			}
		}
	}

	// Build VALUES placeholders: ($1,$2,$3,$4), ($5,$6,$7,$8), ...
	tuples := make([]string, 0, recordCount)
	for i := 0; i < recordCount; i++ {
		base := i*4 + 1
		tuples = append(tuples, fmt.Sprintf("($%d,$%d,$%d,$%d)", base, base+1, base+2, base+3))
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (trace_id, timestamp, event, body) VALUES %s",
		e.config.qualifiedTable(),
		strings.Join(tuples, ","),
	)

	_, err := e.pool.Exec(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("insert log records: %w", err)
	}

	e.telemetry.recordInsert(ctx, insertStart)
	return nil
}
