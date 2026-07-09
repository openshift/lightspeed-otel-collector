package postgresexporter

import (
	"context"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v5"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

func newTestExporter(t *testing.T) (*postgresExporter, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create pgxmock: %v", err)
	}

	ts := component.TelemetrySettings{
		MeterProvider: noop.NewMeterProvider(),
	}
	tel, err := newTelemetry(ts)
	if err != nil {
		t.Fatalf("failed to create telemetry: %v", err)
	}

	cfg := &Config{
		Schema:    "templogs",
		LogsTable: "logs",
	}
	e := &postgresExporter{
		config:    cfg,
		pool:      mock,
		logger:    zap.NewNop(),
		telemetry: tel,
		ts:        ts,
	}
	return e, mock
}

func TestConsumeLogsEmpty(t *testing.T) {
	e, mock := newTestExporter(t)
	defer mock.Close()

	err := e.consumeLogs(context.Background(), plog.NewLogs())
	if err != nil {
		t.Fatalf("unexpected error on empty logs: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected calls: %v", err)
	}
}

func TestConsumeLogsSingleRecord(t *testing.T) {
	e, mock := newTestExporter(t)
	defer mock.Close()

	mock.ExpectExec(`INSERT INTO templogs\.logs`).
		WithArgs(
			pgxmock.AnyArg(), // trace_id
			pgxmock.AnyArg(), // timestamp
			"audit.agent.tool.call",
			pgxmock.AnyArg(), // body
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-svc")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.Body().SetStr(`{"tool":"bash","args":{"cmd":"ls"}}`)
	lr.Attributes().PutStr("event", "audit.agent.tool.call")

	err := e.consumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestConsumeLogsMultipleRecords(t *testing.T) {
	e, mock := newTestExporter(t)
	defer mock.Close()

	// 3 records → single INSERT with 12 args
	mock.ExpectExec(`INSERT INTO templogs\.logs`).
		WithArgs(
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 3))

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 3; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.Body().SetStr(`{}`)
		lr.Attributes().PutStr("event", "audit.agent.text")
	}

	err := e.consumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestConsumeLogsReturnsErrorOnInsertFailure(t *testing.T) {
	e, mock := newTestExporter(t)
	defer mock.Close()

	mock.ExpectExec(`INSERT INTO templogs\.logs`).
		WithArgs(
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
		).
		WillReturnError(context.DeadlineExceeded)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.Body().SetStr(`{}`)
	lr.Attributes().PutStr("event", "audit.agent.started")

	err := e.consumeLogs(context.Background(), ld)
	if err == nil {
		t.Fatal("expected error on insert failure, got nil")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestConsumeLogsEventExtractedFromAttributes(t *testing.T) {
	e, mock := newTestExporter(t)
	defer mock.Close()

	mock.ExpectExec(`INSERT INTO templogs\.logs`).
		WithArgs(
			pgxmock.AnyArg(),
			pgxmock.AnyArg(),
			"custom.event.name",
			pgxmock.AnyArg(),
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.Body().SetStr(`{"data":"test"}`)
	lr.Attributes().PutStr("event", "custom.event.name")

	err := e.consumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestConsumeLogsFallsBackToObservedTimestamp(t *testing.T) {
	e, mock := newTestExporter(t)
	defer mock.Close()

	mock.ExpectExec(`INSERT INTO templogs\.logs`).
		WithArgs(
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	// No Timestamp set — should fall back to ObservedTimestamp
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
	lr.Body().SetStr(`{}`)
	lr.Attributes().PutStr("event", "test")

	err := e.consumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestShutdownClosesPool(t *testing.T) {
	e, _ := newTestExporter(t)
	e.pool = nil
	err := e.shutdown(context.Background())
	if err != nil {
		t.Fatalf("unexpected error on shutdown: %v", err)
	}
}
