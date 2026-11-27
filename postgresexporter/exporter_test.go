package postgresexporter

import (
	"context"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v6"
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

	// 5 args per record: agentic_run_id, phase, timestamp, event, body
	mock.ExpectExec(`INSERT INTO templogs\.logs`).
		WithArgs(
			"550e8400-e29b-41d4-a716-446655440000", // agentic_run_id
			"planning",                             // phase
			pgxmock.AnyArg(),                       // timestamp
			"audit.agent.tool.call",                // event
			pgxmock.AnyArg(),                       // body
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("service.name", "test-svc")
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.Body().SetStr(`{"tool":"bash","args":{"cmd":"ls"}}`)
	lr.Attributes().PutStr("agenticrun.uid", "550e8400-e29b-41d4-a716-446655440000")
	lr.Attributes().PutStr("agenticrun.phase", "planning")
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

	// 3 records × 5 args = 15 args
	mock.ExpectExec(`INSERT INTO templogs\.logs`).
		WithArgs(
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 3))

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	for i := 0; i < 3; i++ {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.Body().SetStr(`{}`)
		lr.Attributes().PutStr("agenticrun.uid", "run-abc")
		lr.Attributes().PutStr("agenticrun.phase", "execution")
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
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
		).
		WillReturnError(context.DeadlineExceeded)

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.Body().SetStr(`{}`)
	lr.Attributes().PutStr("agenticrun.uid", "run-1")
	lr.Attributes().PutStr("agenticrun.phase", "planning")
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
			"run-xyz",           // agentic_run_id
			"",                  // phase (not set)
			pgxmock.AnyArg(),    // timestamp
			"custom.event.name", // event
			pgxmock.AnyArg(),    // body
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.Body().SetStr(`{"data":"test"}`)
	lr.Attributes().PutStr("agenticrun.uid", "run-xyz")
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
			pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(),
		).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	// No Timestamp set — should fall back to ObservedTimestamp
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
	lr.Body().SetStr(`{}`)
	lr.Attributes().PutStr("agenticrun.uid", "run-ts")
	lr.Attributes().PutStr("agenticrun.phase", "planning")
	lr.Attributes().PutStr("event", "test")

	err := e.consumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestConsumeLogsMissingAgenticRunID(t *testing.T) {
	e, mock := newTestExporter(t)
	defer mock.Close()

	// Records without agenticrun.uid are skipped — no INSERT expected.
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	lr.Body().SetStr(`{"raw":"no attrs"}`)

	err := e.consumeLogs(context.Background(), ld)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// No DB calls should have been made.
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
