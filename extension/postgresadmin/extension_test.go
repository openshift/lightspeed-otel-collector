package postgresadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v5"
	"go.uber.org/zap"
)

func newTestAdmin(t *testing.T) (*postgresAdmin, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create pgxmock: %v", err)
	}

	cfg := &Config{
		Endpoint:         "0.0.0.0:8080",
		ConnectionString: "postgres://localhost/db",
		Schema:           "templogs",
		LogsTable:        "logs",
	}

	admin := &postgresAdmin{
		config: cfg,
		logger: zap.NewNop(),
		pool:   mock,
	}
	return admin, mock
}

func TestHandleDeleteLogsMissingAgenticRunID(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/logs", nil)
	w := httptest.NewRecorder()
	admin.handleDeleteLogs(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	var resp deleteResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Error == "" {
		t.Error("expected error message in response")
	}
}

func TestHandleDeleteLogsSuccess(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectExec(`DELETE FROM templogs\.logs WHERE agentic_run_id = \$1`).
		WithArgs("550e8400-e29b-41d4-a716-446655440000").
		WillReturnResult(pgxmock.NewResult("DELETE", 5))

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/logs?agentic_run_id=550e8400-e29b-41d4-a716-446655440000", nil)
	w := httptest.NewRecorder()
	admin.handleDeleteLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp deleteResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Deleted != 5 {
		t.Errorf("expected 5 deleted, got %d", resp.Deleted)
	}
	if resp.AgenticRunID != "550e8400-e29b-41d4-a716-446655440000" {
		t.Errorf("expected agentic_run_id in response, got %q", resp.AgenticRunID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHandleDeleteLogsZeroRows(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectExec(`DELETE FROM templogs\.logs WHERE agentic_run_id = \$1`).
		WithArgs("nonexistent").
		WillReturnResult(pgxmock.NewResult("DELETE", 0))

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/logs?agentic_run_id=nonexistent", nil)
	w := httptest.NewRecorder()
	admin.handleDeleteLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp deleteResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Deleted != 0 {
		t.Errorf("expected 0 deleted, got %d", resp.Deleted)
	}
}

func TestHandleDeleteLogsDBError(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectExec(`DELETE FROM templogs\.logs WHERE agentic_run_id = \$1`).
		WithArgs("abc123").
		WillReturnError(context.DeadlineExceeded)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/logs?agentic_run_id=abc123", nil)
	w := httptest.NewRecorder()
	admin.handleDeleteLogs(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandleGetLogsMissingAgenticRunID(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	var resp getResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Error == "" {
		t.Error("expected error message in response")
	}
}

func TestHandleGetLogsSuccess(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	ts1 := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 7, 9, 12, 0, 1, 0, time.UTC)

	rows := pgxmock.NewRows([]string{"id", "phase", "timestamp", "event", "body"}).
		AddRow(int64(1), "planning", ts1, "audit.agent.started", []byte(`{"msg":"hello"}`)).
		AddRow(int64(2), "planning", ts2, "audit.agent.tool.call", []byte(`{"tool":"bash"}`))

	mock.ExpectQuery(`SELECT id, phase, timestamp, event, body FROM templogs\.logs WHERE agentic_run_id = \$1 AND id > \$2 ORDER BY id ASC LIMIT \$3`).
		WithArgs("abc123", int64(0), 101).
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs?agentic_run_id=abc123", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp getResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Records) != 2 {
		t.Errorf("expected 2 records, got %d", len(resp.Records))
	}
	if resp.HasMore {
		t.Error("expected has_more=false with 2 records")
	}
	if resp.AgenticRunID != "abc123" {
		t.Errorf("expected agentic_run_id=abc123, got %q", resp.AgenticRunID)
	}
	if resp.Records[0].Phase != "planning" {
		t.Errorf("expected phase=planning, got %q", resp.Records[0].Phase)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHandleGetLogsWithPhaseFilter(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	ts := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)

	rows := pgxmock.NewRows([]string{"id", "phase", "timestamp", "event", "body"}).
		AddRow(int64(5), "execution", ts, "audit.agent.tool.call", []byte(`{"tool":"bash"}`))

	mock.ExpectQuery(`SELECT id, phase, timestamp, event, body FROM templogs\.logs WHERE agentic_run_id = \$1 AND phase = \$2 AND id > \$3 ORDER BY id ASC LIMIT \$4`).
		WithArgs("run-1", "execution", int64(0), 101).
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs?agentic_run_id=run-1&phase=execution", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp getResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Records) != 1 {
		t.Errorf("expected 1 record, got %d", len(resp.Records))
	}
	if resp.Phase != "execution" {
		t.Errorf("expected phase=execution in response, got %q", resp.Phase)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHandleGetLogsWithPagination(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	ts := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)

	rows := pgxmock.NewRows([]string{"id", "phase", "timestamp", "event", "body"}).
		AddRow(int64(11), "planning", ts, "audit.agent.started", []byte(`{}`)).
		AddRow(int64(12), "planning", ts, "audit.agent.text", []byte(`{}`)).
		AddRow(int64(13), "planning", ts, "audit.agent.tool.call", []byte(`{}`))

	mock.ExpectQuery(`SELECT id, phase, timestamp, event, body FROM templogs\.logs WHERE agentic_run_id = \$1 AND id > \$2 ORDER BY id ASC LIMIT \$3`).
		WithArgs("trace1", int64(10), 3).
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs?agentic_run_id=trace1&limit=2&after=10", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp getResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if len(resp.Records) != 2 {
		t.Errorf("expected 2 records (trimmed), got %d", len(resp.Records))
	}
	if !resp.HasMore {
		t.Error("expected has_more=true")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHandleGetLogsDBError(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectQuery(`SELECT id, phase, timestamp, event, body FROM templogs\.logs`).
		WithArgs("abc", int64(0), 101).
		WillReturnError(context.DeadlineExceeded)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs?agentic_run_id=abc", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandleGetLogsLimitCapped(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"id", "phase", "timestamp", "event", "body"})

	mock.ExpectQuery(`SELECT id, phase, timestamp, event, body FROM templogs\.logs`).
		WithArgs("trace1", int64(0), 1001).
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs?agentic_run_id=trace1&limit=9999", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHandleGetLogsTextFormat(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	ts1 := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 7, 9, 12, 0, 1, 0, time.UTC)

	// One string body (will be unquoted) and one object body (stays as JSON)
	rows := pgxmock.NewRows([]string{"id", "phase", "timestamp", "event", "body"}).
		AddRow(int64(1), "planning", ts1, "audit.agent.started", []byte(`"[agent] Starting query"`)).
		AddRow(int64(2), "planning", ts2, "audit.agent.tool.call", []byte(`{"tool":"bash"}`))

	mock.ExpectQuery(`SELECT id, phase, timestamp, event, body FROM templogs\.logs WHERE agentic_run_id = \$1 AND id > \$2 ORDER BY id ASC LIMIT \$3`).
		WithArgs("run-text", int64(0), 101).
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs?agentic_run_id=run-text&format=text", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	contentType := w.Header().Get("Content-Type")
	if contentType != "text/plain; charset=utf-8" {
		t.Errorf("expected text/plain content type, got %q", contentType)
	}

	body := w.Body.String()
	lines := strings.Split(strings.TrimRight(body, "\n"), "\n")

	// Metadata header: agentic_run_id, records, has_more, blank line
	if len(lines) < 5 {
		t.Fatalf("expected at least 5 lines, got %d: %q", len(lines), body)
	}
	if lines[0] != "agentic_run_id: run-text" {
		t.Errorf("line 0: expected 'agentic_run_id: run-text', got %q", lines[0])
	}
	if lines[1] != "records: 2" {
		t.Errorf("line 1: expected 'records: 2', got %q", lines[1])
	}
	if lines[2] != "has_more: false" {
		t.Errorf("line 2: expected 'has_more: false', got %q", lines[2])
	}
	if lines[3] != "" {
		t.Errorf("line 3: expected blank separator, got %q", lines[3])
	}

	// Record lines: string body is unquoted, object body stays as JSON
	expectedLine4 := "2026-07-09T12:00:00Z: [agent] Starting query"
	if lines[4] != expectedLine4 {
		t.Errorf("line 4: expected %q, got %q", expectedLine4, lines[4])
	}
	expectedLine5 := `2026-07-09T12:00:01Z: {"tool":"bash"}`
	if lines[5] != expectedLine5 {
		t.Errorf("line 5: expected %q, got %q", expectedLine5, lines[5])
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestShutdownNilServer(t *testing.T) {
	admin := &postgresAdmin{logger: zap.NewNop()}
	if err := admin.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEnsureTableCreatesSchemaAndTable(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectExec(`CREATE SCHEMA IF NOT EXISTS "templogs"`).
		WillReturnResult(pgxmock.NewResult("CREATE SCHEMA", 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "templogs"\."logs"`).
		WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_templogs_logs_agentic_run_id"`).
		WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_templogs_logs_run_phase"`).
		WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_templogs_logs_timestamp"`).
		WillReturnResult(pgxmock.NewResult("CREATE INDEX", 0))

	err := admin.ensureTable(context.Background())
	if err != nil {
		t.Fatalf("ensureTable failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestEnsureTableFailsOnSchemaError(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectExec(`CREATE SCHEMA IF NOT EXISTS "templogs"`).
		WillReturnError(fmt.Errorf("permission denied"))

	err := admin.ensureTable(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestEnsureTableFailsOnTableError(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectExec(`CREATE SCHEMA IF NOT EXISTS "templogs"`).
		WillReturnResult(pgxmock.NewResult("CREATE SCHEMA", 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "templogs"\."logs"`).
		WillReturnError(fmt.Errorf("permission denied"))

	err := admin.ensureTable(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestEnsureTableFailsOnIndexError(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectExec(`CREATE SCHEMA IF NOT EXISTS "templogs"`).
		WillReturnResult(pgxmock.NewResult("CREATE SCHEMA", 0))
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS "templogs"\."logs"`).
		WillReturnResult(pgxmock.NewResult("CREATE TABLE", 0))
	mock.ExpectExec(`CREATE INDEX IF NOT EXISTS "idx_templogs_logs_agentic_run_id"`).
		WillReturnError(fmt.Errorf("permission denied"))

	err := admin.ensureTable(context.Background())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
