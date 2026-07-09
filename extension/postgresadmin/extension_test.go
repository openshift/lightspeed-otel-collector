package postgresadmin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

func TestHandleDeleteLogsMissingTraceID(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/logs", nil)
	w := httptest.NewRecorder()
	admin.handleDeleteLogs(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	var resp deleteResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error == "" {
		t.Error("expected error message in response")
	}
}

func TestHandleDeleteLogsSuccess(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectExec(`DELETE FROM templogs\.logs WHERE trace_id = \$1`).
		WithArgs("abc123def456abc123def456abc123de").
		WillReturnResult(pgxmock.NewResult("DELETE", 5))

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/logs?trace_id=abc123def456abc123def456abc123de", nil)
	w := httptest.NewRecorder()
	admin.handleDeleteLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp deleteResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Deleted != 5 {
		t.Errorf("expected 5 deleted, got %d", resp.Deleted)
	}
	if resp.TraceID != "abc123def456abc123def456abc123de" {
		t.Errorf("expected trace_id in response, got %q", resp.TraceID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHandleDeleteLogsZeroRows(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectExec(`DELETE FROM templogs\.logs WHERE trace_id = \$1`).
		WithArgs("nonexistent").
		WillReturnResult(pgxmock.NewResult("DELETE", 0))

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/logs?trace_id=nonexistent", nil)
	w := httptest.NewRecorder()
	admin.handleDeleteLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp deleteResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Deleted != 0 {
		t.Errorf("expected 0 deleted, got %d", resp.Deleted)
	}
}

func TestHandleDeleteLogsDBError(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	mock.ExpectExec(`DELETE FROM templogs\.logs WHERE trace_id = \$1`).
		WithArgs("abc123").
		WillReturnError(context.DeadlineExceeded)

	req := httptest.NewRequest(http.MethodDelete, "/api/v1/logs?trace_id=abc123", nil)
	w := httptest.NewRecorder()
	admin.handleDeleteLogs(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandleGetLogsMissingTraceID(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	var resp getResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if resp.Error == "" {
		t.Error("expected error message in response")
	}
}

func TestHandleGetLogsSuccess(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	ts1 := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2026, 7, 9, 12, 0, 1, 0, time.UTC)

	rows := pgxmock.NewRows([]string{"id", "timestamp", "event", "body"}).
		AddRow(int64(1), ts1, "audit.agent.started", []byte(`{"msg":"hello"}`)).
		AddRow(int64(2), ts2, "audit.agent.tool.call", []byte(`{"tool":"bash"}`))

	mock.ExpectQuery(`SELECT id, timestamp, event, body FROM templogs\.logs WHERE trace_id = \$1 AND id > \$2 ORDER BY id ASC LIMIT \$3`).
		WithArgs("abc123", int64(0), 101).
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs?trace_id=abc123", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp getResponse
	json.NewDecoder(w.Body).Decode(&resp)
	if len(resp.Records) != 2 {
		t.Errorf("expected 2 records, got %d", len(resp.Records))
	}
	if resp.HasMore {
		t.Error("expected has_more=false with 2 records")
	}
	if resp.TraceID != "abc123" {
		t.Errorf("expected trace_id=abc123, got %q", resp.TraceID)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestHandleGetLogsWithPagination(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	ts := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)

	rows := pgxmock.NewRows([]string{"id", "timestamp", "event", "body"}).
		AddRow(int64(11), ts, "audit.agent.started", []byte(`{}`)).
		AddRow(int64(12), ts, "audit.agent.text", []byte(`{}`)).
		AddRow(int64(13), ts, "audit.agent.tool.call", []byte(`{}`))

	mock.ExpectQuery(`SELECT id, timestamp, event, body FROM templogs\.logs WHERE trace_id = \$1 AND id > \$2 ORDER BY id ASC LIMIT \$3`).
		WithArgs("trace1", int64(10), 3).
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs?trace_id=trace1&limit=2&after=10", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d; body: %s", w.Code, w.Body.String())
	}

	var resp getResponse
	json.NewDecoder(w.Body).Decode(&resp)
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

	mock.ExpectQuery(`SELECT id, timestamp, event, body FROM templogs\.logs`).
		WithArgs("abc", int64(0), 101).
		WillReturnError(context.DeadlineExceeded)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs?trace_id=abc", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestHandleGetLogsLimitCapped(t *testing.T) {
	admin, mock := newTestAdmin(t)
	defer mock.Close()

	rows := pgxmock.NewRows([]string{"id", "timestamp", "event", "body"})

	mock.ExpectQuery(`SELECT id, timestamp, event, body FROM templogs\.logs`).
		WithArgs("trace1", int64(0), 1001).
		WillReturnRows(rows)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/logs?trace_id=trace1&limit=9999", nil)
	w := httptest.NewRecorder()
	admin.handleGetLogs(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
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
