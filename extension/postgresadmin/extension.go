package postgresadmin

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension"
	"go.uber.org/zap"
)

const (
	defaultLimit = 100
	maxLimit     = 1000
)

// pool abstracts pgxpool.Pool for testability.
type pool interface {
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...interface{}) (pgconn.CommandTag, error)
	Close()
}

type postgresAdmin struct {
	config *Config
	logger *zap.Logger
	pool   pool
	server *http.Server
}

var _ extension.Extension = (*postgresAdmin)(nil)

func newPostgresAdmin(set extension.Settings, cfg *Config) (*postgresAdmin, error) {
	return &postgresAdmin{
		config: cfg,
		logger: set.Logger,
	}, nil
}

func (p *postgresAdmin) Start(ctx context.Context, _ component.Host) error {
	pool, err := pgxpool.New(ctx, p.config.ConnectionString)
	if err != nil {
		return fmt.Errorf("postgres_admin: failed to create connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("postgres_admin: failed to ping postgres: %w", err)
	}
	p.pool = pool

	if err := p.ensureTable(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("postgres_admin: failed to ensure table: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/logs", p.handleGetLogs)
	mux.HandleFunc("DELETE /api/v1/logs", p.handleDeleteLogs)

	listener, err := net.Listen("tcp", p.config.Endpoint)
	if err != nil {
		pool.Close()
		return fmt.Errorf("postgres_admin: failed to listen on %s: %w", p.config.Endpoint, err)
	}

	p.server = &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	p.logger.Info("postgres_admin extension started",
		zap.String("endpoint", p.config.Endpoint),
		zap.Bool("tls", p.config.tlsEnabled()),
	)

	go func() {
		var err error
		if p.config.tlsEnabled() {
			err = p.server.ServeTLS(listener, p.config.TLSCertFile, p.config.TLSKeyFile)
		} else {
			err = p.server.Serve(listener)
		}
		if err != nil && err != http.ErrServerClosed {
			p.logger.Error("postgres_admin server error", zap.Error(err))
		}
	}()

	return nil
}

func (p *postgresAdmin) Shutdown(ctx context.Context) error {
	if p.server != nil {
		if err := p.server.Shutdown(ctx); err != nil {
			p.logger.Warn("postgres_admin: error shutting down HTTP server", zap.Error(err))
		}
	}
	if p.pool != nil {
		p.pool.Close()
	}
	return nil
}

// ensureTable creates the schema, table, and indexes if they don't already
// exist, and migrates existing installations from the old schema (trace_id
// column) to the new schema (agentic_run_id + phase columns). All statements
// are idempotent and safe to run on every startup. Extensions start before
// pipelines in the OTel Collector lifecycle, so the table is guaranteed to
// exist before the exporter writes its first batch.
func (p *postgresAdmin) ensureTable(ctx context.Context) error {
	safeSchema := pgx.Identifier{p.config.Schema}.Sanitize()
	safeTable := pgx.Identifier{p.config.Schema, p.config.LogsTable}.Sanitize()

	createSchema := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, safeSchema)
	if _, err := p.pool.Exec(ctx, createSchema); err != nil {
		return fmt.Errorf("create schema %s: %w", safeSchema, err)
	}

	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id              BIGSERIAL PRIMARY KEY,
			agentic_run_id  TEXT NOT NULL,
			phase           TEXT NOT NULL DEFAULT '',
			timestamp       TIMESTAMPTZ NOT NULL,
			event           TEXT NOT NULL,
			body            JSONB
		)`, safeTable)
	if _, err := p.pool.Exec(ctx, createTable); err != nil {
		return fmt.Errorf("create table %s: %w", safeTable, err)
	}

	// Migration: handle existing installations with old schema (trace_id column).
	// Check if trace_id column exists and migrate if needed.
	if err := p.migrateFromTraceID(ctx, safeTable); err != nil {
		return fmt.Errorf("migrate from trace_id: %w", err)
	}

	safeIdxRunID := pgx.Identifier{fmt.Sprintf("idx_%s_%s_agentic_run_id", p.config.Schema, p.config.LogsTable)}.Sanitize()
	safeIdxRunPhase := pgx.Identifier{fmt.Sprintf("idx_%s_%s_run_phase", p.config.Schema, p.config.LogsTable)}.Sanitize()
	safeIdxTimestamp := pgx.Identifier{fmt.Sprintf("idx_%s_%s_timestamp", p.config.Schema, p.config.LogsTable)}.Sanitize()
	indexes := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (agentic_run_id)`, safeIdxRunID, safeTable),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (agentic_run_id, phase)`, safeIdxRunPhase, safeTable),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (timestamp)`, safeIdxTimestamp, safeTable),
	}
	for _, idx := range indexes {
		if _, err := p.pool.Exec(ctx, idx); err != nil {
			return fmt.Errorf("create index: %w", err)
		}
	}

	p.logger.Info("postgres_admin: table ready", zap.String("table", safeTable))
	return nil
}

// migrateFromTraceID handles migration from old schema (trace_id column) to
// new schema (agentic_run_id + phase). This is idempotent - running multiple
// times is safe. If the table already has the new schema, this is a no-op.
func (p *postgresAdmin) migrateFromTraceID(ctx context.Context, safeTable string) error {
	// Check if trace_id column exists
	checkCol := fmt.Sprintf(`
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = $1 AND table_name = $2 AND column_name = 'trace_id'
		)`)

	var hasTraceID bool
	rows, err := p.pool.Query(ctx, checkCol, p.config.Schema, p.config.LogsTable)
	if err != nil {
		return fmt.Errorf("query trace_id column: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		if err := rows.Scan(&hasTraceID); err != nil {
			return fmt.Errorf("scan trace_id existence: %w", err)
		}
	} else {
		return fmt.Errorf("no rows returned from column existence check")
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate column check results: %w", err)
	}

	if !hasTraceID {
		// Table already has new schema, no migration needed
		return nil
	}

	p.logger.Info("postgres_admin: migrating from trace_id to agentic_run_id schema")

	// Drop old indexes on trace_id before renaming column
	oldIdxTraceID := pgx.Identifier{fmt.Sprintf("idx_%s_%s_trace_id", p.config.Schema, p.config.LogsTable)}.Sanitize()
	dropOldIdx := fmt.Sprintf(`DROP INDEX IF EXISTS %s`, oldIdxTraceID)
	if _, err := p.pool.Exec(ctx, dropOldIdx); err != nil {
		return fmt.Errorf("drop old trace_id index: %w", err)
	}

	// Rename trace_id to agentic_run_id
	renameCol := fmt.Sprintf(`ALTER TABLE %s RENAME COLUMN trace_id TO agentic_run_id`, safeTable)
	if _, err := p.pool.Exec(ctx, renameCol); err != nil {
		return fmt.Errorf("rename trace_id to agentic_run_id: %w", err)
	}

	// Add phase column if it doesn't exist (it shouldn't if trace_id exists)
	addPhase := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS phase TEXT NOT NULL DEFAULT ''`, safeTable)
	if _, err := p.pool.Exec(ctx, addPhase); err != nil {
		return fmt.Errorf("add phase column: %w", err)
	}

	p.logger.Info("postgres_admin: migration from trace_id to agentic_run_id completed")
	return nil
}

// --- GET /api/v1/logs?agentic_run_id=<value>&phase=<phase>&limit=100&after=12345 ---

func writeJSON(w http.ResponseWriter, v any) {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

type logRecord struct {
	ID        int64           `json:"id"`
	Phase     string          `json:"phase"`
	Timestamp time.Time       `json:"timestamp"`
	Event     string          `json:"event"`
	Body      json.RawMessage `json:"body"`
}

type getResponse struct {
	AgenticRunID string      `json:"agentic_run_id"`
	Phase        string      `json:"phase,omitempty"`
	Records      []logRecord `json:"records"`
	HasMore      bool        `json:"has_more"`
	Error        string      `json:"error,omitempty"`
}

func (p *postgresAdmin) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	agenticRunID := r.URL.Query().Get("agentic_run_id")
	phase := r.URL.Query().Get("phase")

	w.Header().Set("Content-Type", "application/json")

	if agenticRunID == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, getResponse{
			Error: "'agentic_run_id' query parameter is required",
		})
		return
	}

	limit := defaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil || parsed < 1 {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, getResponse{
				Error: "'limit' must be a positive integer",
			})
			return
		}
		limit = parsed
	}
	if limit > maxLimit {
		limit = maxLimit
	}

	var after int64
	if v := r.URL.Query().Get("after"); v != "" {
		parsed, err := strconv.ParseInt(v, 10, 64)
		if err != nil || parsed < 0 {
			w.WriteHeader(http.StatusBadRequest)
			writeJSON(w, getResponse{
				Error: "'after' must be a non-negative integer",
			})
			return
		}
		after = parsed
	}

	var query string
	var args []interface{}
	if phase != "" {
		query = fmt.Sprintf(
			"SELECT id, phase, timestamp, event, body FROM %s WHERE agentic_run_id = $1 AND phase = $2 AND id > $3 ORDER BY id ASC LIMIT $4",
			p.config.qualifiedTable(),
		)
		args = []interface{}{agenticRunID, phase, after, limit + 1}
	} else {
		query = fmt.Sprintf(
			"SELECT id, phase, timestamp, event, body FROM %s WHERE agentic_run_id = $1 AND id > $2 ORDER BY id ASC LIMIT $3",
			p.config.qualifiedTable(),
		)
		args = []interface{}{agenticRunID, after, limit + 1}
	}

	rows, err := p.pool.Query(r.Context(), query, args...)
	if err != nil {
		p.logger.Error("postgres_admin: query failed",
			zap.String("agentic_run_id", agenticRunID),
			zap.Error(err),
		)
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, getResponse{
			Error:        "query failed; check collector logs",
			AgenticRunID: agenticRunID,
		})
		return
	}
	defer rows.Close()

	records := make([]logRecord, 0, limit)
	for rows.Next() {
		var rec logRecord
		var body []byte
		if err := rows.Scan(&rec.ID, &rec.Phase, &rec.Timestamp, &rec.Event, &body); err != nil {
			p.logger.Error("postgres_admin: row scan failed", zap.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, getResponse{
				Error:        "failed to read results; check collector logs",
				AgenticRunID: agenticRunID,
			})
			return
		}
		rec.Body = json.RawMessage(body)
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		p.logger.Error("postgres_admin: rows iteration failed", zap.Error(err))
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, getResponse{
			Error:        "failed to read results; check collector logs",
			AgenticRunID: agenticRunID,
		})
		return
	}

	hasMore := len(records) > limit
	if hasMore {
		records = records[:limit]
	}

	// Note: error responses always return JSON regardless of the format parameter.
	// This is intentional — errors are consumed programmatically (status code + message).
	if r.URL.Query().Get("format") == "text" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, "agentic_run_id: %s\n", agenticRunID)
		_, _ = fmt.Fprintf(w, "records: %d\n", len(records))
		_, _ = fmt.Fprintf(w, "has_more: %v\n", hasMore)
		_, _ = fmt.Fprintln(w)
		for _, rec := range records {
			var unquoted string
			if err := json.Unmarshal(rec.Body, &unquoted); err == nil {
				_, _ = fmt.Fprintf(w, "%s: %s\n", rec.Timestamp.Format(time.RFC3339Nano), unquoted)
			} else {
				_, _ = fmt.Fprintf(w, "%s: %s\n", rec.Timestamp.Format(time.RFC3339Nano), string(rec.Body))
			}
		}
		return
	}

	w.WriteHeader(http.StatusOK)
	writeJSON(w, getResponse{
		AgenticRunID: agenticRunID,
		Phase:        phase,
		Records:      records,
		HasMore:      hasMore,
	})
}

// --- DELETE /api/v1/logs?agentic_run_id=<value> ---

type deleteResponse struct {
	Deleted      int64  `json:"deleted"`
	AgenticRunID string `json:"agentic_run_id"`
	Error        string `json:"error,omitempty"`
}

func (p *postgresAdmin) handleDeleteLogs(w http.ResponseWriter, r *http.Request) {
	agenticRunID := r.URL.Query().Get("agentic_run_id")

	w.Header().Set("Content-Type", "application/json")

	if agenticRunID == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, deleteResponse{
			Error: "'agentic_run_id' query parameter is required",
		})
		return
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE agentic_run_id = $1", p.config.qualifiedTable())

	ct, err := p.pool.Exec(r.Context(), query, agenticRunID)
	if err != nil {
		p.logger.Error("postgres_admin: delete failed",
			zap.String("agentic_run_id", agenticRunID),
			zap.Error(err),
		)
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, deleteResponse{
			Error:        "delete query failed; check collector logs",
			AgenticRunID: agenticRunID,
		})
		return
	}

	rowsAffected := ct.RowsAffected()

	p.logger.Info("postgres_admin: deleted log records",
		zap.String("agentic_run_id", agenticRunID),
		zap.Int64("deleted", rowsAffected),
	)

	w.WriteHeader(http.StatusOK)
	writeJSON(w, deleteResponse{
		Deleted:      rowsAffected,
		AgenticRunID: agenticRunID,
	})
}
