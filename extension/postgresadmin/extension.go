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
// exist. All statements use IF NOT EXISTS so this is idempotent and safe to
// run on every startup. Extensions start before pipelines in the OTel
// Collector lifecycle, so the table is guaranteed to exist before the
// exporter writes its first batch.
func (p *postgresAdmin) ensureTable(ctx context.Context) error {
	safeSchema := pgx.Identifier{p.config.Schema}.Sanitize()
	safeTable := pgx.Identifier{p.config.Schema, p.config.LogsTable}.Sanitize()

	createSchema := fmt.Sprintf(`CREATE SCHEMA IF NOT EXISTS %s`, safeSchema)
	if _, err := p.pool.Exec(ctx, createSchema); err != nil {
		return fmt.Errorf("create schema %s: %w", safeSchema, err)
	}

	createTable := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         BIGSERIAL PRIMARY KEY,
			trace_id   TEXT NOT NULL,
			timestamp  TIMESTAMPTZ NOT NULL,
			event      TEXT NOT NULL,
			body       JSONB
		)`, safeTable)
	if _, err := p.pool.Exec(ctx, createTable); err != nil {
		return fmt.Errorf("create table %s: %w", safeTable, err)
	}

	safeIdxTraceID := pgx.Identifier{fmt.Sprintf("idx_%s_%s_trace_id", p.config.Schema, p.config.LogsTable)}.Sanitize()
	safeIdxTimestamp := pgx.Identifier{fmt.Sprintf("idx_%s_%s_timestamp", p.config.Schema, p.config.LogsTable)}.Sanitize()
	indexes := []string{
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (trace_id)`, safeIdxTraceID, safeTable),
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

// --- GET /api/v1/logs?trace_id=<value>&limit=100&after=12345 ---

func writeJSON(w http.ResponseWriter, v any) {
	if err := json.NewEncoder(w).Encode(v); err != nil {
		http.Error(w, "failed to encode response", http.StatusInternalServerError)
	}
}

type logRecord struct {
	ID        int64           `json:"id"`
	Timestamp time.Time       `json:"timestamp"`
	Event     string          `json:"event"`
	Body      json.RawMessage `json:"body"`
}

type getResponse struct {
	TraceID string      `json:"trace_id"`
	Records []logRecord `json:"records"`
	HasMore bool        `json:"has_more"`
	Error   string      `json:"error,omitempty"`
}

func (p *postgresAdmin) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	traceID := r.URL.Query().Get("trace_id")

	w.Header().Set("Content-Type", "application/json")

	if traceID == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, getResponse{
			Error: "'trace_id' query parameter is required",
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

	query := fmt.Sprintf(
		"SELECT id, timestamp, event, body FROM %s WHERE trace_id = $1 AND id > $2 ORDER BY id ASC LIMIT $3",
		p.config.qualifiedTable(),
	)

	rows, err := p.pool.Query(r.Context(), query, traceID, after, limit+1)
	if err != nil {
		p.logger.Error("postgres_admin: query failed",
			zap.String("trace_id", traceID),
			zap.Error(err),
		)
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, getResponse{
			Error:   "query failed; check collector logs",
			TraceID: traceID,
		})
		return
	}
	defer rows.Close()

	records := make([]logRecord, 0, limit)
	for rows.Next() {
		var rec logRecord
		var body []byte
		if err := rows.Scan(&rec.ID, &rec.Timestamp, &rec.Event, &body); err != nil {
			p.logger.Error("postgres_admin: row scan failed", zap.Error(err))
			w.WriteHeader(http.StatusInternalServerError)
			writeJSON(w, getResponse{
				Error:   "failed to read results; check collector logs",
				TraceID: traceID,
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
			Error:   "failed to read results; check collector logs",
			TraceID: traceID,
		})
		return
	}

	hasMore := len(records) > limit
	if hasMore {
		records = records[:limit]
	}

	w.WriteHeader(http.StatusOK)
	writeJSON(w, getResponse{
		TraceID: traceID,
		Records: records,
		HasMore: hasMore,
	})
}

// --- DELETE /api/v1/logs?trace_id=<value> ---

type deleteResponse struct {
	Deleted int64  `json:"deleted"`
	TraceID string `json:"trace_id"`
	Error   string `json:"error,omitempty"`
}

func (p *postgresAdmin) handleDeleteLogs(w http.ResponseWriter, r *http.Request) {
	traceID := r.URL.Query().Get("trace_id")

	w.Header().Set("Content-Type", "application/json")

	if traceID == "" {
		w.WriteHeader(http.StatusBadRequest)
		writeJSON(w, deleteResponse{
			Error: "'trace_id' query parameter is required",
		})
		return
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE trace_id = $1", p.config.qualifiedTable())

	ct, err := p.pool.Exec(r.Context(), query, traceID)
	if err != nil {
		p.logger.Error("postgres_admin: delete failed",
			zap.String("trace_id", traceID),
			zap.Error(err),
		)
		w.WriteHeader(http.StatusInternalServerError)
		writeJSON(w, deleteResponse{
			Error:   "delete query failed; check collector logs",
			TraceID: traceID,
		})
		return
	}

	rowsAffected := ct.RowsAffected()

	p.logger.Info("postgres_admin: deleted log records",
		zap.String("trace_id", traceID),
		zap.Int64("deleted", rowsAffected),
	)

	w.WriteHeader(http.StatusOK)
	writeJSON(w, deleteResponse{
		Deleted: rowsAffected,
		TraceID: traceID,
	})
}
