//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// --- shared helpers for these tests ---

type logRecord struct {
	ID        int64           `json:"id"`
	Phase     string          `json:"phase"`
	Timestamp string          `json:"timestamp"`
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

type deleteResponse struct {
	Deleted      int64  `json:"deleted"`
	AgenticRunID string `json:"agentic_run_id"`
	Error        string `json:"error,omitempty"`
}

func adminURL(env *testEnv, path string) string {
	return fmt.Sprintf("http://%s%s", env.Endpoints.AdminAPI, path)
}

func sendLogs(t *testing.T, env *testEnv, records []logAttrs) {
	t.Helper()
	if err := trySendLogs(env, records); err != nil {
		t.Fatalf("sendLogs: %v", err)
	}
}

// trySendLogs is the goroutine-safe variant that returns an error instead of calling t.Fatal.
func trySendLogs(env *testEnv, records []logAttrs) error {
	ctx := context.Background()

	conn, err := grpc.NewClient(
		env.Endpoints.OTLPgRPC,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("dial collector: %w", err)
	}
	defer func() { _ = conn.Close() }()

	exporter, err := otlploggrpc.New(ctx, otlploggrpc.WithGRPCConn(conn))
	if err != nil {
		return fmt.Errorf("create log exporter: %w", err)
	}

	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)),
	)
	defer func() { _ = provider.Shutdown(ctx) }()

	logger := provider.Logger("e2e-test")

	for _, r := range records {
		var rec log.Record
		rec.SetTimestamp(time.Now())
		rec.SetBody(attribute.StringValue(r.body))
		rec.AddAttributes(
			attribute.String("agenticrun.uid", r.runID),
			attribute.String("agenticrun.phase", r.phase),
			attribute.String("event", r.event),
		)
		logger.Emit(ctx, rec)
	}

	if err := provider.ForceFlush(ctx); err != nil {
		return fmt.Errorf("flush logs: %w", err)
	}
	return nil
}

type logAttrs struct {
	runID string
	phase string
	event string
	body  string
}

func waitForRecordCount(t *testing.T, env *testEnv, runID string, expected int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var count int
		err := env.PGPool.QueryRow(context.Background(),
			"SELECT count(*) FROM templogs.logs WHERE agentic_run_id = $1", runID).Scan(&count)
		if err != nil {
			t.Logf("waitForRecordCount: query error (will retry): %v", err)
		} else if count >= expected {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("timed out waiting for %d records with run_id=%s", expected, runID)
}

// --- Test 2: Admin API GET with phase filter + pagination ---

func TestAdminAPIGet(t *testing.T) {
	runID := "e2e-get-" + fmt.Sprint(time.Now().UnixMilli())

	// Send 5 records: 3 in "planning", 2 in "execution".
	var records []logAttrs
	for i := 0; i < 3; i++ {
		records = append(records, logAttrs{
			runID: runID, phase: "planning",
			event: fmt.Sprintf("plan.step.%d", i), body: fmt.Sprintf("plan step %d", i),
		})
	}
	for i := 0; i < 2; i++ {
		records = append(records, logAttrs{
			runID: runID, phase: "execution",
			event: fmt.Sprintf("exec.step.%d", i), body: fmt.Sprintf("exec step %d", i),
		})
	}
	sendLogs(t, env, records)
	waitForRecordCount(t, env, runID, 5, 10*time.Second)

	// GET all records for run.
	resp, err := http.Get(adminURL(env, "/api/v1/logs?agentic_run_id="+runID))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET status %d: %s", resp.StatusCode, body)
	}

	var getResp getResponse
	if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
		t.Fatalf("decode GET response: %v", err)
	}
	if len(getResp.Records) != 5 {
		t.Errorf("expected 5 records, got %d", len(getResp.Records))
	}
	if getResp.HasMore {
		t.Error("expected has_more=false")
	}

	// GET with phase filter.
	resp2, err := http.Get(adminURL(env, "/api/v1/logs?agentic_run_id="+runID+"&phase=planning"))
	if err != nil {
		t.Fatalf("GET phase filter: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()

	var getResp2 getResponse
	if err := json.NewDecoder(resp2.Body).Decode(&getResp2); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(getResp2.Records) != 3 {
		t.Errorf("expected 3 planning records, got %d", len(getResp2.Records))
	}

	// GET with pagination: limit=2.
	resp3, err := http.Get(adminURL(env, "/api/v1/logs?agentic_run_id="+runID+"&limit=2"))
	if err != nil {
		t.Fatalf("GET paginated: %v", err)
	}
	defer func() { _ = resp3.Body.Close() }()

	var getResp3 getResponse
	if err := json.NewDecoder(resp3.Body).Decode(&getResp3); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(getResp3.Records) != 2 {
		t.Fatalf("expected 2 records (page 1), got %d", len(getResp3.Records))
	}
	if !getResp3.HasMore {
		t.Error("expected has_more=true for first page")
	}

	// GET next page using cursor.
	cursor := getResp3.Records[1].ID
	resp4, err := http.Get(adminURL(env, fmt.Sprintf("/api/v1/logs?agentic_run_id=%s&limit=2&after=%d", runID, cursor)))
	if err != nil {
		t.Fatalf("GET page 2: %v", err)
	}
	defer func() { _ = resp4.Body.Close() }()

	var getResp4 getResponse
	if err := json.NewDecoder(resp4.Body).Decode(&getResp4); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(getResp4.Records) != 2 {
		t.Errorf("expected 2 records (page 2), got %d", len(getResp4.Records))
	}

	// Verify text format.
	resp5, err := http.Get(adminURL(env, "/api/v1/logs?agentic_run_id="+runID+"&format=text"))
	if err != nil {
		t.Fatalf("GET text: %v", err)
	}
	defer func() { _ = resp5.Body.Close() }()

	if ct := resp5.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Errorf("expected text/plain content-type, got %q", ct)
	}
	body, _ := io.ReadAll(resp5.Body)
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) < 2 {
		t.Fatalf("text format: expected at least 2 lines, got %d", len(lines))
	}
	if !strings.HasPrefix(lines[0], "agentic_run_id: "+runID) {
		t.Errorf("text format: unexpected first line: %q", lines[0])
	}
	if lines[1] != "records: 5" {
		t.Errorf("text format: expected 'records: 5', got %q", lines[1])
	}
}

// --- Test 3: Admin API DELETE ---

func TestAdminAPIDelete(t *testing.T) {
	runID := "e2e-del-" + fmt.Sprint(time.Now().UnixMilli())

	// Insert some records.
	var records []logAttrs
	for i := 0; i < 5; i++ {
		records = append(records, logAttrs{
			runID: runID, phase: "planning",
			event: "step", body: fmt.Sprintf("record %d", i),
		})
	}
	sendLogs(t, env, records)
	waitForRecordCount(t, env, runID, 5, 10*time.Second)

	// DELETE all.
	req, err := http.NewRequest(http.MethodDelete, adminURL(env, "/api/v1/logs?agentic_run_id="+runID), nil)
	if err != nil {
		t.Fatalf("build DELETE request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("DELETE status %d: %s", resp.StatusCode, body)
	}

	var delResp deleteResponse
	if err := json.NewDecoder(resp.Body).Decode(&delResp); err != nil {
		t.Fatalf("decode DELETE response: %v", err)
	}
	if delResp.Deleted != 5 {
		t.Errorf("expected deleted=5, got %d", delResp.Deleted)
	}
	if delResp.AgenticRunID != runID {
		t.Errorf("expected agentic_run_id=%q, got %q", runID, delResp.AgenticRunID)
	}

	// Verify records are gone.
	var count int
	err = env.PGPool.QueryRow(context.Background(),
		"SELECT count(*) FROM templogs.logs WHERE agentic_run_id = $1", runID).Scan(&count)
	if err != nil {
		t.Fatalf("count after delete: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records after delete, got %d", count)
	}
}

// --- Test 4: Large batch (1000 records) ---

func TestLargeBatch(t *testing.T) {
	runID := "e2e-batch-" + fmt.Sprint(time.Now().UnixMilli())

	records := make([]logAttrs, 1000)
	for i := range records {
		records[i] = logAttrs{
			runID: runID, phase: "batch",
			event: fmt.Sprintf("event.%d", i), body: fmt.Sprintf("batch record %d", i),
		}
	}
	sendLogs(t, env, records)
	waitForRecordCount(t, env, runID, 1000, 30*time.Second)

	// Verify via Admin API that pagination works for a large set.
	resp, err := http.Get(adminURL(env, fmt.Sprintf("/api/v1/logs?agentic_run_id=%s&limit=100", runID)))
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var getResp getResponse
	if err := json.NewDecoder(resp.Body).Decode(&getResp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(getResp.Records) != 100 {
		t.Errorf("expected 100 records, got %d", len(getResp.Records))
	}
	if !getResp.HasMore {
		t.Error("expected has_more=true for 1000 total records")
	}
}

// --- Test 5: Concurrent clients (5 goroutines) ---

func TestConcurrentClients(t *testing.T) {
	const numClients = 5
	const recordsPerClient = 20

	prefix := fmt.Sprintf("e2e-conc-%d", time.Now().UnixMilli())

	errCh := make(chan error, numClients)
	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			runID := fmt.Sprintf("%s-%d", prefix, clientID)
			records := make([]logAttrs, recordsPerClient)
			for j := range records {
				records[j] = logAttrs{
					runID: runID, phase: "concurrent",
					event: fmt.Sprintf("client%d.step%d", clientID, j),
					body:  fmt.Sprintf("client %d record %d", clientID, j),
				}
			}
			if err := trySendLogs(env, records); err != nil {
				errCh <- fmt.Errorf("client %d: %w", clientID, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Fatalf("sendLogs failed: %v", err)
	}

	// Wait for all records to land.
	time.Sleep(5 * time.Second)

	// Verify total count via direct PG query.
	var count int
	err := env.PGPool.QueryRow(context.Background(),
		"SELECT count(*) FROM templogs.logs WHERE agentic_run_id LIKE $1", prefix+"-%").Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	expected := numClients * recordsPerClient
	if count != expected {
		t.Errorf("expected %d records from concurrent clients, got %d", expected, count)
	}
}
