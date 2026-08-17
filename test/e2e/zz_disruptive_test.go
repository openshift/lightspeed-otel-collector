//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Disruptive tests — these kill pods and reconnect port-forwards.
// They run last (file sorts alphabetically after admin/ingest).

func TestRetryOnPGRestart(t *testing.T) {
	runID := "e2e-retry-" + fmt.Sprint(time.Now().UnixMilli())

	sendLogs(t, env, []logAttrs{
		{runID: runID, phase: "before", event: "pre-restart", body: "before pg restart"},
	})
	waitForRecordCount(t, env, runID, 1, 10*time.Second)

	t.Log("restarting postgres pod")
	kubectl(t, "delete", "pod", "-n", env.Namespace, "-l", "app=postgres", "--wait=true", "--timeout=60s")
	waitForRollout(t, env.Namespace, "statefulset", "postgres")

	// Reconnect PG port-forward and pool after pod restart.
	t.Log("reconnecting PG port-forward")
	env.PGPool.Close()
	env.PGPool = env.PortForwards.ReconnectPG()

	// Send records AFTER restart — the collector should reconnect and deliver.
	sendLogs(t, env, []logAttrs{
		{runID: runID, phase: "after", event: "post-restart", body: "after pg restart"},
	})
	waitForRecordCount(t, env, runID, 2, 60*time.Second)

	var count int
	err := env.PGPool.QueryRow(context.Background(),
		"SELECT count(*) FROM templogs.logs WHERE agentic_run_id = $1", runID).Scan(&count)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 records (before + after restart), got %d", count)
	}
}

func TestGracefulShutdown(t *testing.T) {
	runID := "e2e-shutdown-" + fmt.Sprint(time.Now().UnixMilli())

	records := make([]logAttrs, 50)
	for i := range records {
		records[i] = logAttrs{
			runID: runID, phase: "shutdown",
			event: fmt.Sprintf("step.%d", i), body: fmt.Sprintf("record %d", i),
		}
	}
	sendLogs(t, env, records)

	// Give the batch processor time to flush before killing.
	time.Sleep(3 * time.Second)

	t.Log("restarting collector pod")
	kubectl(t, "delete", "pod", "-n", env.Namespace, "-l", "app=otel-collector", "--wait=true",
		"--timeout=30s")

	waitForRollout(t, env.Namespace, "deployment", "otel-collector")

	// Reconnect collector port-forwards (new ports).
	t.Log("reconnecting collector port-forwards")
	env.PortForwards.ReconnectCollector(env)

	waitForRecordCount(t, env, runID, 50, 30*time.Second)
}
