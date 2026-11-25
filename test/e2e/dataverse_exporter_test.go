//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

// These tests exercise the dataverse exporter end to end against a fake
// ingress deployed inside the ephemeral namespace. They run only in the
// Konflux IntegrationTestScenario (cluster + OTEL_COLLECTOR_IMAGE); all
// cluster mutations are scoped to the ephemeral namespace or restored,
// and unavailability of prerequisites (shell in the collector image,
// pull-secret access) skips rather than fails.

// TestDataverseQuietClusterSelectionAndFlush verifies .ai/spec/what/data-collection.md
// rules 2, 3, 5, 8, 9 (and parent agentic-data-collection.md rules 24, 25, 26, 29):
//
//   - rule 2: only service.name lightspeed-agentic-operator/sandbox traces
//     reach traces/data_collection (routing via the connector table);
//   - rule 3: non-matching service.name values are excluded — a batch
//     with service.name "something-else" never uploads;
//   - rule 5: the product pipeline terminates in the dataverse exporter's
//     own queue-based batching (no pipeline-level batch processor);
//   - rule 8: a single small batch on a quiet cluster is delivered by the
//     flush_timeout alone — no volume-based trigger is needed;
//   - rule 9: the upload wire shape is byte-compatible with the retired
//     Python sidecar: multipart field "file", filename
//     lightspeed-assistant.tgz, part content type
//     application/vnd.redhat.ols.periodic+tar, a single gzip TAR member
//     named agentic-v1/traces-*.json holding a one-element JSON array of
//     the OTLP JSON batch.
//
// For rule 11 (parent rules 24/29), the fake ingress records whether a
// non-empty bearer token was present, not its value. The test also checks
// that User-Agent contains the cluster ID from ClusterVersion.
func TestDataverseQuietClusterSelectionAndFlush(t *testing.T) {
	collector := sharedDataverseCollector(t)
	clusterID := getClusterID(t)

	fakeIngressSetMode(t, "202")

	// One small batch from an allowlisted service must flush on
	// flush_timeout alone within the poll budget (3 minutes).
	marker := fmt.Sprintf("dv-quiet-%d", time.Now().UnixNano())
	sendDataverseTraceBatch(t, collector.otlpAddr, "lightspeed-agentic-sandbox", marker)

	pollUntil(t, pollTimeout, 2*time.Second,
		fmt.Sprintf("upload of quiet-cluster batch %q", marker), func() bool {
			return len(dataverseRequestsForMarker(t, marker)) >= 1
		})

	uploads := dataverseRequestsForMarker(t, marker)
	if len(uploads) != 1 {
		t.Fatalf("batch %q uploaded %d times, want exactly 1", marker, len(uploads))
	}
	assertDataverseUpload(t, uploads[0], clusterID, marker)

	// A non-matching service.name must never reach the exporter.
	otherMarker := marker + "-excluded"
	sendDataverseTraceBatch(t, collector.otlpAddr, "something-else", otherMarker)
	time.Sleep(dataverseNoUploadWait)
	if got := len(dataverseRequestsForMarker(t, otherMarker)); got != 0 {
		t.Fatalf("non-matching service.name produced %d uploads, want 0 (rules 2, 3)", got)
	}
}

// TestDataverseCredentialDegradationAndSelfHeal verifies .ai/spec/what/data-collection.md
// rule 11 (and parent agentic-data-collection.md rules 30, 35): a
// credential AVAILABILITY failure — the Kubernetes API rejecting the
// pull-secret read after RBAC revocation — is retryable. The collector
// pod stays Ready with no crashloop, the producer-side OTLP send
// succeeds (enqueue-time acknowledgement), nothing is uploaded while
// the read fails, and once access is re-granted the SAME batch is
// delivered by the retry loop with no pod restart. The credential
// SHAPE failure (missing cloud.openshift.com entry -> permanent
// counted drop) cannot be tested at cluster level on hosted clusters,
// where the pull-secret is controller-managed and re-synced; it is
// covered by the dataverseexporter unit tests.
func TestDataverseCredentialDegradationAndSelfHeal(t *testing.T) {
	requireCloudCredential(t)
	ensureDataverseRBAC(t)
	ensureFakeIngress(t)

	fakeIngressSetMode(t, "202")
	testStart := time.Now().Add(-time.Minute)

	// Fresh collector instance: no cached credentials, so the first
	// upload attempt must fetch them through the Kubernetes API.
	collector := deployDataverseCollector(t)
	podName := collector.podName(t)
	podStart := collector.podStartTime(t)
	if restarts := collector.podRestartCount(t); restarts != "0" {
		t.Fatalf("collector pod restarted %s times during startup, want 0", restarts)
	}

	// Revoke pull-secret read: the API server now returns 403 for the
	// credential fetch — an availability error, retryable within the
	// budget (rule 11). Re-grant before the test ends either way.
	revokeDataversePullSecretRBAC(t)
	t.Cleanup(func() { grantDataversePullSecretRBAC(t) })

	marker := fmt.Sprintf("dv-cred-%d", time.Now().UnixNano())
	sendDataverseTraceBatch(t, collector.otlpAddr, "lightspeed-agentic-sandbox", marker)

	// While revoked: no upload, the collector stays healthy, and the
	// batch is retried (not dropped — availability errors exhaust the
	// retry budget before dropping, and this window is far inside it).
	time.Sleep(30 * time.Second)
	if got := len(dataverseRequestsForMarker(t, marker)); got != 0 {
		t.Fatalf("batch %q uploaded %d times while credential read was revoked, want 0", marker, got)
	}
	if restarts := collector.podRestartCount(t); restarts != "0" {
		t.Fatalf("collector pod restarted %s times while credential read failed, want 0 (rule 11: no crashloop)", restarts)
	}
	if logs := collector.logsSince(t, testStart); strings.Contains(logs, "dropping trace batch") {
		t.Fatalf("availability error was treated as a permanent drop:\n%s", logs)
	}

	// Re-grant access: the retry loop re-reads the credentials on its
	// next attempt and delivers the SAME batch — no restart involved.
	grantDataversePullSecretRBAC(t)

	clusterID := getClusterID(t)
	pollUntil(t, pollTimeout, 2*time.Second,
		fmt.Sprintf("upload of revoked-phase batch %q after re-grant", marker), func() bool {
			return len(dataverseRequestsForMarker(t, marker)) >= 1
		})
	uploads := dataverseRequestsForMarker(t, marker)
	if len(uploads) != 1 {
		t.Fatalf("batch %q uploaded %d times, want exactly 1", marker, len(uploads))
	}
	assertDataverseUpload(t, uploads[0], clusterID, marker)

	// No restart at any point (rule 11 / parent rule 35).
	if collector.podName(t) != podName || collector.podStartTime(t) != podStart {
		t.Fatalf("collector pod was replaced during the test (was %s started %s, now %s started %s)",
			podName, podStart, collector.podName(t), collector.podStartTime(t))
	}
}

// TestDataverseRetryThenRecover verifies .ai/spec/what/data-collection.md
// rule 12 (and parent agentic-data-collection.md rules 31, 33): HTTP 5xx
// from the ingress is retryable within the configured retry budget — the
// batch is attempted repeatedly (request log grows, no delivery while the
// ingress fails) — and once the ingress recovers, the SAME batch is
// delivered late rather than lost.
func TestDataverseRetryThenRecover(t *testing.T) {
	collector := sharedDataverseCollector(t)

	// Fail persistently: retries must pile up without delivery.
	fakeIngressSetMode(t, "500")
	marker := fmt.Sprintf("dv-retry-%d", time.Now().UnixNano())
	sendDataverseTraceBatch(t, collector.otlpAddr, "lightspeed-agentic-sandbox", marker)

	pollUntil(t, pollTimeout, 2*time.Second,
		fmt.Sprintf("repeated retry attempts for batch %q", marker), func() bool {
			return len(dataverseRequestsForMarker(t, marker)) >= 2
		})
	for _, req := range dataverseRequestsForMarker(t, marker) {
		if req.Status == http.StatusAccepted {
			t.Fatalf("batch %q was accepted while the ingress returned 500", marker)
		}
	}

	// Recover: the queued batch must be delivered, not lost.
	fakeIngressSetMode(t, "202")
	pollUntil(t, pollTimeout, 2*time.Second,
		fmt.Sprintf("late delivery of retried batch %q", marker), func() bool {
			for _, req := range dataverseRequestsForMarker(t, marker) {
				if req.Status == http.StatusAccepted {
					return true
				}
			}
			return false
		})
	clusterID := getClusterID(t)
	for _, req := range dataverseRequestsForMarker(t, marker) {
		if req.Status == http.StatusAccepted {
			assertDataverseUpload(t, req, clusterID, marker)
		}
	}
}

// TestDataversePermanentDropIsCounted verifies .ai/spec/what/data-collection.md
// rules 6, 12 (and parent agentic-data-collection.md rule 31): HTTP 403
// from the ingress is a permanent failure — exactly ONE attempt (no
// retries), no delivery, and a drop log naming the reason "http_403".
// Metrics scraping is out of scope here; the logs carry the assertion.
func TestDataversePermanentDropIsCounted(t *testing.T) {
	collector := sharedDataverseCollector(t)

	fakeIngressSetMode(t, "403")
	testStart := time.Now().Add(-time.Minute)
	marker := fmt.Sprintf("dv-drop403-%d", time.Now().UnixNano())
	sendDataverseTraceBatch(t, collector.otlpAddr, "lightspeed-agentic-sandbox", marker)

	// The single attempt must arrive (flush_timeout) ...
	pollUntil(t, pollTimeout, 2*time.Second,
		fmt.Sprintf("the single 403 attempt for batch %q", marker), func() bool {
			return len(dataverseRequestsForMarker(t, marker)) >= 1
		})

	// ... and the drop must be logged with the counted reason.
	pollUntil(t, pollTimeout, 2*time.Second,
		fmt.Sprintf("permanent-drop log for batch %q", marker), func() bool {
			logs := collector.logsSince(t, testStart)
			return strings.Contains(logs, "dropping trace batch after permanent upload error") &&
				strings.Contains(logs, "http_403")
		})

	// No retries: the attempt count must stay at exactly one well past
	// several retry intervals, and nothing may be delivered.
	time.Sleep(30 * time.Second)
	requests := dataverseRequestsForMarker(t, marker)
	if len(requests) != 1 {
		t.Fatalf("batch %q saw %d attempts, want exactly 1 (403 is permanent: no retries)", marker, len(requests))
	}
	if requests[0].Status != http.StatusForbidden {
		t.Fatalf("recorded attempt status = %d, want 403", requests[0].Status)
	}
}

// TestDataverseQueueFullIsolation verifies .ai/spec/what/data-collection.md
// rule 13 (and parent agentic-data-collection.md rule 32): with a tiny
// queue and an unresponsive (hanging) ingress, the exporter's queue fills
// and refuses batches — but the queue-full condition is swallowed inside
// the exporter wrapper: producer-side OTLP export calls keep succeeding
// (no error returned to the sender, so no producer retry and no duplicate
// delivery to other trace consumers), and a rate-limited queue-full
// warning appears in the collector logs.
func TestDataverseQueueFullIsolation(t *testing.T) {
	ensureDataverseRBAC(t)
	requireCloudCredential(t)

	fakeIngressSetMode(t, "hang")
	testStart := time.Now().Add(-time.Minute)

	// Own instance: tiny byte queue with a small (reachable) batch
	// minimum — exporterhelper validation requires min_size <=
	// queue_size, and the queue still fills because the single consumer
	// is stuck on the hung upload while further batches enqueue behind
	// it. The flush timeout stays beyond the test.
	collector := deployDataverseCollector(t, func(cfg *dataverseCollectorConfig) {
		cfg.queueSize = 64 << 10 // 64 KiB
		cfg.minSize = 4 << 10    // 4 KiB (must be <= queue_size)
		cfg.flushTimeout = 10 * time.Minute
		cfg.retryElapsed = 30 * time.Minute
	})

	queueFull := func() bool {
		return strings.Contains(collector.logsSince(t, testStart), "queue full")
	}

	// Pump batches until the queue fills. Every send must succeed —
	// that IS the isolation assertion (a failed send would mean the
	// queue-full error escaped the exporter wrapper).
	const batchPayload = 4 << 10 // 4 KiB per batch
	sent := 0
	for i := 0; i < 2000 && !queueFull(); i++ {
		marker := fmt.Sprintf("dv-queue-%d-%04d", time.Now().UnixNano(), i)
		if err := sendDataverseTraceBatchErr(t, collector.otlpAddr, "lightspeed-agentic-sandbox", marker, batchPayload); err != nil {
			t.Fatalf("OTLP export %d failed with the dataverse queue full — queue-full error escaped the exporter wrapper (rule 13): %v", i, err)
		}
		sent++
		// The first 4 KiB batch flushes to the hung consumer and
		// blocks it; later batches pile up in the queue. Check the
		// logs only periodically.
		if i%50 == 49 {
			time.Sleep(time.Second)
		}
	}
	if sent == 0 {
		t.Fatal("no batches could be sent")
	}
	if !queueFull() {
		t.Fatalf("queue-full warning never appeared after %d batches (%d KiB) against a hanging ingress", sent, sent*batchPayload>>10)
	}

	// The queue is full and the ingress hangs: sends must STILL succeed.
	for i := 0; i < 50; i++ {
		marker := fmt.Sprintf("dv-queue-post-%d-%04d", time.Now().UnixNano(), i)
		if err := sendDataverseTraceBatchErr(t, collector.otlpAddr, "lightspeed-agentic-sandbox", marker, batchPayload); err != nil {
			t.Fatalf("OTLP export failed with the dataverse queue full — producer-side failure (rule 13): %v", err)
		}
	}
}
