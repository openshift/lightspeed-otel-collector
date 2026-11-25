package dataverseexporter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
)

type recordedUpload struct {
	authorization string
	userAgent     string
	contentType   string
	body          []byte
	attempts      int
}

func fakeIngress(t *testing.T, status func(call int) int, rec *recordedUpload) *httptest.Server {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		body, _ := io.ReadAll(r.Body)
		rec.attempts = calls
		rec.authorization = r.Header.Get("Authorization")
		rec.userAgent = r.Header.Get("User-Agent")
		rec.contentType = r.Header.Get("Content-Type")
		rec.body = body
		code := status(calls)
		if code == http.StatusAccepted {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(map[string]string{"request_id": "req-123"})
			return
		}
		w.WriteHeader(code)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestIngressClient(srv *httptest.Server, auth *clusterAuth) *ingressClient {
	return &ingressClient{
		client:    srv.Client(),
		endpoint:  srv.URL,
		serviceID: "ols",
		auth:      auth,
		logger:    zap.NewNop(),
	}
}

func TestUploadSuccess(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid-1")
	var rec recordedUpload
	srv := fakeIngress(t, func(int) int { return http.StatusAccepted }, &rec)
	c := newTestIngressClient(srv, auth)

	requestID, err := c.upload(context.Background(), bytes.NewReader([]byte("tarball-bytes")))
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	if requestID != "req-123" {
		t.Errorf("request_id = %q, want req-123", requestID)
	}
	if rec.authorization != "Bearer tok-1" {
		t.Errorf("Authorization = %q, want Bearer tok-1", rec.authorization)
	}
	if rec.userAgent != "openshift-lightspeed-operator/user-data-collection cluster/cid-1" {
		t.Errorf("User-Agent = %q", rec.userAgent)
	}
	// multipart request: content type must carry the multipart boundary,
	// and the body must contain our part with the vendored content type.
	if rec.contentType == "" || !bytes.Contains(rec.body, []byte("tarball-bytes")) {
		t.Error("expected multipart body and content type")
	}
	if !bytes.Contains(rec.body, []byte("application/vnd.redhat.ols.periodic+tar")) {
		t.Error("expected part content type application/vnd.redhat.ols.periodic+tar in body")
	}
	if !bytes.Contains(rec.body, []byte(`name="file"`)) ||
		!bytes.Contains(rec.body, []byte("lightspeed-assistant.tgz")) {
		t.Error("expected multipart field name file and filename lightspeed-assistant.tgz")
	}
}

func TestUploadNon202IsError(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid-1")
	var rec recordedUpload
	srv := fakeIngress(t, func(int) int { return http.StatusInternalServerError }, &rec)
	c := newTestIngressClient(srv, auth)

	if _, err := c.upload(context.Background(), bytes.NewReader([]byte("x"))); err == nil {
		t.Fatal("expected error on 500")
	}
	if rec.attempts != 1 {
		t.Errorf("attempts = %d, want 1 (no retry on 500)", rec.attempts)
	}
}

func TestUploadRetriesOnceOn401(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid-1")
	var rec recordedUpload
	srv := fakeIngress(t, func(call int) int {
		if call == 1 {
			// Rotate the served token before the retry lands.
			cfg.Store(ptr(testDockerConfig("tok-2")))
			return http.StatusUnauthorized
		}
		return http.StatusAccepted
	}, &rec)
	c := newTestIngressClient(srv, auth)

	requestID, err := c.upload(context.Background(), bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("upload after 401 retry: %v", err)
	}
	if requestID != "req-123" {
		t.Errorf("request_id = %q", requestID)
	}
	if rec.authorization != "Bearer tok-2" {
		t.Errorf("Authorization after retry = %q, want Bearer tok-2", rec.authorization)
	}
	if rec.attempts != 2 {
		t.Errorf("attempts = %d, want 2", rec.attempts)
	}
}

func TestUploadDoesNotRetry401Twice(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid-1")
	var rec recordedUpload
	srv := fakeIngress(t, func(int) int { return http.StatusUnauthorized }, &rec)
	c := newTestIngressClient(srv, auth)

	if _, err := c.upload(context.Background(), bytes.NewReader([]byte("x"))); err == nil {
		t.Fatal("expected error when 401 persists after one retry")
	}
	if rec.attempts != 2 {
		t.Errorf("attempts = %d, want exactly 2 (initial + one retry)", rec.attempts)
	}
	if len(rec.body) == 0 {
		t.Error("expected requests to have been made")
	}
}

func TestUploadContextCancellation(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid-1")
	started := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusAccepted)
	}))
	t.Cleanup(srv.CloseClientConnections)
	t.Cleanup(srv.Close)
	t.Cleanup(srv.Close)
	c := newTestIngressClient(srv, auth)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-started
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		_, err := c.upload(ctx, bytes.NewReader([]byte("x")))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error on cancelled context")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want context.Canceled", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("upload did not return promptly after context cancellation")
	}
}

// TestUpload401WithinRefreshWindowReturnsError verifies the rate-limited
// refresh: when the pull-secret was read less than refreshInterval ago, a
// 401 must return immediately — no second pull-secret API call, no
// re-send with the known-stale token.
func TestUpload401WithinRefreshWindowReturnsError(t *testing.T) {
	var pullSecretCalls atomic.Int64
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/namespaces/openshift-config/secrets/pull-secret":
			pullSecretCalls.Add(1)
			secret := map[string]map[string]string{
				"data": {".dockerconfigjson": base64.StdEncoding.EncodeToString([]byte(*cfg.Load()))},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(secret)
		case "/apis/config.openshift.io/v1/clusterversions/version":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"spec": map[string]any{"clusterID": "cid-1"}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("sa-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	auth := &clusterAuth{
		apiURL:          srv.URL,
		tokenFile:       tokenFile,
		client:          srv.Client(),
		refreshInterval: time.Hour, // window far from elapsing
	}

	// Prime the token cache: this counts as the read attempt.
	if _, err := auth.UploadToken(context.Background(), false); err != nil {
		t.Fatal(err)
	}

	var rec recordedUpload
	ingress := fakeIngress(t, func(int) int { return http.StatusUnauthorized }, &rec)
	c := newTestIngressClient(ingress, auth)

	if _, err := c.upload(context.Background(), bytes.NewReader([]byte("x"))); err == nil {
		t.Fatal("expected 401 to surface as an error inside the refresh window")
	}
	if rec.attempts != 1 {
		t.Errorf("ingress attempts = %d; want exactly 1 (no re-send of a known-stale token)", rec.attempts)
	}
	if got := pullSecretCalls.Load(); got != 1 {
		t.Errorf("pull-secret API calls = %d; want exactly 1 (refresh must be rate-limited)", got)
	}
}

// TestUpload401RefreshesAfterWindowElapsesWithCacheHits is the end-to-end
// regression test for the rate-limited 401 refresh path: uploads served
// from the token cache must not keep the refresh window alive. Inside the
// window a 401 is returned as-is (1 attempt); once the window elapses
// since the last real read, a 401 refreshes the token and the retried
// upload succeeds with the rotated token.
func TestUpload401RefreshesAfterWindowElapsesWithCacheHits(t *testing.T) {
	var pullSecretCalls atomic.Int64
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/namespaces/openshift-config/secrets/pull-secret":
			pullSecretCalls.Add(1)
			secret := map[string]map[string]string{
				"data": {".dockerconfigjson": base64.StdEncoding.EncodeToString([]byte(*cfg.Load()))},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(secret)
		case "/apis/config.openshift.io/v1/clusterversions/version":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"spec": map[string]any{"clusterID": "cid-1"}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("sa-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	auth := &clusterAuth{
		apiURL:          srv.URL,
		tokenFile:       tokenFile,
		client:          srv.Client(),
		refreshInterval: 400 * time.Millisecond,
	}

	// Prime the token cache with a real read.
	if _, err := auth.UploadToken(context.Background(), false); err != nil {
		t.Fatal(err)
	}

	// Rotate the secret: only a fresh read will pick up tok-2.
	cfg.Store(ptr(testDockerConfig("tok-2")))

	var rec recordedUpload
	// Ingress 401s until a refresh actually happens: first the primed
	// (stale tok-1) request inside the window, then the first request of
	// the post-window upload; its retry carries the rotated token.
	ingress := fakeIngress(t, func(call int) int {
		if call <= 2 {
			return http.StatusUnauthorized
		}
		return http.StatusAccepted
	}, &rec)
	c := newTestIngressClient(ingress, auth)

	if _, err := c.upload(context.Background(), bytes.NewReader([]byte("x"))); err == nil {
		t.Fatal("expected 401 inside the refresh window to surface as an error")
	}
	if rec.attempts != 1 {
		t.Fatalf("ingress attempts = %d; want exactly 1 inside the refresh window", rec.attempts)
	}
	if got := pullSecretCalls.Load(); got != 1 {
		t.Fatalf("pull-secret API calls = %d; want exactly 1 inside the refresh window", got)
	}

	// Upload a few more times from the cache while the window is still
	// comfortably open — cache hits must not extend the window. With a
	// 400ms interval and ~60ms of loop sleeps there is a wide margin
	// before the window elapses.
	for i := 0; i < 3; i++ {
		if _, err := auth.UploadToken(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Wait deterministically until the window has elapsed since the last
	// real read, then let the next upload succeed after the refresh.
	if err := waitFor(2*time.Second, func() bool { return auth.ShouldRefresh() }); err != nil {
		t.Fatal("refresh window never elapsed; cache hits appear to have extended it")
	}
	requestID, err := c.upload(context.Background(), bytes.NewReader([]byte("x")))
	if err != nil {
		t.Fatalf("upload after window elapsed: %v", err)
	}
	if requestID != "req-123" {
		t.Errorf("request_id = %q", requestID)
	}
	if rec.authorization != "Bearer tok-2" {
		t.Errorf("Authorization = %q; want Bearer tok-2 (rotated token)", rec.authorization)
	}
	if got := pullSecretCalls.Load(); got != 2 {
		t.Errorf("pull-secret API calls = %d; want exactly 2 (prime + one refresh)", got)
	}
}

func TestUploadRefreshFailureSurfaces(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid-1")
	var rec recordedUpload
	srv := fakeIngress(t, func(call int) int {
		// Rotate to a pull secret with no cloud.openshift.com entry before
		// the forced refresh, so the refresh fails.
		cfg.Store(ptr(`{"auths":{"quay.io":{"auth":"other"}}}`))
		return http.StatusUnauthorized
	}, &rec)
	c := newTestIngressClient(srv, auth)

	_, err := c.upload(context.Background(), bytes.NewReader([]byte("x")))
	if err == nil {
		t.Fatal("expected refresh failure to surface")
	}
	if !strings.Contains(err.Error(), "cloud.openshift.com") {
		t.Errorf("error = %v, want it to mention cloud.openshift.com", err)
	}
	if rec.attempts != 1 {
		t.Errorf("attempts = %d, want exactly 1 (no retry when refresh fails)", rec.attempts)
	}
}

func TestUploadErrorBodyTruncated(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid-1")
	big := strings.Repeat("a", 8<<10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, big)
	}))
	t.Cleanup(srv.Close)
	c := newTestIngressClient(srv, auth)

	_, err := c.upload(context.Background(), bytes.NewReader([]byte("x")))
	if err == nil {
		t.Fatal("expected error on 500")
	}
	if len(err.Error()) > 1200 {
		t.Errorf("error string length = %d, want < 1200", len(err.Error()))
	}
	if !strings.Contains(err.Error(), strings.Repeat("a", 512)) {
		t.Error("expected truncated body prefix in error")
	}
}

func TestUpload202MissingRequestIDIsError(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid-1")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{}`))
	}))
	t.Cleanup(srv.Close)
	c := newTestIngressClient(srv, auth)

	if _, err := c.upload(context.Background(), bytes.NewReader([]byte("x"))); err == nil {
		t.Fatal("expected error when 202 response is missing request_id")
	}
}
