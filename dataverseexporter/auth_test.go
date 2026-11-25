package dataverseexporter

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeK8sAPI serves the pull-secret and clusterversion endpoints. The
// docker-config payload is read from a mutable variable so tests can
// simulate secret rotation between calls.
func fakeK8sAPI(t *testing.T, dockerConfigJSON *atomic.Pointer[string], clusterID string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer sa-token" {
			t.Errorf("Authorization = %q, want Bearer sa-token", got)
		}
		switch r.URL.Path {
		case "/api/v1/namespaces/openshift-config/secrets/pull-secret":
			secret := map[string]map[string]string{
				"data": {".dockerconfigjson": base64.StdEncoding.EncodeToString([]byte(*dockerConfigJSON.Load()))},
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(secret)
		case "/apis/config.openshift.io/v1/clusterversions/version":
			cv := map[string]any{"spec": map[string]any{"clusterID": clusterID}}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(cv)
		default:
			t.Errorf("unexpected API call: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func newTestAuth(t *testing.T, dockerConfigJSON *atomic.Pointer[string], clusterID string) *clusterAuth {
	t.Helper()
	srv := fakeK8sAPI(t, dockerConfigJSON, clusterID)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("sa-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	return &clusterAuth{
		apiURL:    srv.URL,
		tokenFile: tokenFile,
		client:    srv.Client(),
	}
}

func ptr(s string) *string { return &s }

func testDockerConfig(auth string) string {
	return `{"auths":{"cloud.openshift.com":{"auth":"` + auth + `"},"quay.io":{"auth":"other"}}}`
}

func TestUploadToken(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid")
	got, err := auth.UploadToken(context.Background(), false)
	if err != nil {
		t.Fatalf("UploadToken: %v", err)
	}
	if got != "tok-1" {
		t.Errorf("token = %q, want tok-1", got)
	}
}

func TestUploadTokenCacheAndRefresh(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid")
	if _, err := auth.UploadToken(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	// Without refresh: served from cache even as the API changes.
	cfg.Store(ptr(testDockerConfig("tok-2")))
	got, err := auth.UploadToken(context.Background(), false)
	if err != nil || got != "tok-1" {
		t.Fatalf("cached token = %q, %v; want tok-1", got, err)
	}
	// With refresh: re-read from the API.
	got, err = auth.UploadToken(context.Background(), true)
	if err != nil || got != "tok-2" {
		t.Fatalf("refreshed token = %q, %v; want tok-2", got, err)
	}
}

func TestUploadTokenMissingCloudEntry(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(`{"auths":{"quay.io":{"auth":"x"}}}`))
	auth := newTestAuth(t, &cfg, "cid")
	if _, err := auth.UploadToken(context.Background(), false); err == nil {
		t.Fatal("expected error for missing cloud.openshift.com entry")
	}
}

func TestClusterID(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, "my-cluster-id")
	got, err := auth.ClusterID(context.Background(), false)
	if err != nil {
		t.Fatalf("ClusterID: %v", err)
	}
	if got != "my-cluster-id" {
		t.Errorf("clusterID = %q, want my-cluster-id", got)
	}
}

func TestClusterIDEmpty(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, " ")
	if _, err := auth.ClusterID(context.Background(), false); err == nil {
		t.Fatal("expected error for empty spec.clusterID")
	}
}

func TestUploadTokenNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "  forbidden \n", http.StatusForbidden)
	}))
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("sa-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	auth := &clusterAuth{apiURL: srv.URL, tokenFile: tokenFile, client: srv.Client()}
	_, err := auth.UploadToken(context.Background(), false)
	if err == nil {
		t.Fatal("expected error on 403")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Errorf("error %q does not mention 403", err)
	}
	if !strings.Contains(err.Error(), "forbidden") {
		t.Errorf("error %q does not mention body text", err)
	}
}

func TestUploadTokenTokenFileMissing(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid")
	auth.tokenFile = filepath.Join(t.TempDir(), "does-not-exist")
	_, err := auth.UploadToken(context.Background(), false)
	if err == nil {
		t.Fatal("expected error for unreadable token file")
	}
	if !strings.Contains(err.Error(), "token") {
		t.Errorf("error %q does not mention the token file", err)
	}
}

// TestUploadTokenRefreshFailureClearsCache verifies that a failed refresh
// clears the stale cached token: the following non-refresh call must also
// fail instead of silently serving the known-bad old value.
func TestUploadTokenRefreshFailureClearsCache(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid")
	if _, err := auth.UploadToken(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	cfg.Store(ptr(`{"auths":{"quay.io":{"auth":"x"}}}`))
	if _, err := auth.UploadToken(context.Background(), true); err == nil {
		t.Fatal("expected refresh to fail with missing cloud.openshift.com entry")
	}
	if _, err := auth.UploadToken(context.Background(), false); err == nil {
		t.Fatal("expected non-refresh call after failed refresh to fail, not serve tok-1")
	}
}

// TestUploadTokenConcurrent exercises concurrent callers on both cache
// paths, including a refresh mixed in. Run with -race (go test -race);
// the race detector is what actually verifies the locking.
func TestUploadTokenConcurrent(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid")
	if _, err := auth.UploadToken(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	cfg.Store(ptr(testDockerConfig("tok-2")))

	const n = 10
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			got, err := auth.UploadToken(context.Background(), false)
			if err != nil {
				t.Errorf("UploadToken cached: %v", err)
				return
			}
			if got != "tok-1" && got != "tok-2" {
				t.Errorf("token = %q, want tok-1 or tok-2", got)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := auth.ClusterID(context.Background(), false); err != nil {
				t.Errorf("ClusterID: %v", err)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := auth.UploadToken(context.Background(), true); err != nil {
			t.Errorf("UploadToken refresh: %v", err)
		}
	}()
	wg.Wait()
}

func TestShouldRefresh(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok")))
	auth := newTestAuth(t, &cfg, "cid")
	auth.refreshInterval = 50 * time.Millisecond

	// No read has happened yet: refresh is allowed.
	if !auth.ShouldRefresh() {
		t.Fatal("ShouldRefresh = false before any token read; want true")
	}
	if _, err := auth.UploadToken(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	// Immediately after a read: refresh is rate-limited.
	if auth.ShouldRefresh() {
		t.Fatal("ShouldRefresh = true immediately after a token read; want false")
	}
	// After the interval elapses: allowed again.
	time.Sleep(75 * time.Millisecond)
	if !auth.ShouldRefresh() {
		t.Fatal("ShouldRefresh = false after refreshInterval elapsed; want true")
	}
}

// TestShouldRefreshRecordsFailedAttempts verifies the rate limiter is
// primed even when the pull-secret read fails: a failing API must not
// hammer the API server on every 401.
func TestShouldRefreshRecordsFailedAttempts(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(`{"auths":{"quay.io":{"auth":"x"}}}`)) // no cloud entry
	auth := newTestAuth(t, &cfg, "cid")
	auth.refreshInterval = time.Hour
	if _, err := auth.UploadToken(context.Background(), false); err == nil {
		t.Fatal("expected read to fail")
	}
	if auth.ShouldRefresh() {
		t.Fatal("ShouldRefresh = true after a failed read; want false (failures count as attempts)")
	}
}

// TestShouldRefreshIgnoresCacheHits is a regression test: cache hits
// (UploadToken with refresh=false on a populated cache) must NOT stamp
// lastTokenRead. Production uploads call that path before every request;
// if it stamped the read time, ShouldRefresh would never become true and
// a 401 after secret rotation would never trigger a refresh.
func TestShouldRefreshIgnoresCacheHits(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid")
	auth.refreshInterval = 100 * time.Millisecond

	// First read: real API read, stamps lastTokenRead.
	if _, err := auth.UploadToken(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	if auth.ShouldRefresh() {
		t.Fatal("ShouldRefresh = true immediately after a real read; want false")
	}

	// Simulate a burst of uploads spread over ~250ms — LONGER than the
	// 100ms refreshInterval — each a cache hit. Because the burst outlasts
	// the interval, ShouldRefresh must become true during it; stamping
	// lastTokenRead on cache hits would keep it false for the whole burst.
	for i := 0; i < 5; i++ {
		time.Sleep(50 * time.Millisecond)
		got, err := auth.UploadToken(context.Background(), false)
		if err != nil || got != "tok-1" {
			t.Fatalf("cached read %d = %q, %v; want tok-1", i, got, err)
		}
		if i >= 2 && !auth.ShouldRefresh() { // 100ms+ elapsed since the real read
			t.Fatalf("ShouldRefresh = false after %dms of cache hits; cache hits must not stamp/extend lastTokenRead", (i+1)*50)
		}
	}

	// Elapse the window since the last REAL read (the first one).
	if err := waitFor(150*time.Millisecond, func() bool { return auth.ShouldRefresh() }); err != nil {
		t.Fatal("ShouldRefresh never became true after the refreshInterval elapsed since the last real read")
	}

	// A refresh=true read re-reads the API and re-stamps.
	cfg.Store(ptr(testDockerConfig("tok-2")))
	got, err := auth.UploadToken(context.Background(), true)
	if err != nil || got != "tok-2" {
		t.Fatalf("refreshed token = %q, %v; want tok-2", got, err)
	}
	if auth.ShouldRefresh() {
		t.Fatal("ShouldRefresh = true immediately after a refresh read; want false")
	}
}

// waitFor polls cond until it returns true or the timeout elapses,
// making time-dependent assertions deterministic.
func waitFor(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	if cond() {
		return nil
	}
	return fmt.Errorf("condition not met within %v", timeout)
}

func TestNewClusterAuthRequiresInClusterEnv(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	t.Setenv("KUBERNETES_SERVICE_PORT", "")
	if _, err := newClusterAuth(); err == nil {
		t.Fatal("expected error outside a cluster")
	}
}

// writeTestCA generates a self-signed certificate PEM file and returns its
// path. Uses the standard crypto/ecdsa + x509.CreateCertificate pattern.
func writeTestCA(t *testing.T, dir string) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "ca.crt")
	pemFile, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pemFile.Close()
	if err := pem.Encode(pemFile, &pem.Block{Type: "CERTIFICATE", Bytes: der}); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestNewClusterAuthFromPaths(t *testing.T) {
	dir := t.TempDir()
	caFile := writeTestCA(t, dir)
	tokenFile := filepath.Join(dir, "token")
	if err := os.WriteFile(tokenFile, []byte("sa-token"), 0o600); err != nil {
		t.Fatal(err)
	}
	auth, err := newClusterAuthFromPaths("https://api.example:6443", tokenFile, caFile)
	if err != nil {
		t.Fatalf("newClusterAuthFromPaths: %v", err)
	}
	if auth.apiURL != "https://api.example:6443" {
		t.Errorf("apiURL = %q", auth.apiURL)
	}
	if auth.tokenFile != tokenFile {
		t.Errorf("tokenFile = %q", auth.tokenFile)
	}
	if auth.client == nil || auth.client.Transport == nil {
		t.Errorf("client = %+v, want non-nil client with transport", auth.client)
	}
	if auth.refreshInterval != time.Minute {
		t.Errorf("refreshInterval = %v, want 1m", auth.refreshInterval)
	}
	if auth.client.Timeout != 30*time.Second {
		t.Errorf("client timeout = %v, want 30s", auth.client.Timeout)
	}
}

func TestNewClusterAuthFromPathsUnparseableCA(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(caFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := newClusterAuthFromPaths("https://api.example:6443", filepath.Join(dir, "token"), caFile)
	if err == nil {
		t.Fatal("expected error for unparseable CA")
	}
	if !strings.Contains(err.Error(), caFile) {
		t.Errorf("error %q does not mention CA path %q", err, caFile)
	}
}

func TestNewClusterAuthFromPathsMissingCA(t *testing.T) {
	dir := t.TempDir()
	caFile := filepath.Join(dir, "missing.crt")
	_, err := newClusterAuthFromPaths("https://api.example:6443", filepath.Join(dir, "token"), caFile)
	if err == nil {
		t.Fatal("expected error for missing CA file")
	}
	if !strings.Contains(err.Error(), caFile) {
		t.Errorf("error %q does not mention CA path %q", err, caFile)
	}
}

// TestNewClusterAuthEnvResolution checks the URL formatting done by env
// resolution. The standard in-cluster CA path does not exist in tests, so
// the URL is asserted from the CA-read error path: the URL is resolved
// before the CA is read, and the error text carries the standard path.
func TestInClusterAPIURL(t *testing.T) {
	tests := []struct {
		name string
		host string
		port string
		want string
	}{
		{"ipv4", "10.0.0.1", "443", "https://10.0.0.1:443"},
		{"hostname", "kubernetes.default", "443", "https://kubernetes.default:443"},
		{"ipv6", "::1", "443", "https://[::1]:443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("KUBERNETES_SERVICE_HOST", tt.host)
			t.Setenv("KUBERNETES_SERVICE_PORT", tt.port)
			got, err := inClusterAPIURL()
			if err != nil {
				t.Fatalf("inClusterAPIURL: %v", err)
			}
			if got != tt.want {
				t.Errorf("URL = %q, want %q", got, tt.want)
			}
		})
	}
	t.Run("missing env", func(t *testing.T) {
		t.Setenv("KUBERNETES_SERVICE_HOST", "")
		t.Setenv("KUBERNETES_SERVICE_PORT", "")
		if _, err := inClusterAPIURL(); err == nil {
			t.Fatal("expected error outside a cluster")
		}
	})
}
