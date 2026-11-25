package dataverseexporter

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
)

// requirePermanentUploadError asserts err is a permanent upload error with
// the given drop reason.
func requirePermanentUploadError(t *testing.T, err error, reason string) {
	t.Helper()
	require.Error(t, err)
	require.True(t, consumererror.IsPermanent(err),
		"error %v must be permanent (counted drop), not retryable", err)
	var perr *permanentUploadError
	require.True(t, errors.As(err, &perr),
		"error %v must wrap a permanentUploadError", err)
	require.Equal(t, reason, perr.reason)
}

// requireRetryableUploadError asserts err is not a permanent error.
func requireRetryableUploadError(t *testing.T, err error) {
	t.Helper()
	require.Error(t, err)
	require.False(t, consumererror.IsPermanent(err),
		"error %v must stay retryable", err)
}

// uploadWithStatus runs one upload against a fake ingress that always
// returns the given status.
func uploadWithStatus(t *testing.T, status int) error {
	t.Helper()
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid-1")
	var rec recordedUpload
	srv := fakeIngress(t, func(int) int { return status }, &rec)
	c := newTestIngressClient(srv, auth)
	_, err := c.upload(context.Background(), bytes.NewReader([]byte("x")))
	return err
}

func TestUploadPermanentHTTPStatuses(t *testing.T) {
	for _, status := range []int{
		http.StatusForbidden,             // 403
		http.StatusRequestEntityTooLarge, // 413
		http.StatusBadRequest,            // 400
	} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			err := uploadWithStatus(t, status)
			requirePermanentUploadError(t, err, fmt.Sprintf("http_%d", status))
		})
	}
}

func TestUploadRetryableHTTPStatuses(t *testing.T) {
	for _, status := range []int{
		http.StatusRequestTimeout,      // 408
		http.StatusTooManyRequests,     // 429
		http.StatusBadGateway,          // 502
		http.StatusInternalServerError, // 500
	} {
		t.Run(fmt.Sprintf("status_%d", status), func(t *testing.T) {
			err := uploadWithStatus(t, status)
			requireRetryableUploadError(t, err)
		})
	}
}

// TestUpload401AfterFailedRefreshStaysRetryable: a 401 that survives the
// refresh path (refresh allowed but the retried request still 401s) must
// stay retryable — it goes back to retry_on_failure, not a counted drop.
func TestUpload401AfterFailedRefreshStaysRetryable(t *testing.T) {
	cfg := atomic.Pointer[string]{}
	cfg.Store(ptr(testDockerConfig("tok-1")))
	auth := newTestAuth(t, &cfg, "cid-1")
	var rec recordedUpload
	srv := fakeIngress(t, func(int) int { return http.StatusUnauthorized }, &rec)
	c := newTestIngressClient(srv, auth)

	_, err := c.upload(context.Background(), bytes.NewReader([]byte("x")))
	requireRetryableUploadError(t, err)
	require.Equal(t, 2, rec.attempts, "initial + one refresh retry")
}

// newK8sAuthServer builds a fake Kubernetes API whose pull-secret endpoint
// responds with the given HTTP status.
func newK8sAuthServer(t *testing.T, secretStatus int) *clusterAuth {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/namespaces/openshift-config/secrets/pull-secret":
			w.WriteHeader(secretStatus)
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
	return &clusterAuth{
		apiURL:          srv.URL,
		tokenFile:       tokenFile,
		client:          srv.Client(),
		refreshInterval: time.Minute,
	}
}

// newShapeAuthServer builds a fake Kubernetes API serving the given raw
// dockerconfigjson (possibly malformed / missing the cloud entry).
func newShapeAuthServer(t *testing.T, dockerConfig string) *clusterAuth {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/namespaces/openshift-config/secrets/pull-secret":
			secret := map[string]map[string]string{
				"data": {".dockerconfigjson": base64.StdEncoding.EncodeToString([]byte(dockerConfig))},
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
	return &clusterAuth{
		apiURL:          srv.URL,
		tokenFile:       tokenFile,
		client:          srv.Client(),
		refreshInterval: time.Minute,
	}
}

func uploadViaAuth(t *testing.T, auth *clusterAuth) error {
	t.Helper()
	var rec recordedUpload
	srv := fakeIngress(t, func(int) int { return http.StatusAccepted }, &rec)
	c := newTestIngressClient(srv, auth)
	_, err := c.upload(context.Background(), bytes.NewReader([]byte("x")))
	return err
}

// TestUploadCredentialShapeErrorsArePermanent: a pull-secret without the
// cloud.openshift.com entry, or with a malformed .dockerconfigjson, is a
// permanent drop (reason "credentials") — retrying cannot fix the secret's
// shape.
func TestUploadCredentialShapeErrorsArePermanent(t *testing.T) {
	t.Run("missing_cloud_entry", func(t *testing.T) {
		auth := newShapeAuthServer(t, `{"auths":{"quay.io":{"auth":"other"}}}`)
		err := uploadViaAuth(t, auth)
		requirePermanentUploadError(t, err, "credentials")
		require.Contains(t, err.Error(), "cloud.openshift.com",
			"error text must keep the existing message")
	})
	t.Run("malformed_dockerconfigjson", func(t *testing.T) {
		auth := newShapeAuthServer(t, `not-json`)
		err := uploadViaAuth(t, auth)
		requirePermanentUploadError(t, err, "credentials")
		require.Contains(t, err.Error(), "parsing pull-secret dockerconfigjson",
			"error text must keep the existing message")
	})
}

// TestUploadCredentialAvailabilityErrorsAreRetryable: API-server failures
// while loading credentials are transient and must stay retryable.
func TestUploadCredentialAvailabilityErrorsAreRetryable(t *testing.T) {
	auth := newK8sAuthServer(t, http.StatusInternalServerError)
	err := uploadViaAuth(t, auth)
	requireRetryableUploadError(t, err)
}
