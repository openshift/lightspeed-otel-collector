package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestServer(t *testing.T) (*server, string) {
	t.Helper()
	dir := t.TempDir()
	return newServer(dir), dir
}

func setMode(t *testing.T, dir, mode string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "mode"), []byte(mode), 0o644); err != nil {
		t.Fatalf("write mode: %v", err)
	}
}

func postUpload(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "multipart/form-data; boundary=x", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	return resp
}

func readRecords(t *testing.T, dir string) []recordedRequest {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "requests.log"))
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("read requests.log: %v", err)
	}
	var records []recordedRequest
	for _, line := range strings.Split(strings.TrimSpace(string(b)), "\n") {
		if line == "" {
			continue
		}
		var rec recordedRequest
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("parse record %q: %v", line, err)
		}
		records = append(records, rec)
	}
	return records
}

func TestModeDispatch(t *testing.T) {
	cases := []struct {
		mode       string
		wantStatus int
	}{
		{"202", http.StatusAccepted},
		{"401", http.StatusUnauthorized},
		{"403", http.StatusForbidden},
		{"500", http.StatusInternalServerError},
		{"bogus", http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			srv, dir := newTestServer(t)
			ts := httptest.NewServer(srv)
			defer ts.Close()

			setMode(t, dir, tc.mode)
			resp := postUpload(t, ts.URL+uploadPath, "payload")
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("mode %q: status = %d, want %d", tc.mode, resp.StatusCode, tc.wantStatus)
			}
			if got := len(readRecords(t, dir)); got != 1 {
				t.Fatalf("mode %q: recorded %d requests, want 1", tc.mode, got)
			}
		})
	}
}

// TestModeDispatchDefault covers the 202 default when no mode file exists.
func TestModeDispatchDefault(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp := postUpload(t, ts.URL+uploadPath, "payload")
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("default mode: status = %d, want 202", resp.StatusCode)
	}
}

func TestRequestIDCounter(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	for i := 1; i <= 3; i++ {
		resp := postUpload(t, ts.URL+uploadPath, "payload")
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		var accepted struct {
			RequestID string `json:"request_id"`
		}
		if err := json.Unmarshal(body, &accepted); err != nil {
			t.Fatalf("decode 202 body %q: %v", body, err)
		}
		want := fmt.Sprintf("fake-%d", i)
		if accepted.RequestID != want {
			t.Fatalf("request_id = %q, want %q", accepted.RequestID, want)
		}
	}
}

// TestRequestRecording verifies the recorded headers and the base64 body
// round-trip.
func TestRequestRecording(t *testing.T) {
	srv, dir := newTestServer(t)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	body := "raw multipart bytes \x00\x01\xff"
	req, err := http.NewRequest(http.MethodPost, ts.URL+uploadPath, strings.NewReader(body))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("User-Agent", "openshift-lightspeed-operator/user-data-collection cluster/abc123")
	req.Header.Set("Content-Type", "multipart/form-data; boundary=x")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()

	records := readRecords(t, dir)
	if len(records) != 1 {
		t.Fatalf("recorded %d requests, want 1", len(records))
	}
	rec := records[0]
	log, err := os.ReadFile(filepath.Join(dir, "requests.log"))
	if err != nil || bytes.Contains(log, []byte("test-token")) || !bytes.Contains(log, []byte(`"has_bearer":true`)) {
		t.Fatal("record must report bearer presence without storing the credential")
	}
	if rec.UserAgent != "openshift-lightspeed-operator/user-data-collection cluster/abc123" {
		t.Errorf("UserAgent = %q", rec.UserAgent)
	}
	if rec.ContentType != "multipart/form-data; boundary=x" {
		t.Errorf("ContentType = %q", rec.ContentType)
	}
	if rec.Mode != "202" || rec.Status != http.StatusAccepted {
		t.Errorf("mode/status = %q/%d", rec.Mode, rec.Status)
	}
	decoded, err := base64.StdEncoding.DecodeString(rec.BodyB64)
	if err != nil {
		t.Fatalf("decode body_b64: %v", err)
	}
	if !bytes.Equal(decoded, []byte(body)) {
		t.Errorf("body round-trip = %q, want %q", decoded, body)
	}
}

func TestHealthz(t *testing.T) {
	srv, _ := newTestServer(t)
	ts := httptest.NewServer(srv)
	defer ts.Close()

	resp, err := http.Get(ts.URL + healthPath)
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", resp.StatusCode)
	}
}

// TestCertGenerationAndTLSHandshake generates the self-signed certificate
// and performs a real TLS handshake against a listener using it, trusting
// only the emitted CA PEM — the same trust model as the collector's
// tls.ca_file.
func TestCertGenerationAndTLSHandshake(t *testing.T) {
	for _, namespace := range []string{"", "e2e-test-ns"} {
		cert, caPEM, err := generateSelfSignedCert(namespace)
		if err != nil {
			t.Fatalf("namespace %q: generate: %v", namespace, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			t.Fatalf("namespace %q: CA PEM not parseable", namespace)
		}

		listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
		})
		if err != nil {
			t.Fatalf("namespace %q: listen: %v", namespace, err)
		}
		go http.Serve(listener, http.NotFoundHandler())
		defer listener.Close()

		client := &http.Client{
			Timeout: 5 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"},
			},
		}
		resp, err := client.Get("https://" + listener.Addr().String() + healthPath)
		if err != nil {
			t.Fatalf("namespace %q: TLS request: %v", namespace, err)
		}
		resp.Body.Close()

		// Service DNS SANs must be present when a namespace is given.
		if namespace != "" {
			leaf, err := x509.ParseCertificate(cert.Certificate[0])
			if err != nil {
				t.Fatalf("parse leaf: %v", err)
			}
			want := "fake-ingress." + namespace + ".svc"
			found := false
			for _, name := range leaf.DNSNames {
				if name == want {
					found = true
				}
			}
			if !found {
				t.Fatalf("certificate DNS SANs %v lack %q", leaf.DNSNames, want)
			}
		}
	}
}

// TestRunWritesCAAndReadyMarker exercises run() end to end on an ephemeral
// port and data directory.
func TestRunWritesCAAndReadyMarker(t *testing.T) {
	dir := t.TempDir()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	done := make(chan error, 1)
	go func() { done <- run(addr, dir, "/nonexistent/namespace-file") }()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(dir, "ready")); err == nil {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("run exited early: %v", err)
		default:
		}
		time.Sleep(50 * time.Millisecond)
	}
	if _, err := os.Stat(filepath.Join(dir, "ready")); err != nil {
		t.Fatalf("ready marker never appeared: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "ca.pem")); err != nil {
		t.Fatalf("ca.pem never appeared: %v", err)
	}
}
