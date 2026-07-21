package httpsmetrics

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/extension/extensiontest"
)

func TestProxyForwardsMetrics(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("# HELP otelcol_exporter_sent total\notelcol_exporter_sent 42\n"))
	}))
	defer upstream.Close()

	cfg := &Config{
		Endpoint: "127.0.0.1:0",
		Upstream: upstream.URL,
	}

	set := extensiontest.NewNopSettings(Type)
	ext := newHTTPSMetrics(set, cfg)

	err := ext.Start(context.Background(), componenttest.NewNopHost())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = ext.Shutdown(context.Background()) }()

	addr := ext.server.Addr
	if l, ok := getListenerAddr(ext); ok {
		addr = l
	}

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Fatal("expected non-empty body")
	}
}

func TestNonMetricsPathReturns404(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	cfg := &Config{
		Endpoint: "127.0.0.1:0",
		Upstream: upstream.URL,
	}

	set := extensiontest.NewNopSettings(Type)
	ext := newHTTPSMetrics(set, cfg)

	err := ext.Start(context.Background(), componenttest.NewNopHost())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = ext.Shutdown(context.Background()) }()

	addr := ext.server.Addr
	if l, ok := getListenerAddr(ext); ok {
		addr = l
	}

	resp, err := http.Get("http://" + addr + "/other")
	if err != nil {
		t.Fatalf("GET /other failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestShutdownNilServer(t *testing.T) {
	ext := &httpsMetrics{config: &Config{}}
	if err := ext.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown on nil server should not error: %v", err)
	}
}

func TestStartFailsOnInvalidUpstream(t *testing.T) {
	cfg := &Config{
		Endpoint: "127.0.0.1:0",
		Upstream: "://bad-url",
	}

	set := extensiontest.NewNopSettings(Type)
	ext := newHTTPSMetrics(set, cfg)

	err := ext.Start(context.Background(), componenttest.NewNopHost())
	if err == nil {
		_ = ext.Shutdown(context.Background())
		t.Fatal("expected error for invalid upstream URL")
	}
}

func TestTLSServesHTTPS(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("otelcol_up 1\n"))
	}))
	defer upstream.Close()

	certFile, keyFile := generateSelfSignedCert(t)

	cfg := &Config{
		Endpoint:    "127.0.0.1:0",
		Upstream:    upstream.URL,
		TLSCertFile: certFile,
		TLSKeyFile:  keyFile,
	}

	set := extensiontest.NewNopSettings(Type)
	ext := newHTTPSMetrics(set, cfg)

	err := ext.Start(context.Background(), componenttest.NewNopHost())
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer func() { _ = ext.Shutdown(context.Background()) }()

	addr, _ := getListenerAddr(ext)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}

	resp, err := client.Get("https://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("GET /metrics over TLS failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "otelcol_up 1\n" {
		t.Fatalf("expected upstream content, got %q", body)
	}
}

func TestStartFailsMissingCertFile(t *testing.T) {
	cfg := &Config{
		Endpoint:    "127.0.0.1:0",
		Upstream:    "http://127.0.0.1:18888/metrics",
		TLSCertFile: "/nonexistent/tls.crt",
		TLSKeyFile:  "/nonexistent/tls.key",
	}

	set := extensiontest.NewNopSettings(Type)
	ext := newHTTPSMetrics(set, cfg)

	err := ext.Start(context.Background(), componenttest.NewNopHost())
	if err == nil {
		_ = ext.Shutdown(context.Background())
		t.Fatal("expected error for missing cert file")
	}
}

// generateSelfSignedCert creates a temporary self-signed cert+key for testing.
func generateSelfSignedCert(t *testing.T) (certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"test"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	certFile := filepath.Join(t.TempDir(), "tls.crt")
	keyFile := filepath.Join(t.TempDir(), "tls.key")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if err := os.WriteFile(certFile, certPEM, 0600); err != nil {
		t.Fatal(err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyFile, keyPEM, 0600); err != nil {
		t.Fatal(err)
	}

	return certFile, keyFile
}

// getListenerAddr extracts the actual listener address when port 0 is used.
func getListenerAddr(ext *httpsMetrics) (string, bool) {
	if ext.listener != nil {
		return ext.listener.Addr().String(), true
	}
	return "", false
}
