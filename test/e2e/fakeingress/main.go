// Command fakeingress is a self-contained fake of the Red Hat Dataverse
// ingress upload API (console.redhat.com/api/ingress/v1/upload), designed
// to run inside a pod for cluster-level e2e tests of the dataverse
// exporter.
//
// It serves TLS on :8080 with a self-signed ECDSA certificate generated at
// startup and writes the CA PEM to <dir>/ca.pem so the test harness can
// mount it for the collector's tls.ca_file. Every received upload is
// appended as one JSON line to <dir>/requests.log. The response for each
// upload is controlled per request by the contents of <dir>/mode:
//
//	202  → HTTP 202 with {"request_id":"fake-<n>"} (default)
//	401  → HTTP 401
//	403  → HTTP 403
//	500  → HTTP 500
//	hang → sleep 10 minutes before responding (status logged as 0)
//
// Any other mode value produces HTTP 500. All paths are overridable via
// flags so unit tests can run against t.TempDir().
package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"
)

const (
	uploadPath = "/api/ingress/v1/upload"
	healthPath = "/healthz"

	// maxUploadBody bounds how much of a request body we buffer; the real
	// ingress caps payloads at 100 MiB, so allow headroom above that.
	maxUploadBody = 112 << 20

	// hangDuration is how long mode "hang" stalls before answering. It
	// exceeds the collector's HTTP client timeout so the request fails
	// client-side, exercising retry, while keeping the fake's log entry
	// written at receipt time.
	hangDuration = 10 * time.Minute
)

// recordedRequest is one JSON line of requests.log.
type recordedRequest struct {
	TS          string `json:"ts"`
	Mode        string `json:"mode"`
	Status      int    `json:"status"`
	HasBearer   bool   `json:"has_bearer"`
	UserAgent   string `json:"user_agent"`
	ContentType string `json:"content_type"`
	BodyB64     string `json:"body_b64"`
}

// server serves the fake ingress API from a data directory.
type server struct {
	dir     string
	counter atomic.Uint64
}

func newServer(dir string) *server {
	return &server{dir: dir}
}

func (s *server) modePath() string { return filepath.Join(s.dir, "mode") }
func (s *server) logPath() string  { return filepath.Join(s.dir, "requests.log") }

// readMode returns the currently selected response mode.
func (s *server) readMode() string {
	b, err := os.ReadFile(s.modePath())
	if err != nil || strings.TrimSpace(string(b)) == "" {
		return "202"
	}
	return strings.TrimSpace(string(b))
}

// statusForMode maps a mode to its HTTP status. "hang" is special-cased
// by the handler.
func statusForMode(mode string) (int, bool) {
	switch mode {
	case "202":
		return http.StatusAccepted, false
	case "401":
		return http.StatusUnauthorized, false
	case "403":
		return http.StatusForbidden, false
	case "500":
		return http.StatusInternalServerError, false
	case "hang":
		return 0, true
	default:
		return http.StatusInternalServerError, false
	}
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case healthPath:
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
		return
	case uploadPath:
	default:
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxUploadBody))
	if err != nil {
		http.Error(w, "reading body: "+err.Error(), http.StatusBadRequest)
		return
	}

	mode := s.readMode()
	status, hang := statusForMode(mode)

	// Record the request at receipt. For hang mode the status is logged
	// as 0 (pending) because the response is delayed past any test's
	// patience.
	token, bearer := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	rec := recordedRequest{
		TS:          time.Now().UTC().Format(time.RFC3339Nano),
		Mode:        mode,
		Status:      status,
		HasBearer:   bearer && strings.TrimSpace(token) != "",
		UserAgent:   r.Header.Get("User-Agent"),
		ContentType: r.Header.Get("Content-Type"),
		BodyB64:     base64.StdEncoding.EncodeToString(body),
	}
	line, err := json.Marshal(rec)
	if err != nil {
		http.Error(w, "encoding request record", http.StatusInternalServerError)
		return
	}
	s.appendRecord(line)

	if hang {
		time.Sleep(hangDuration)
		http.Error(w, "hang mode: intentional stall", http.StatusServiceUnavailable)
		return
	}
	if status == http.StatusAccepted {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = fmt.Fprintf(w, `{"request_id":"fake-%d"}`, s.counter.Add(1))
		return
	}
	http.Error(w, "mode "+mode, status)
}

// appendRecord appends one JSON line to requests.log.
func (s *server) appendRecord(line []byte) {
	f, err := os.OpenFile(s.logPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Printf("fakeingress: opening %s: %v", s.logPath(), err)
		return
	}
	defer f.Close()
	if _, err := f.Write(append(line, '\n')); err != nil {
		log.Printf("fakeingress: writing %s: %v", s.logPath(), err)
	}
}

// generateSelfSignedCert creates a self-signed ECDSA P-256 certificate
// valid for the fake ingress service names in the given namespace and for
// localhost, plus the PEM encoding to use as the collector's tls.ca_file.
func generateSelfSignedCert(namespace string) (tls.Certificate, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("generating key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("generating serial: %w", err)
	}

	template := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "fake-ingress-e2e"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		DNSNames:              []string{"localhost", "fake-ingress"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	if namespace != "" {
		template.DNSNames = append(template.DNSNames,
			"fake-ingress."+namespace+".svc",
			"fake-ingress."+namespace+".svc.cluster.local",
		)
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("creating certificate: %w", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("marshaling key: %w", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(caPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("building key pair: %w", err)
	}
	return cert, caPEM, nil
}

// run starts the fake ingress server: prepares the data directory,
// generates the TLS certificate, writes ca.pem and (after the listener is
// bound) the ready marker, then serves until the process exits.
func run(addr, dir, namespaceFile string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating data dir %s: %w", dir, err)
	}

	namespace := ""
	if b, err := os.ReadFile(namespaceFile); err == nil {
		namespace = strings.TrimSpace(string(b))
	}

	cert, caPEM, err := generateSelfSignedCert(namespace)
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "ca.pem"), caPEM, 0o644); err != nil {
		return fmt.Errorf("writing ca.pem: %w", err)
	}

	listener, err := tls.Listen("tcp", addr, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		return fmt.Errorf("listening on %s: %w", addr, err)
	}
	// Signal readiness only after the listener is bound.
	if err := os.WriteFile(filepath.Join(dir, "ready"), []byte("ok\n"), 0o644); err != nil {
		return fmt.Errorf("writing ready marker: %w", err)
	}

	srv := &http.Server{Handler: newServer(dir)}
	log.Printf("fakeingress: serving TLS on %s (dir %s, namespace %q)", addr, dir, namespace)
	return srv.Serve(listener)
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	dir := flag.String("dir", "/tmp/fakeingress", "data directory (mode, ca.pem, requests.log, ready)")
	namespaceFile := flag.String("namespace-file",
		"/var/run/secrets/kubernetes.io/serviceaccount/namespace",
		"file holding the pod namespace, used for TLS SANs; empty disables service SANs")
	flag.Parse()
	if err := run(*addr, *dir, *namespaceFile); err != nil {
		log.Fatalf("fakeingress: %v", err)
	}
}
