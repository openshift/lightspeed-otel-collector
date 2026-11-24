package dataverseexporter

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	inClusterTokenFile  = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	inClusterCAFile     = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	cloudAuthKey        = "cloud.openshift.com"
	pullSecretNamespace = "openshift-config"
	pullSecretName      = "pull-secret"
)

// clusterAuth resolves the upload bearer token (cloud.openshift.com entry
// of the cluster pull-secret) and the cluster identity (ClusterVersion
// spec.clusterID) over the in-cluster Kubernetes API, using the pod's
// projected ServiceAccount credentials. Values are cached; refresh forces a
// re-read so secret rotation self-heals after an ingress 401.
type clusterAuth struct {
	apiURL    string
	tokenFile string
	client    *http.Client

	// Separate mutexes so a hung pull-secret call cannot block a
	// ClusterID call (and vice versa). Each is still held across its
	// HTTP call to dedup concurrent refreshes of that value.
	tokenMu     sync.Mutex
	uploadToken string

	// lastTokenRead records the time of the most recent pull-secret read
	// ATTEMPT (success or failure); refreshInterval rate-limits how often
	// a 401 may trigger a fresh read. lastTokenRead is guarded by tokenMu.
	lastTokenRead   time.Time
	refreshInterval time.Duration

	idMu      sync.Mutex
	clusterID string
}

// errCredentialShape marks pull-secret errors that are problems with the
// secret's CONTENT (missing cloud.openshift.com entry, malformed
// .dockerconfigjson) rather than its AVAILABILITY. Shape errors are
// permanent: retrying an unparseable secret cannot help. API-server
// (availability) errors are NOT of this type and stay retryable.
type errCredentialShape struct {
	msg string
}

func (e *errCredentialShape) Error() string { return e.msg }

// inClusterAPIURL builds the HTTPS URL from Kubernetes service environment variables.
func inClusterAPIURL() (string, error) {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	port := os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return "", fmt.Errorf("not running in a Kubernetes cluster: KUBERNETES_SERVICE_HOST/PORT unset")
	}
	return "https://" + net.JoinHostPort(host, port), nil
}

func newClusterAuth() (*clusterAuth, error) {
	apiURL, err := inClusterAPIURL()
	if err != nil {
		return nil, err
	}
	return newClusterAuthFromPaths(apiURL, inClusterTokenFile, inClusterCAFile)
}

// newClusterAuthFromPaths builds clusterAuth from an explicit API URL,
// token file, and CA file. newClusterAuth resolves these from the in-cluster
// environment; the split exists so tests can exercise the construction
// without real ServiceAccount paths.
func newClusterAuthFromPaths(apiURL, tokenFile, caFile string) (*clusterAuth, error) {
	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("reading in-cluster CA %s: %w", caFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parsing in-cluster CA %s", caFile)
	}
	transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}
	// Timeout guards callers that pass a context without a deadline;
	// otherwise a stuck read could wedge the auth path.
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}
	return &clusterAuth{
		apiURL:    apiURL,
		tokenFile: tokenFile,
		client:    client,
		// 401-triggered refreshes are rate-limited to at most one pull-secret
		// read per minute; tests construct clusterAuth directly and shorten it.
		refreshInterval: time.Minute,
	}, nil
}

func (a *clusterAuth) getJSON(ctx context.Context, path string, out any) error {
	token, err := os.ReadFile(a.tokenFile)
	if err != nil {
		return fmt.Errorf("reading service account token: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.apiURL+path, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(token)))
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("GET %s: status %d: %q", path, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// ShouldRefresh reports whether a 401-triggered token refresh is allowed:
// at most one pull-secret read attempt per refreshInterval. True when no
// read has happened yet (zero lastTokenRead).
func (a *clusterAuth) ShouldRefresh() bool {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	return time.Since(a.lastTokenRead) >= a.refreshInterval
}

// UploadToken returns the pull-secret cloud.openshift.com auth value.
// A missing entry is an error: the exporter cannot upload without it.
func (a *clusterAuth) UploadToken(ctx context.Context, refresh bool) (string, error) {
	a.tokenMu.Lock()
	defer a.tokenMu.Unlock()
	if a.uploadToken != "" && !refresh {
		return a.uploadToken, nil
	}
	// Measure the interval from the last API read, not the last cache hit.
	// Otherwise regular uploads could postpone a refresh indefinitely.
	a.lastTokenRead = time.Now()
	var secret struct {
		// encoding/json base64-decodes []byte fields automatically.
		Data map[string][]byte `json:"data"`
	}
	if err := a.getJSON(ctx,
		fmt.Sprintf("/api/v1/namespaces/%s/secrets/%s", pullSecretNamespace, pullSecretName),
		&secret); err != nil {
		if refresh {
			// Clear the stale value so the next call cannot silently
			// serve a token we know the API just rejected.
			a.uploadToken = ""
		}
		return "", fmt.Errorf("reading pull-secret: %w", err)
	}
	var dockerConfig struct {
		Auths map[string]struct {
			Auth string `json:"auth"`
		} `json:"auths"`
	}
	if err := json.Unmarshal(secret.Data[".dockerconfigjson"], &dockerConfig); err != nil {
		return "", &errCredentialShape{msg: fmt.Sprintf("parsing pull-secret dockerconfigjson: %v", err)}
	}
	entry, ok := dockerConfig.Auths[cloudAuthKey]
	if !ok || entry.Auth == "" {
		a.uploadToken = ""
		return "", &errCredentialShape{msg: fmt.Sprintf("pull-secret has no %s auth entry", cloudAuthKey)}
	}
	a.uploadToken = entry.Auth
	return a.uploadToken, nil
}

// ClusterID returns the ClusterVersion spec.clusterID.
func (a *clusterAuth) ClusterID(ctx context.Context, refresh bool) (string, error) {
	a.idMu.Lock()
	defer a.idMu.Unlock()
	if a.clusterID != "" && !refresh {
		return a.clusterID, nil
	}
	var cv struct {
		Spec struct {
			ClusterID string `json:"clusterID"`
		} `json:"spec"`
	}
	if err := a.getJSON(ctx, "/apis/config.openshift.io/v1/clusterversions/version", &cv); err != nil {
		return "", fmt.Errorf("reading clusterversion: %w", err)
	}
	id := strings.TrimSpace(cv.Spec.ClusterID)
	if id == "" {
		if refresh {
			a.clusterID = ""
		}
		return "", fmt.Errorf("clusterversion has empty spec.clusterID")
	}
	a.clusterID = id
	return a.clusterID, nil
}
