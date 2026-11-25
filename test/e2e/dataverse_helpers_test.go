//go:build e2e

package e2e

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Dataverse-exporter e2e support: a fake ingress service stood up inside
// the ephemeral namespace, cluster-scoped RBAC mirroring the operator's
// grants, and a second collector
// deployment terminating traces/data_collection in the dataverse exporter.
//
// Shared resources (fake ingress pod, shared collector deployment) are
// deployed lazily exactly once and live until the ephemeral namespace is
// torn down; per-test resources register their own cleanup.

const (
	fakeIngressPod     = "fake-ingress"
	fakeIngressPort    = 8080
	fakeIngressDir     = "/tmp/fakeingress"
	fakeIngressBin     = "/tmp/fakeingress/fakeingress"
	fakeIngressCACM    = "fake-ingress-ca"
	fakeIngressCAKey   = "tls-ca-bundle.pem"
	fakeIngressCAMount = "/etc/dataverse/ca"

	pullSecretNamespace = "openshift-config"
	pullSecretName      = "pull-secret"

	dataverseUploadPath = "/api/ingress/v1/upload"
	dataverseMemberRe   = `^agentic-v1/traces-.*\.json$`

	dataverseSharedDeployment = "otel-collector-dataverse"

	// dataverseNoUploadWait must exceed the shared deployment's
	// flush_timeout so a "no upload" assertion covers a full flush cycle.
	dataverseNoUploadWait = 60 * time.Second
)

// --- kubectl plumbing ---

// kubectlWithStdin runs kubectl with the given stdin and fails the test
// on error.
func kubectlWithStdin(t *testing.T, stdin io.Reader, args ...string) {
	t.Helper()
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = stdin
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl %s: %v\nstderr: %s", strings.Join(args, " "), err, stderr.String())
	}
}

// kubectlApplyNS applies a manifest, optionally namespace-scoped, failing
// the test on error.
func kubectlApplyNS(t *testing.T, ns, manifest string) {
	t.Helper()
	args := []string{"apply", "-f", "-"}
	if ns != "" {
		args = append(args, "-n", ns)
	}
	cmd := exec.Command("kubectl", args...)
	cmd.Stdin = strings.NewReader(manifest)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("kubectl apply: %v\nstderr: %s\nmanifest:\n%s", err, stderr.String(), manifest)
	}
}

// kubectlDeleteBestEffort deletes resources, ignoring errors.
func kubectlDeleteBestEffort(args ...string) {
	_, _ = runKubectl(append([]string{"delete", "--ignore-not-found=true", "--wait=false"}, args...)...)
}

// pollUntil polls cond every interval until it holds or the timeout
// elapses, failing the test with desc on timeout.
func pollUntil(t *testing.T, timeout, interval time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(interval)
	}
	if !cond() {
		t.Fatalf("timed out after %s waiting for %s", timeout, desc)
	}
}

// --- fake ingress binary ---

// fakeIngressTargetArch resolves the GOARCH to build the fake ingress
// binary for. The binary executes on CLUSTER NODES, which may differ
// from the test runner's platform (e.g. an arm64 laptop against an amd64
// cluster, or an amd64 Konflux runner against an arm64 cluster profile),
// so the runner's own architecture is never used as the target. The
// FAKEINGRESS_GOARCH override wins; otherwise the architecture is
// detected from the cluster's nodes.
func fakeIngressTargetArch(t *testing.T) string {
	t.Helper()
	if arch := strings.TrimSpace(os.Getenv("FAKEINGRESS_GOARCH")); arch != "" {
		return arch
	}
	out, err := runKubectl("get", "nodes",
		"-o", "jsonpath={.items[0].status.nodeInfo.architecture}")
	if err != nil {
		t.Fatalf("detecting the cluster node architecture for the fake ingress binary (set FAKEINGRESS_GOARCH to override): %v", err)
	}
	switch arch := strings.TrimSpace(out); arch {
	case "amd64", "arm64", "arm", "s390x", "ppc64le", "riscv64":
		return arch
	case "":
		t.Fatalf("the cluster reported an empty node architecture; set FAKEINGRESS_GOARCH to override")
	default:
		t.Fatalf("cluster node architecture %q has no Go GOARCH mapping; set FAKEINGRESS_GOARCH to override", arch)
	}
	return "" // unreachable
}

// buildFakeIngressBinary cross-compiles the fake ingress server for the
// cluster's nodes (see fakeIngressTargetArch). GOOS is always linux.
func buildFakeIngressBinary(t *testing.T) string {
	t.Helper()
	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skipf("go toolchain not available to build the fake ingress binary: %v", err)
	}
	arch := fakeIngressTargetArch(t)
	binPath := filepath.Join(t.TempDir(), "fakeingress")
	cmd := exec.Command(goBin, "build", "-o", binPath, "./fakeingress")
	// Test binaries run with the package directory as cwd.
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+arch, "CGO_ENABLED=0")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("building fake ingress binary (GOARCH=%s): %v\nstderr: %s", arch, err, stderr.String())
	}
	return binPath
}

// --- shared fake ingress deployment ---

var fakeIngressState struct {
	mu     sync.Mutex
	ready  bool
	failed error
	caPEM  string
}

// ensureFakeIngress deploys the fake ingress pod exactly once per run and
// returns its CA PEM. Later callers reuse it; if an earlier attempt
// failed or skipped, they skip with the recorded cause. The pod and its
// service live until the ephemeral namespace is deleted (deliberately no
// per-test cleanup: it is shared).
func ensureFakeIngress(t *testing.T) string {
	t.Helper()
	fakeIngressState.mu.Lock()
	defer fakeIngressState.mu.Unlock()
	if fakeIngressState.failed != nil {
		t.Skipf("fake ingress unavailable (earlier setup failed): %v", fakeIngressState.failed)
	}
	if fakeIngressState.ready {
		return fakeIngressState.caPEM
	}
	// Pessimistic marker: if deployment exits via t.Skip/t.Fatal, later
	// callers see a cause instead of retrying.
	fakeIngressState.failed = errors.New("fake ingress deployment skipped or failed — see the first dataverse test's output for the cause")
	caPEM := deployFakeIngress(t)
	fakeIngressState.caPEM = caPEM
	fakeIngressState.ready = true
	fakeIngressState.failed = nil
	return caPEM
}

// deployFakeIngress runs the fake ingress binary inside a pod of the
// collector image (hermetic: no new image dependency) and returns the
// server's CA PEM. Requires a shell in the image; skips (never fails)
// when the image cannot run sh.
func deployFakeIngress(t *testing.T) string {
	t.Helper()
	binary := buildFakeIngressBinary(t)

	_, err := runKubectl("run", fakeIngressPod, "-n", env.Namespace,
		"--image", getCollectorImage(), "--restart", "Never",
		"--labels", "app="+fakeIngressPod,
		"--command", "--", "sh", "-c", "sleep infinity")
	if err != nil {
		t.Skipf("cannot run the fake ingress pod from the collector image: %v", err)
	}

	if _, err := runKubectl("wait", "-n", env.Namespace, "--for=condition=Ready",
		"pod/"+fakeIngressPod, "--timeout="+pollTimeout.String()); err != nil {
		podState, _ := runKubectl("get", "pod", fakeIngressPod, "-n", env.Namespace,
			"-o", "jsonpath={.status.phase}/{.status.containerStatuses[0].state.waiting.reason}")
		t.Skipf("fake ingress pod never became Ready (%v; pod state %q) — "+
			"the collector image likely has no shell, which the fake ingress requires; "+
			"dataverse e2e tests need a shell-enabled image", err, podState)
	}

	// Probe sh explicitly before relying on it.
	if out, err := runKubectl("exec", "-n", env.Namespace, fakeIngressPod,
		"--", "sh", "-c", "echo ok"); err != nil || strings.TrimSpace(out) != "ok" {
		t.Skipf("cannot exec sh in pod %s (%v, output %q) — the dataverse e2e tests require a shell in the collector image",
			fakeIngressPod, err, out)
	}

	// Pipe the binary into the pod.
	bin, err := os.Open(binary)
	if err != nil {
		t.Fatalf("open built fake ingress binary: %v", err)
	}
	defer bin.Close()
	kubectlWithStdin(t, bin, "exec", "-i", "-n", env.Namespace, fakeIngressPod,
		"--", "sh", "-c",
		fmt.Sprintf("mkdir -p %s && cat > %s && chmod +x %s", fakeIngressDir, fakeIngressBin, fakeIngressBin))

	// Start the server detached inside the pod.
	kubectl(t, "exec", "-n", env.Namespace, fakeIngressPod, "--", "sh", "-c",
		fmt.Sprintf("%s -addr :%d -dir %s >%s/server.log 2>&1 &",
			fakeIngressBin, fakeIngressPort, fakeIngressDir, fakeIngressDir))

	// Service + CA ConfigMap (mounted by the dataverse collector
	// deployments). Applied only once — later calls are no-ops
	// (idempotent apply).
	var caPEM string
	pollUntil(t, pollTimeout, 2*time.Second, "fake ingress ready marker", func() bool {
		caPEM = kubectlQuiet(t, "exec", "-n", env.Namespace, fakeIngressPod,
			"--", "cat", fakeIngressDir+"/ca.pem")
		ready, _ := runKubectl("exec", "-n", env.Namespace, fakeIngressPod,
			"--", "sh", "-c", "test -f "+fakeIngressDir+"/ready && echo ready")
		return strings.TrimSpace(ready) == "ready" && caPEM != ""
	})

	kubectlApplyNS(t, env.Namespace, fmt.Sprintf(`---
apiVersion: v1
kind: ConfigMap
metadata:
  name: %s
data:
  %s: |
%s
---
apiVersion: v1
kind: Service
metadata:
  name: %s
spec:
  selector:
    app: %s
  ports:
  - port: %d
    targetPort: %d
`, fakeIngressCACM, fakeIngressCAKey, indentYAMLBlock(caPEM, 4),
		fakeIngressPod, fakeIngressPod, fakeIngressPort, fakeIngressPort))

	return caPEM
}

// kubectlQuiet runs kubectl and returns stdout ("" on error) without
// failing the test.
func kubectlQuiet(t *testing.T, args ...string) string {
	t.Helper()
	out, err := runKubectl(args...)
	if err != nil {
		return ""
	}
	return out
}

// indentYAMLBlock indents every line of s for embedding as a YAML block
// scalar.
func indentYAMLBlock(s string, indent int) string {
	pad := strings.Repeat(" ", indent)
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i, line := range lines {
		lines[i] = pad + line
	}
	return strings.Join(lines, "\n")
}

// fakeIngressSetMode switches the fake ingress response mode.
func fakeIngressSetMode(t *testing.T, mode string) {
	t.Helper()
	kubectlWithStdin(t, strings.NewReader(mode),
		"exec", "-i", "-n", env.Namespace, fakeIngressPod, "--",
		"sh", "-c", "cat > "+fakeIngressDir+"/mode")
}

// recordedRequest is one parsed line of the fake ingress requests.log.
type recordedRequest struct {
	TS          string
	Mode        string
	Status      int
	HasBearer   bool
	UserAgent   string
	ContentType string
	Body        []byte // decoded from body_b64
}

// fakeIngressRequests returns all requests recorded so far.
func fakeIngressRequests(t *testing.T) []recordedRequest {
	t.Helper()
	out, err := runKubectl("exec", "-n", env.Namespace, fakeIngressPod,
		"--", "cat", fakeIngressDir+"/requests.log")
	if err != nil {
		// The log does not exist until the first request is recorded.
		// Tolerate both the "No such file" message and a bare non-zero
		// exit with no recorded lines, so a differently-wrapped error
		// during the first poll window reads as "no requests" instead
		// of failing the test.
		if strings.Contains(err.Error(), "No such file") ||
			strings.Contains(err.Error(), "exited with code 1") {
			return nil
		}
		t.Fatalf("read fake ingress requests.log: %v", err)
	}
	var requests []recordedRequest
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var rec struct {
			TS          string `json:"ts"`
			Mode        string `json:"mode"`
			Status      int    `json:"status"`
			HasBearer   bool   `json:"has_bearer"`
			UserAgent   string `json:"user_agent"`
			ContentType string `json:"content_type"`
			BodyB64     string `json:"body_b64"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("parse fake ingress request line %q: %v", line, err)
		}
		body, err := base64.StdEncoding.DecodeString(rec.BodyB64)
		if err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		requests = append(requests, recordedRequest{
			TS: rec.TS, Mode: rec.Mode, Status: rec.Status,
			HasBearer: rec.HasBearer, UserAgent: rec.UserAgent,
			ContentType: rec.ContentType, Body: body,
		})
	}
	return requests
}

// --- cluster-scoped RBAC (operator-mirroring) ---

var dataverseRBAC struct {
	mu   sync.Mutex
	refs int
}

// ensureDataverseRBAC grants the test namespace's default ServiceAccount
// read access to openshift-config/pull-secret and to the
// clusterversions/version resource, mirroring the operator's grants. The
// objects are reference-counted across tests: they are created on the
// first reference and deleted when the last referencing test finishes.
func ensureDataverseRBAC(t *testing.T) {
	t.Helper()
	dataverseRBAC.mu.Lock()
	defer dataverseRBAC.mu.Unlock()
	if dataverseRBAC.refs == 0 {
		name := "e2e-dv-" + env.Namespace
		kubectlApplyNS(t, "", fmt.Sprintf(`---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: %s-pull-secret
rules:
- apiGroups: [""]
  resources: ["secrets"]
  resourceNames: ["%s"]
  verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %s-pull-secret
  namespace: %s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: %s-pull-secret
subjects:
- kind: ServiceAccount
  name: default
  namespace: %s
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: %s-cluster-version
rules:
- apiGroups: ["config.openshift.io"]
  resources: ["clusterversions"]
  resourceNames: ["version"]
  verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: %s-cluster-version
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: %s-cluster-version
subjects:
- kind: ServiceAccount
  name: default
  namespace: %s
`, name, pullSecretName, name, pullSecretNamespace, name, env.Namespace,
			name, name, name, env.Namespace))
	}
	dataverseRBAC.refs++
	t.Cleanup(func() {
		dataverseRBAC.mu.Lock()
		defer dataverseRBAC.mu.Unlock()
		dataverseRBAC.refs--
		if dataverseRBAC.refs == 0 {
			name := "e2e-dv-" + env.Namespace
			kubectlDeleteBestEffort("clusterrole", name+"-pull-secret")
			kubectlDeleteBestEffort("rolebinding", name+"-pull-secret", "-n", pullSecretNamespace)
			kubectlDeleteBestEffort("clusterrole", name+"-cluster-version")
			kubectlDeleteBestEffort("clusterrolebinding", name+"-cluster-version")
		}
	})
}

// revokeDataversePullSecretRBAC deletes (best-effort) the RoleBinding
// that grants the test namespace's default ServiceAccount pull-secret
// read access, making the API server return 403 for credential fetches.
func revokeDataversePullSecretRBAC(t *testing.T) {
	t.Helper()
	name := "e2e-dv-" + env.Namespace
	kubectlDeleteBestEffort("rolebinding", name+"-pull-secret", "-n", pullSecretNamespace)
}

// grantDataversePullSecretRBAC (re-)applies the pull-secret RoleBinding.
func grantDataversePullSecretRBAC(t *testing.T) {
	t.Helper()
	name := "e2e-dv-" + env.Namespace
	kubectlApplyNS(t, "", fmt.Sprintf(`---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %s-pull-secret
  namespace: %s
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: %s-pull-secret
subjects:
- kind: ServiceAccount
  name: default
  namespace: %s
`, name, pullSecretNamespace, name, env.Namespace))
}

// requireCloudCredential checks the E2E prerequisite without changing the cluster secret.
func requireCloudCredential(t *testing.T) {
	t.Helper()
	out, err := runKubectl("get", "secret", pullSecretName, "-n", pullSecretNamespace, "-o", `go-template={{index .data ".dockerconfigjson"}}`)
	if err != nil {
		t.Skip("cluster pull-secret is unavailable")
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(out))
	var cfg struct {
		Auths map[string]struct{ Auth string }
	}
	if err != nil || json.Unmarshal(raw, &cfg) != nil || cfg.Auths["cloud.openshift.com"].Auth == "" {
		t.Skip("test requires an existing cloud.openshift.com credential")
	}
}

// getClusterID returns the cluster's real ClusterVersion clusterID.
func getClusterID(t *testing.T) string {
	t.Helper()
	out, err := runKubectl("get", "clusterversion", "version", "-o", "jsonpath={.spec.clusterID}")
	if err != nil || strings.TrimSpace(out) == "" {
		t.Skipf("cannot read clusterversion version spec.clusterID (%v, %q) — skipping", err, out)
	}
	return strings.TrimSpace(out)
}

// --- dataverse collector deployment ---

// dataverseCollectorConfig controls the dataverse exporter's queue,
// batch, and retry settings for a test deployment.
type dataverseCollectorConfig struct {
	name         string
	queueSize    int64
	minSize      int64
	maxSize      int64
	flushTimeout time.Duration
	retryInitial time.Duration
	retryMax     time.Duration
	retryElapsed time.Duration
	// shared deployments skip per-test cleanup: they live until the
	// ephemeral namespace is deleted.
	shared bool
}

func defaultDataverseCollectorConfig(name string) dataverseCollectorConfig {
	// Standard shape: quiet-cluster flush at 30s (min_size unreachable
	// for test-sized batches), 4 MiB batches, large queue, fast retries.
	return dataverseCollectorConfig{
		name:         name,
		queueSize:    128 << 20,
		minSize:      4 << 20,
		maxSize:      4 << 20,
		flushTimeout: 30 * time.Second,
		retryInitial: 2 * time.Second,
		retryMax:     5 * time.Second,
		retryElapsed: 10 * time.Minute,
	}
}

// dataverseCollector is a deployed collector whose traces/data_collection
// pipeline terminates in the dataverse exporter.
type dataverseCollector struct {
	name       string
	otlpAddr   string
	healthAddr string
	pfOTLP     *portForward
	pfHealth   *portForward
}

func (c *dataverseCollector) podName(t *testing.T) string {
	t.Helper()
	return kubectl(t, "get", "pod", "-n", env.Namespace, "-l", "app="+c.name,
		"-o", "jsonpath={.items[0].metadata.name}")
}

func (c *dataverseCollector) podStartTime(t *testing.T) string {
	t.Helper()
	return kubectl(t, "get", "pod", "-n", env.Namespace, "-l", "app="+c.name,
		"-o", "jsonpath={.items[0].status.startTime}")
}

func (c *dataverseCollector) podRestartCount(t *testing.T) string {
	t.Helper()
	return kubectl(t, "get", "pod", "-n", env.Namespace, "-l", "app="+c.name,
		"-o", "jsonpath={.items[0].status.containerStatuses[0].restartCount}")
}

func (c *dataverseCollector) logsSince(t *testing.T, since time.Time) string {
	t.Helper()
	return kubectl(t, "logs", "-n", env.Namespace, c.podName(t),
		"--since-time="+since.UTC().Format(time.RFC3339Nano))
}

func renderDataverseCollectorConfig(cfg dataverseCollectorConfig, endpoint string) string {
	return fmt.Sprintf(`receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317

connectors:
  routing/data_collection:
    table:
      - context: resource
        condition: attributes["service.name"] == "lightspeed-agentic-operator" or attributes["service.name"] == "lightspeed-agentic-sandbox"
        pipelines: [traces/data_collection]

exporters:
  dataverse:
    endpoint: %s
    service_id: ols
    archive_path_prefix: agentic-v1/
    tls:
      ca_file: %s
    sending_queue:
      sizer: bytes
      queue_size: %d
      num_consumers: 1
      batch:
        sizer: bytes
        min_size: %d
        max_size: %d
        flush_timeout: %s
    retry_on_failure:
      enabled: true
      initial_interval: %s
      max_interval: %s
      max_elapsed_time: %s
  debug:
    verbosity: basic

extensions:
  health_check:
    endpoint: 0.0.0.0:13133

service:
  extensions: [health_check]
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [routing/data_collection]
    traces/data_collection:
      receivers: [routing/data_collection]
      exporters: [dataverse]
`,
		endpoint,
		fakeIngressCAMount+"/"+fakeIngressCAKey,
		cfg.queueSize, cfg.minSize, cfg.maxSize, cfg.flushTimeout,
		cfg.retryInitial, cfg.retryMax, cfg.retryElapsed)
}

func renderDataverseCollectorManifest(cfg dataverseCollectorConfig, config string) string {
	indented := ""
	for _, line := range strings.Split(config, "\n") {
		indented += "    " + line + "\n"
	}
	return fmt.Sprintf(`---
apiVersion: v1
kind: ConfigMap
metadata:
  name: %s-config
data:
  config.yaml: |
%s
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: %s
  template:
    metadata:
      labels:
        app: %s
    spec:
      containers:
      - name: collector
        image: %s
        imagePullPolicy: Always
        args: ["--config=/etc/otelcol/config.yaml"]
        ports:
        - containerPort: 4317
          name: otlp-grpc
        - containerPort: 13133
          name: health
        volumeMounts:
        - name: config
          mountPath: /etc/otelcol
          readOnly: true
        - name: dataverse-ca
          mountPath: %s
          readOnly: true
        readinessProbe:
          httpGet:
            path: /
            port: 13133
          initialDelaySeconds: 5
          periodSeconds: 5
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "256Mi"
      volumes:
      - name: config
        configMap:
          name: %s-config
      - name: dataverse-ca
        configMap:
          name: %s
---
apiVersion: v1
kind: Service
metadata:
  name: %s
spec:
  selector:
    app: %s
  ports:
  - port: 4317
    targetPort: 4317
    name: otlp-grpc
  - port: 13133
    targetPort: 13133
    name: health
`,
		cfg.name, indented, cfg.name, cfg.name, cfg.name, getCollectorImage(),
		fakeIngressCAMount, cfg.name, fakeIngressCACM, cfg.name, cfg.name)
}

// deployDataverseCollector deploys a collector whose
// traces/data_collection pipeline terminates in the dataverse exporter
// pointed at the fake ingress, waits for rollout and health, and sets up
// port-forwards. Non-shared deployments are deleted on test cleanup.
func deployDataverseCollector(t *testing.T, tweaks ...func(*dataverseCollectorConfig)) *dataverseCollector {
	t.Helper()
	// The fake ingress (and its CA ConfigMap) must exist first.
	ensureFakeIngress(t)

	cfg := defaultDataverseCollectorConfig("otel-collector-dv-" + fmt.Sprint(time.Now().UnixMilli()%100000))
	for _, tweak := range tweaks {
		tweak(&cfg)
	}

	endpoint := fmt.Sprintf("https://%s.%s.svc:%d%s",
		fakeIngressPod, env.Namespace, fakeIngressPort, dataverseUploadPath)
	kubectlApplyNS(t, env.Namespace, renderDataverseCollectorManifest(cfg,
		renderDataverseCollectorConfig(cfg, endpoint)))
	waitForRollout(t, env.Namespace, "deployment", cfg.name)

	clientset := newK8sClientset()
	pfOTLP := newPortForward(clientset, env.Namespace, cfg.name, 4317)
	pfHealth := newPortForward(clientset, env.Namespace, cfg.name, 13133)
	if err := waitHealthy(pfHealth.Addr(), pollTimeout); err != nil {
		t.Fatalf("dataverse collector %s health check: %v", cfg.name, err)
	}

	collector := &dataverseCollector{
		name:       cfg.name,
		otlpAddr:   pfOTLP.Addr(),
		healthAddr: pfHealth.Addr(),
		pfOTLP:     pfOTLP,
		pfHealth:   pfHealth,
	}

	if !cfg.shared {
		t.Cleanup(func() {
			pfOTLP.Stop()
			pfHealth.Stop()
			kubectlDeleteBestEffort("deployment", cfg.name, "-n", env.Namespace)
			kubectlDeleteBestEffort("svc", cfg.name, "-n", env.Namespace)
			kubectlDeleteBestEffort("configmap", cfg.name+"-config", "-n", env.Namespace)
		})
	}
	return collector
}

// --- shared dataverse collector (lazy, once per run) ---

var sharedDataverseState struct {
	mu        sync.Mutex
	ready     bool
	failed    error
	collector *dataverseCollector
}

// sharedDataverseCollector returns the shared standard-config dataverse
// collector, deploying it lazily on first use. Per-test cluster
// prerequisites (RBAC, existing cloud credential) are ensured on every call; the
// deployment itself happens exactly once and lives until namespace
// teardown.
func sharedDataverseCollector(t *testing.T) *dataverseCollector {
	t.Helper()
	ensureDataverseRBAC(t)
	requireCloudCredential(t)

	sharedDataverseState.mu.Lock()
	defer sharedDataverseState.mu.Unlock()
	if sharedDataverseState.failed != nil {
		t.Skipf("shared dataverse collector unavailable (earlier setup failed): %v", sharedDataverseState.failed)
	}
	if sharedDataverseState.ready {
		return sharedDataverseState.collector
	}
	sharedDataverseState.failed = errors.New("shared dataverse deployment skipped or failed — see the first dataverse test's output for the cause")
	collector := deployDataverseCollector(t, func(cfg *dataverseCollectorConfig) {
		cfg.name = dataverseSharedDeployment
		cfg.shared = true
	})
	sharedDataverseState.collector = collector
	sharedDataverseState.ready = true
	sharedDataverseState.failed = nil
	return collector
}

// regexpMatchString is a tiny wrapper kept separate so the matcher can
// be swapped in tests if needed.
func regexpMatchString(pattern, s string) bool {
	matched, err := regexp.MatchString(pattern, s)
	return err == nil && matched
}

// --- OTLP trace sending ---

// sendDataverseTraceBatchErr sends one small OTLP trace batch with the
// given service.name and a unique span marker, returning any gRPC error
// so callers can assert producer-side success.
func sendDataverseTraceBatchErr(t *testing.T, addr, serviceName, marker string, payloadBytes int) error {
	t.Helper()
	seed := markerSeed(marker)
	payload := marker
	if payloadBytes > len(marker) {
		payload += strings.Repeat("x", payloadBytes-len(marker))
	}
	start := uint64(time.Now().UnixNano())
	request := &collectortracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
				stringAttribute("service.name", serviceName),
				stringAttribute(requestMarkerAttribute, marker),
			}},
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: &commonpb.InstrumentationScope{Name: "dataverse-e2e", Version: "1.0"},
				Spans: []*tracepb.Span{{
					TraceId:           traceID(seed),
					SpanId:            spanID(seed),
					Name:              marker,
					StartTimeUnixNano: start,
					EndTimeUnixNano:   start + uint64(time.Millisecond),
					Attributes: []*commonpb.KeyValue{
						stringAttribute(requestMarkerAttribute, marker),
						stringAttribute("e2e.payload", payload),
					},
				}},
			}},
		}},
	}

	connection, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial collector at %s: %v", addr, err)
	}
	defer func() { _ = connection.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err = collectortracepb.NewTraceServiceClient(connection).Export(ctx, request)
	return err
}

// sendDataverseTraceBatch sends one OTLP trace batch, failing the test if
// the collector rejects it (the producer-side success assertion).
func sendDataverseTraceBatch(t *testing.T, addr, serviceName, marker string) {
	t.Helper()
	if err := sendDataverseTraceBatchErr(t, addr, serviceName, marker, 0); err != nil {
		t.Fatalf("OTLP export of trace batch %q (service %s) failed: %v", marker, serviceName, err)
	}
}

// markerSeed derives a deterministic span-ID seed from a marker.
func markerSeed(marker string) byte {
	var seed byte
	for _, r := range marker {
		seed = seed*31 + byte(r)
	}
	if seed == 0 {
		seed = 1
	}
	return seed
}

// --- upload assertions ---

// parsedUpload is the decoded multipart/TAR content of one recorded
// upload request.
type parsedUpload struct {
	formName        string
	filename        string
	partContentType string
	memberName      string
	memberContent   string
}

// parseDataverseUpload decodes the recorded request body: the multipart
// framing, the single gzip TAR member, and its JSON content.
func parseDataverseUpload(t *testing.T, req recordedRequest) parsedUpload {
	t.Helper()
	mediaType, params, err := mime.ParseMediaType(req.ContentType)
	if err != nil {
		t.Fatalf("parse recorded Content-Type %q: %v", req.ContentType, err)
	}
	if mediaType != "multipart/form-data" {
		t.Fatalf("recorded Content-Type = %q, want multipart/form-data", mediaType)
	}
	mr := multipart.NewReader(bytes.NewReader(req.Body), params["boundary"])
	part, err := mr.NextPart()
	if err != nil {
		t.Fatalf("read first multipart part: %v", err)
	}
	parsed := parsedUpload{
		formName:        part.FormName(),
		filename:        part.FileName(),
		partContentType: part.Header.Get("Content-Type"),
	}
	body, err := io.ReadAll(part)
	if err != nil {
		t.Fatalf("read multipart part body: %v", err)
	}
	if _, err := mr.NextPart(); !errors.Is(err, io.EOF) {
		t.Fatalf("upload multipart body has more than one part (next part error: %v); want exactly one", err)
	}

	gz, err := gzip.NewReader(bytes.NewReader(body))
	if err != nil {
		t.Fatalf("gzip-decode upload body: %v", err)
	}
	tr := tar.NewReader(gz)
	header, err := tr.Next()
	if err != nil {
		t.Fatalf("read first TAR member: %v", err)
	}
	parsed.memberName = header.Name
	content, err := io.ReadAll(tr)
	if err != nil {
		t.Fatalf("read TAR member content: %v", err)
	}
	parsed.memberContent = string(content)
	if _, err := tr.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("upload TAR has more than one member (next member error: %v); want exactly one", err)
	}
	return parsed
}

// dataverseRequestsForMarker returns recorded requests (any status)
// whose TAR member content contains the span marker.
func dataverseRequestsForMarker(t *testing.T, marker string) []recordedRequest {
	t.Helper()
	var matches []recordedRequest
	for _, req := range fakeIngressRequests(t) {
		parsed := parseDataverseUpload(t, req)
		if strings.Contains(parsed.memberContent, marker) {
			matches = append(matches, req)
		}
	}
	return matches
}

// assertDataverseUpload asserts the full wire shape of one recorded
// upload: headers, multipart framing, TAR member name, JSON array
// content, span marker, cluster ID, and bearer presence (never its value).
func assertDataverseUpload(t *testing.T, req recordedRequest, clusterID, marker string) {
	t.Helper()
	if req.Status != http.StatusAccepted {
		t.Errorf("upload response status = %d, want 202", req.Status)
	}
	if !req.HasBearer {
		t.Error("missing bearer credential")
	}
	if !strings.Contains(req.UserAgent, "cluster/"+clusterID) {
		t.Errorf("User-Agent = %q, want it to contain cluster/%s", req.UserAgent, clusterID)
	}

	parsed := parseDataverseUpload(t, req)
	if parsed.formName != "file" {
		t.Errorf("multipart field name = %q, want %q", parsed.formName, "file")
	}
	if parsed.filename != "lightspeed-assistant.tgz" {
		t.Errorf("multipart filename = %q, want %q", parsed.filename, "lightspeed-assistant.tgz")
	}
	if parsed.partContentType != "application/vnd.redhat.ols.periodic+tar" {
		t.Errorf("multipart part Content-Type = %q, want %q",
			parsed.partContentType, "application/vnd.redhat.ols.periodic+tar")
	}
	if !regexpMatchString(dataverseMemberRe, parsed.memberName) {
		t.Errorf("TAR member name = %q, want match %s", parsed.memberName, dataverseMemberRe)
	}
	var array []json.RawMessage
	if err := json.Unmarshal([]byte(parsed.memberContent), &array); err != nil {
		t.Errorf("TAR member content is not a JSON array: %v", err)
	} else if len(array) != 1 {
		t.Errorf("JSON array length = %d, want 1", len(array))
	}
	if !strings.Contains(parsed.memberContent, marker) {
		t.Errorf("TAR member content does not contain span marker %q", marker)
	}
}
