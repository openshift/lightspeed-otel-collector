//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/portforward"
	"k8s.io/client-go/transport/spdy"
)

const (
	pgImage = "registry.redhat.io/rhel9/postgresql-16@sha256:42f385ac3c9b8913426da7c57e70bc6617cd237aaf697c667f6385a8c0b0118b"

	pgUser     = "otel"
	pgPassword = "otel"
	pgDatabase = "oteldb"
	pgPort     = 5432

	pollTimeout = 3 * time.Minute
)

// Shared environment for all tests — initialized in TestMain.
var env *testEnv

func getCollectorImage() string {
	img := os.Getenv("OTEL_COLLECTOR_IMAGE")
	if img == "" {
		fmt.Fprintln(os.Stderr, "FATAL: OTEL_COLLECTOR_IMAGE env var is required — set to the collector image to test")
		os.Exit(1)
	}
	return img
}

func TestMain(m *testing.M) {
	collectorImage := getCollectorImage()

	ns := os.Getenv("TEST_NAMESPACE")
	manageNS := ns == ""
	if manageNS {
		ns = fmt.Sprintf("e2e-otel-%d", time.Now().UnixMilli())
		fmt.Printf("creating namespace %s\n", ns)
		mustKubectl("create", "namespace", ns)
	} else {
		fmt.Printf("using existing namespace %s\n", ns)
	}

	// Deploy PostgreSQL (StatefulSet with PVC for data persistence across restarts).
	mustKubectlApply(ns, renderPGManifest())
	mustWaitForRollout(ns, "statefulset", "postgres")

	// Deploy collector.
	dsn := fmt.Sprintf("postgres://%s:%s@postgres.%s.svc:%d/%s?sslmode=disable",
		pgUser, pgPassword, ns, pgPort, pgDatabase)
	collectorConfig := renderCollectorConfig()
	mustKubectlApply(ns, renderCollectorManifest(collectorImage, dsn, collectorConfig))
	mustWaitForRollout(ns, "deployment", "otel-collector")

	// Set up SPDY port-forwarding.
	pfs := setupPortForwards(ns)

	// Wait for health check.
	ep := pfs.Endpoints()
	if err := waitHealthy(ep.Health, 60*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
		os.Exit(1)
	}

	// Connect to PG.
	pool, err := pgxpool.New(context.Background(), ep.PG)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: connect to PG: %v\n", err)
		os.Exit(1)
	}

	env = &testEnv{
		Namespace:    ns,
		PGPool:       pool,
		Endpoints:    ep,
		PortForwards: pfs,
	}

	code := m.Run()

	// Teardown.
	pool.Close()
	pfs.StopAll()
	if manageNS {
		_ = exec.Command("kubectl", "delete", "namespace", ns, "--wait=false").Run()
		fmt.Printf("deleted namespace %s\n", ns)
	}

	os.Exit(code)
}

// --- Test Environment ---

type endpoints struct {
	OTLPgRPC string
	AdminAPI string
	Health   string
	PG       string
}

type testEnv struct {
	Namespace    string
	PGPool       *pgxpool.Pool
	Endpoints    endpoints
	PortForwards *portForwards
}

// --- kubectl helpers ---

func kubectl(t *testing.T, args ...string) string {
	t.Helper()
	out, err := runKubectl(args...)
	if err != nil {
		t.Fatalf("kubectl %s: %v", strings.Join(args, " "), err)
	}
	return out
}

func mustKubectl(args ...string) {
	if _, err := runKubectl(args...); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: kubectl %s: %v\n", strings.Join(args, " "), err)
		os.Exit(1)
	}
}

func runKubectl(args ...string) (string, error) {
	cmd := exec.Command("kubectl", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w\nstderr: %s", err, stderr.String())
	}
	return strings.TrimSpace(stdout.String()), nil
}

func mustKubectlApply(ns, manifest string) {
	cmd := exec.Command("kubectl", "apply", "-n", ns, "-f", "-")
	cmd.Stdin = strings.NewReader(manifest)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: kubectl apply: %v\nstderr: %s\n", err, stderr.String())
		os.Exit(1)
	}
}

func mustWaitForRollout(ns, kind, name string) {
	cmd := exec.Command("kubectl", "rollout", "status",
		kind+"/"+name, "-n", ns, "--timeout="+pollTimeout.String())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: rollout %s/%s/%s: %v\nstderr: %s\n", ns, kind, name, err, stderr.String())
		os.Exit(1)
	}
}

func waitForRollout(t *testing.T, ns, kind, name string) {
	t.Helper()
	cmd := exec.Command("kubectl", "rollout", "status",
		kind+"/"+name, "-n", ns, "--timeout="+pollTimeout.String())
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("rollout status %s/%s/%s: %v\nstderr: %s", ns, kind, name, err, stderr.String())
	}
}

func waitHealthy(healthAddr string, timeout time.Duration) error {
	url := fmt.Sprintf("http://%s", healthAddr)
	client := &http.Client{Timeout: 5 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := client.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("health check at %s did not pass within %s", healthAddr, timeout)
}

// --- SPDY Port Forwarding ---

type portForward struct {
	mu         sync.Mutex
	clientset  *kubernetes.Clientset
	ns         string
	svc        string
	remotePort int
	localPort  int
	stopCh     chan struct{}
	readyCh    chan struct{}
	gen        uint64
	stopped    bool
}

func newK8sClientset() *kubernetes.Clientset {
	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, _ := os.UserHomeDir()
		kubeconfig = home + "/.kube/config"
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: build kubeconfig: %v\n", err)
		os.Exit(1)
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: create clientset: %v\n", err)
		os.Exit(1)
	}
	return clientset
}

func newPortForward(clientset *kubernetes.Clientset, ns, svc string, remotePort int) *portForward {
	pf := &portForward{
		clientset:  clientset,
		ns:         ns,
		svc:        svc,
		remotePort: remotePort,
	}
	pf.mustStart()
	return pf
}

func (pf *portForward) mustStart() {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	if err := pf.startLocked(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: port-forward %s/%s:%d: %v\n", pf.ns, pf.svc, pf.remotePort, err)
		os.Exit(1)
	}
}

func (pf *portForward) startLocked() error {
	podName, err := pf.findPodForService()
	if err != nil {
		return fmt.Errorf("find pod for svc/%s: %w", pf.svc, err)
	}

	kubeconfig := os.Getenv("KUBECONFIG")
	if kubeconfig == "" {
		home, _ := os.UserHomeDir()
		kubeconfig = home + "/.kube/config"
	}
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("build config: %w", err)
	}

	req := pf.clientset.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(pf.ns).
		Name(podName).
		SubResource("portforward")

	transport, upgrader, err := spdy.RoundTripperFor(config)
	if err != nil {
		return fmt.Errorf("SPDY roundtripper: %w", err)
	}

	dialer := spdy.NewDialer(upgrader, &http.Client{Transport: transport}, "POST", req.URL())

	pf.stopCh = make(chan struct{})
	pf.readyCh = make(chan struct{})

	localSpec := "0"
	if pf.localPort != 0 {
		localSpec = fmt.Sprintf("%d", pf.localPort)
	}
	ports := []string{fmt.Sprintf("%s:%d", localSpec, pf.remotePort)}
	fw, err := portforward.New(dialer, ports, pf.stopCh, pf.readyCh, nil, nil)
	if err != nil {
		return fmt.Errorf("create portforwarder: %w", err)
	}

	errCh := make(chan error, 1)
	go func() {
		if err := fw.ForwardPorts(); err != nil {
			select {
			case errCh <- err:
			default:
			}
		}
	}()

	select {
	case <-pf.readyCh:
		fwPorts, err := fw.GetPorts()
		if err != nil || len(fwPorts) == 0 {
			close(pf.stopCh)
			return fmt.Errorf("get forwarded ports: %v", err)
		}
		pf.localPort = int(fwPorts[0].Local)
		pf.gen++
		go pf.monitor(errCh, pf.gen, pf.stopCh)
		return nil
	case err := <-errCh:
		close(pf.stopCh)
		return fmt.Errorf("forward failed: %w", err)
	case <-time.After(30 * time.Second):
		close(pf.stopCh)
		return fmt.Errorf("timeout waiting for port-forward")
	}
}

func (pf *portForward) monitor(errCh <-chan error, startGen uint64, stopCh chan struct{}) {
	select {
	case <-stopCh:
		return
	case err := <-errCh:
		pf.mu.Lock()
		defer pf.mu.Unlock()
		if pf.stopped || pf.gen != startGen {
			return
		}
		fmt.Fprintf(os.Stderr, "port-forward %s/%s:%d died (%v), reconnecting...\n",
			pf.ns, pf.svc, pf.remotePort, err)
		time.Sleep(2 * time.Second)
		if pf.stopped || pf.gen != startGen {
			return
		}
		if err := pf.startLocked(); err != nil {
			fmt.Fprintf(os.Stderr, "WARN: auto-reconnect %s/%s:%d failed: %v\n",
				pf.ns, pf.svc, pf.remotePort, err)
		}
	}
}

func (pf *portForward) findPodForService() (string, error) {
	svc, err := pf.clientset.CoreV1().Services(pf.ns).Get(context.Background(), pf.svc, metav1.GetOptions{})
	if err != nil {
		return "", err
	}

	selector := ""
	for k, v := range svc.Spec.Selector {
		if selector != "" {
			selector += ","
		}
		selector += k + "=" + v
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		pods, err := pf.clientset.CoreV1().Pods(pf.ns).List(context.Background(), metav1.ListOptions{
			LabelSelector: selector,
		})
		if err != nil {
			return "", err
		}

		for _, pod := range pods.Items {
			if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil {
				return pod.Name, nil
			}
		}
		time.Sleep(2 * time.Second)
	}
	return "", fmt.Errorf("no running pod found for svc/%s (selector: %s) within 30s", pf.svc, selector)
}

func (pf *portForward) Reconnect() {
	pf.mu.Lock()
	select {
	case <-pf.stopCh:
	default:
		close(pf.stopCh)
	}
	if err := pf.startLocked(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: reconnect %s/%s:%d: %v\n", pf.ns, pf.svc, pf.remotePort, err)
		os.Exit(1)
	}
	pf.mu.Unlock()
}

func (pf *portForward) Stop() {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	pf.stopped = true
	if pf.stopCh != nil {
		select {
		case <-pf.stopCh:
		default:
			close(pf.stopCh)
		}
	}
}

func (pf *portForward) Addr() string {
	pf.mu.Lock()
	defer pf.mu.Unlock()
	return fmt.Sprintf("127.0.0.1:%d", pf.localPort)
}

// --- Port-forward group ---

type portForwards struct {
	OTLP   *portForward
	Admin  *portForward
	Health *portForward
	PG     *portForward
}

func setupPortForwards(ns string) *portForwards {
	clientset := newK8sClientset()
	return &portForwards{
		OTLP:   newPortForward(clientset, ns, "otel-collector", 4317),
		Admin:  newPortForward(clientset, ns, "otel-collector", 8080),
		Health: newPortForward(clientset, ns, "otel-collector", 13133),
		PG:     newPortForward(clientset, ns, "postgres", pgPort),
	}
}

func (pfs *portForwards) Endpoints() endpoints {
	return endpoints{
		OTLPgRPC: pfs.OTLP.Addr(),
		AdminAPI: pfs.Admin.Addr(),
		Health:   pfs.Health.Addr(),
		PG: fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
			pgUser, pgPassword, pfs.PG.Addr(), pgDatabase),
	}
}

func (pfs *portForwards) StopAll() {
	pfs.OTLP.Stop()
	pfs.Admin.Stop()
	pfs.Health.Stop()
	pfs.PG.Stop()
}

func (pfs *portForwards) ReconnectCollector(env *testEnv) {
	pfs.OTLP.Reconnect()
	pfs.Admin.Reconnect()
	pfs.Health.Reconnect()
	env.Endpoints.OTLPgRPC = pfs.OTLP.Addr()
	env.Endpoints.AdminAPI = pfs.Admin.Addr()
	env.Endpoints.Health = pfs.Health.Addr()
}

func (pfs *portForwards) ReconnectPG() *pgxpool.Pool {
	pfs.PG.Reconnect()
	dsn := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		pgUser, pgPassword, pfs.PG.Addr(), pgDatabase)
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: reconnect PG pool: %v\n", err)
		os.Exit(1)
	}
	return pool
}

// --- Manifest rendering ---

func renderPGManifest() string {
	return fmt.Sprintf(`---
apiVersion: v1
kind: Secret
metadata:
  name: postgres-secret
stringData:
  password: "%s"
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
spec:
  serviceName: postgres
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: %s
        imagePullPolicy: Always
        ports:
        - containerPort: %d
        env:
        - name: POSTGRESQL_USER
          value: "%s"
        - name: POSTGRESQL_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-secret
              key: password
        - name: POSTGRESQL_DATABASE
          value: "%s"
        volumeMounts:
        - name: data
          mountPath: /var/lib/pgsql/data
        readinessProbe:
          tcpSocket:
            port: %d
          initialDelaySeconds: 5
          periodSeconds: 5
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 1Gi
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
spec:
  selector:
    app: postgres
  ports:
  - port: %d
    targetPort: %d
`, pgPassword, pgImage, pgPort, pgUser, pgDatabase, pgPort, pgPort, pgPort)
}

func renderCollectorConfig() string {
	return `receivers:
  otlp:
    protocols:
      grpc:
        endpoint: 0.0.0.0:4317
      http:
        endpoint: 0.0.0.0:4318

processors:
  batch:
    timeout: 200ms
    send_batch_size: 50

exporters:
  postgres:
    connection_string: "${env:POSTGRES_CONNECTION_STRING}"
    schema: templogs
    logs_table: logs
    retry_on_failure:
      enabled: true
      initial_interval: 100ms
      max_interval: 1s
      max_elapsed_time: 10s
    sending_queue:
      enabled: true
      num_consumers: 2
      queue_size: 100
      storage: file_storage
  nop:

extensions:
  health_check:
    endpoint: 0.0.0.0:13133
  postgres_admin:
    endpoint: 0.0.0.0:8080
    connection_string: "${env:POSTGRES_CONNECTION_STRING}"
    schema: templogs
    logs_table: logs
  file_storage:
    directory: /var/lib/otelcol/file_storage
    create_directory: true

service:
  extensions: [health_check, file_storage, postgres_admin]
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [postgres]
    traces:
      receivers: [otlp]
      exporters: [nop]
  telemetry:
    logs:
      level: info
`
}

func renderCollectorManifest(image, dsn, config string) string {
	tmpl := `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: otel-collector-config
data:
  config.yaml: |
{{.Config}}
---
apiVersion: v1
kind: Secret
metadata:
  name: otel-collector-dsn
stringData:
  connection-string: "{{.DSN}}"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: otel-collector
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: otel-collector
  template:
    metadata:
      labels:
        app: otel-collector
    spec:
      containers:
      - name: collector
        image: {{.Image}}
        imagePullPolicy: Always
        args: ["--config=/etc/otelcol/config.yaml"]
        ports:
        - containerPort: 4317
          name: otlp-grpc
        - containerPort: 4318
          name: otlp-http
        - containerPort: 8080
          name: admin
        - containerPort: 13133
          name: health
        env:
        - name: POSTGRES_CONNECTION_STRING
          valueFrom:
            secretKeyRef:
              name: otel-collector-dsn
              key: connection-string
        volumeMounts:
        - name: config
          mountPath: /etc/otelcol
          readOnly: true
        - name: file-storage
          mountPath: /var/lib/otelcol/file_storage
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
          name: otel-collector-config
      - name: file-storage
        emptyDir:
          sizeLimit: 100Mi
---
apiVersion: v1
kind: Service
metadata:
  name: otel-collector
spec:
  selector:
    app: otel-collector
  ports:
  - port: 4317
    targetPort: 4317
    name: otlp-grpc
  - port: 4318
    targetPort: 4318
    name: otlp-http
  - port: 8080
    targetPort: 8080
    name: admin
  - port: 13133
    targetPort: 13133
    name: health
`

	indentedConfig := ""
	for _, line := range strings.Split(config, "\n") {
		indentedConfig += "    " + line + "\n"
	}

	parsed, err := template.New("collector").Parse(tmpl)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: parse collector template: %v\n", err)
		os.Exit(1)
	}
	var buf bytes.Buffer
	if err := parsed.Execute(&buf, map[string]string{
		"Config": indentedConfig,
		"DSN":    dsn,
		"Image":  image,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: execute collector template: %v\n", err)
		os.Exit(1)
	}
	return buf.String()
}
