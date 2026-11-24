package dataverseexporter

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		Type,
		createDefaultConfig,
		exporter.WithTraces(createTracesExporter, component.StabilityLevelDevelopment),
	)
}

func createTracesExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Traces, error) {
	dcfg := cfg.(*Config)
	auth, err := newClusterAuth()
	if err != nil {
		return nil, fmt.Errorf("dataverse exporter requires in-cluster credentials: %w", err)
	}
	return newTracesExporter(ctx, set, dcfg, auth)
}

// newTracesExporter wires the push function into exporterhelper: stock
// queue, batching, and retry. The auth parameter is injected so tests can
// substitute a fake Kubernetes API.
func newTracesExporter(ctx context.Context, set exporter.Settings, dcfg *Config, auth *clusterAuth) (exporter.Traces, error) {
	httpClient, err := dcfg.ClientConfig.ToClient(ctx, nil, set.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("building HTTP client: %w", err)
	}
	tel, err := newTelemetry(set.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("registering telemetry: %w", err)
	}
	e := &dataverseExporter{
		config: dcfg,
		client: &ingressClient{
			client:    httpClient,
			endpoint:  dcfg.Endpoint,
			serviceID: dcfg.ServiceID,
			auth:      auth,
			logger:    set.Logger,
		},
		logger:    set.Logger,
		telemetry: tel,
	}
	// Credentials are resolved lazily on the first upload, not here: the
	// Collector process is shared with other pipelines and must not be
	// taken down by a dataverse credential problem.
	inner, err := exporterhelper.NewTraces(ctx, set, dcfg,
		e.consumeTraces,
		// Let the configured HTTP timeout govern each request, including a 401 retry.
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: 0}),
		exporterhelper.WithRetry(dcfg.RetryConfig),
		exporterhelper.WithQueue(dcfg.QueueConfig),
	)
	if err != nil {
		return nil, err
	}
	// Fan-out isolation: queue-full errors are dropped here instead of
	// surfacing to the receiver, which would make the producer retry and
	// duplicate the batch for every other trace consumer.
	return newQueueFullWrapper(inner, set.Logger, tel), nil
}
