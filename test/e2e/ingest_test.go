//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestLogIngestionHappyPath(t *testing.T) {
	ctx := context.Background()

	conn, err := grpc.NewClient(
		env.Endpoints.OTLPgRPC,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial collector: %v", err)
	}
	defer func() { _ = conn.Close() }()

	exporter, err := otlploggrpc.New(ctx, otlploggrpc.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("create log exporter: %v", err)
	}

	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(sdklog.NewSimpleProcessor(exporter)),
	)
	defer func() { _ = provider.Shutdown(ctx) }()

	logger := provider.Logger("e2e-test")

	runID := "e2e-run-" + fmt.Sprint(time.Now().UnixMilli())

	var rec log.Record
	rec.SetTimestamp(time.Now())
	rec.SetBody(attribute.StringValue("hello from e2e test"))
	rec.AddAttributes(
		attribute.String("agenticrun.uid", runID),
		attribute.String("agenticrun.phase", "planning"),
		attribute.String("event", "audit.agent.started"),
	)
	logger.Emit(ctx, rec)

	var rec2 log.Record
	rec2.SetTimestamp(time.Now())
	rec2.SetBody(attribute.StringValue("executing tool"))
	rec2.AddAttributes(
		attribute.String("agenticrun.uid", runID),
		attribute.String("agenticrun.phase", "execution"),
		attribute.String("event", "audit.agent.tool.call"),
	)
	logger.Emit(ctx, rec2)

	if err := provider.ForceFlush(ctx); err != nil {
		t.Fatalf("flush logs: %v", err)
	}

	waitForRecordCount(t, env, runID, 2, 10*time.Second)
}
