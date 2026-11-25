package dataverseexporter

import (
	"context"
	"errors"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type dataverseExporter struct {
	config    *Config
	client    *ingressClient
	logger    *zap.Logger
	telemetry *telemetry
}

// consumeTraces is the push function, invoked once per flushed queue batch.
func (e *dataverseExporter) consumeTraces(ctx context.Context, td ptrace.Traces) error {
	payload, name, err := buildPayload(td, e.config.ArchivePathPrefix, e.config.MaxPayloadSize)
	if err != nil {
		var tooLarge *errPayloadTooLarge
		if errors.As(err, &tooLarge) {
			// Counted drop: retrying identical bytes cannot succeed.
			e.logger.Error("dropping oversized trace batch",
				zap.String("member", name), zap.Error(err))
			e.telemetry.recordDroppedBatch(ctx, "payload_too_large", td.SpanCount())
			return nil
		}
		return err
	}
	requestID, err := e.client.upload(ctx, payload)
	if err != nil {
		if consumererror.IsPermanent(err) {
			// Counted drop: a permanent failure (bad HTTP status, credential
			// shape) cannot succeed on retry. Rare, so no log rate limiting.
			reason := dropReasonFor(err)
			e.logger.Error("dropping trace batch after permanent upload error",
				zap.String("reason", reason), zap.Error(err))
			e.telemetry.recordDroppedBatch(ctx, reason, td.SpanCount())
			return nil
		}
		return err // surfaced to retry_on_failure
	}
	e.logger.Info("uploaded trace batch",
		zap.String("member", name), zap.String("request_id", requestID))
	return nil
}
