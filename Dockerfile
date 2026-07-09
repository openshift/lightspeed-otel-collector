# Stage 1: Build the custom collector binary using OCB.
# UBI9 go-toolset provides Go + standard build tools on a Red Hat base.
FROM registry.redhat.io/ubi9/go-toolset:9.8-1780490420 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# Cache Go module downloads before copying source — changing source code
# won't invalidate the (slow) module download layer.
COPY postgresexporter/go.mod postgresexporter/go.sum postgresexporter/
RUN cd postgresexporter && go mod download

COPY extension/postgresadmin/go.mod extension/postgresadmin/go.sum extension/postgresadmin/
RUN cd extension/postgresadmin && go mod download

# Copy the source and builder manifest.
COPY postgresexporter/ postgresexporter/
COPY extension/ extension/
COPY builder-config.yaml builder-config.yaml

# Switch to root for build steps (go-toolset default user can't write to /workspace/bin).
USER root

# Install OCB and build the collector.
ARG OCB_VERSION=0.155.0
RUN GOBIN=/workspace/bin go install go.opentelemetry.io/collector/cmd/builder@v${OCB_VERSION} && \
    mv /workspace/bin/builder /workspace/bin/ocb

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    /workspace/bin/ocb --config=builder-config.yaml


# Stage 2: Minimal runtime image.
# UBI9 minimal has no shell, no package manager — small footprint, fewer CVEs.
FROM registry.redhat.io/ubi9/ubi-minimal:9.8-1782366411

WORKDIR /

COPY --from=builder /workspace/dist/otelcol-lightspeed .

# Red Hat certification requires LICENSE under /licenses/.
RUN mkdir /licenses
COPY LICENSE /licenses/.

LABEL name="openshift-lightspeed/otelcol-lightspeed" \
      summary="Custom OpenTelemetry Collector for OpenShift Lightspeed" \
      description="Receives OTLP telemetry and writes logs directly to PostgreSQL." \
      io.k8s.display-name="OTel Collector — Lightspeed" \
      io.k8s.description="Custom OpenTelemetry Collector distribution that exports logs to PostgreSQL." \
      io.openshift.tags="opentelemetry,otel,collector,postgres,logs"

# OTLP gRPC/HTTP, health check, and admin API ports.
EXPOSE 4317 4318 8080 13133

# Run as non-root, same UID as lightspeed-operator.
USER 65532:65532

ENTRYPOINT ["/otelcol-lightspeed"]
