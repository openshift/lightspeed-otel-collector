# OpenTelemetry Collector — OpenShift Lightspeed
# Follows conventions from lightspeed-operator.

VERSION ?= latest
IMAGE_TAG_BASE ?= quay.io/openshift-lightspeed/otelcol-lightspeed
IMG ?= $(IMAGE_TAG_BASE):$(VERSION)
export IMG

PLATFORM ?= linux/amd64

# Auto-detect podman or docker.
CONTAINER_TOOL ?= $(shell which podman >/dev/null 2>&1 && echo podman || echo docker)

# Collector builder version — must match otelcol_version in builder-config.yaml.
OCB_VERSION ?= 0.155.0
OCB ?= $(LOCALBIN)/ocb

LOCALBIN ?= $(shell pwd)/bin

##@ General

.PHONY: help
help: ## Show this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Development

.PHONY: test
test: ## Run unit tests.
	cd postgresexporter && go test -v ./...
	cd extension/postgresadmin && go test -v ./...

.PHONY: lint
lint: ## Run golangci-lint.
	cd postgresexporter && golangci-lint run ./...
	cd extension/postgresadmin && golangci-lint run ./...

.PHONY: fmt
fmt: ## Run go fmt.
	cd postgresexporter && go fmt ./...
	cd extension/postgresadmin && go fmt ./...

.PHONY: vet
vet: ## Run go vet.
	cd postgresexporter && go vet ./...
	cd extension/postgresadmin && go vet ./...

##@ Build

GENDIR = cmd/otelcol-lightspeed

.PHONY: generate
generate: ocb ## Generate collector source code (commit for hermetic CI builds).
	$(OCB) --skip-compilation --config=builder-config.yaml
	@echo "Generated source in $(GENDIR)/ — commit this directory."

.PHONY: verify-generate
verify-generate: generate ## Verify generated source is up to date.
	@if [ -n "$$(git diff --name-only $(GENDIR)/)" ]; then \
		echo "ERROR: $(GENDIR)/ is out of date. Run 'make generate' and commit."; \
		git diff --stat $(GENDIR)/; \
		exit 1; \
	fi

.PHONY: build
build: ocb ## Build the collector binary.
	$(OCB) --config=builder-config.yaml

.PHONY: run
run: build ## Build and run the collector locally.
	./$(GENDIR)/otelcol-lightspeed --config=config.yaml

.PHONY: clean
clean: ## Remove build artifacts.
	rm -rf $(GENDIR)/otelcol-lightspeed

##@ Container

.PHONY: docker-build
docker-build: test ## Build container image (runs tests first).
	$(CONTAINER_TOOL) build --platform $(PLATFORM) -t $(IMG) .

.PHONY: docker-push
docker-push: ## Push container image.
	$(CONTAINER_TOOL) push $(IMG)

##@ Tools

.PHONY: ocb
ocb: $(OCB) ## Install OCB (OpenTelemetry Collector Builder).
$(OCB):
	@mkdir -p $(LOCALBIN)
	@echo "Installing ocb $(OCB_VERSION)..."
	GOBIN=$(LOCALBIN) go install go.opentelemetry.io/collector/cmd/builder@v$(OCB_VERSION)
	@mv $(LOCALBIN)/builder $(LOCALBIN)/ocb
