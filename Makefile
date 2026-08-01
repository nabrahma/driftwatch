# driftwatch Makefile
#
# Targets are added as the phases in docs/PRD.md §20 land. The full intended
# target list is §17.5; this file carries the subset that has something real to
# do today. Recipes are POSIX sh — on Windows run make from Git Bash or WSL.

SHELL := /bin/sh
.DEFAULT_GOAL := help

MODULE      := github.com/nabrahma/driftwatch
BIN_DIR     := bin
GOLANGCI_VERSION := v1.64.8

# Kubernetes toolchain. Pinned, because a controller-gen upgrade rewrites every
# generated manifest and hack/verify-crd-docs.sh would then fail CI for a reason
# that has nothing to do with the change under review.
CONTROLLER_GEN_VERSION := v0.17.2
ENVTEST_VERSION        := release-0.20
ENVTEST_K8S_VERSION    := 1.31.0
KIND_VERSION           := v0.26.0

# Resolved from PATH first, then from GOPATH/bin. Two lookups rather than one
# because on Windows the installed binaries are controller-gen.exe, so a bare
# `test -x $(GOPATH)/bin/controller-gen` finds nothing and reinstalls on every
# invocation.
GOPATH_BIN     := $(shell go env GOPATH)/bin
CONTROLLER_GEN ?= $(shell command -v controller-gen 2>/dev/null || echo $(GOPATH_BIN)/controller-gen)
ENVTEST        ?= $(shell command -v setup-envtest 2>/dev/null || echo $(GOPATH_BIN)/setup-envtest)

IMG        ?= ghcr.io/nabrahma/driftwatch:latest
KIND_CLUSTER ?= driftwatch

VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -s -w \
	-X $(MODULE)/internal/buildinfo.Version=$(VERSION) \
	-X $(MODULE)/internal/buildinfo.Commit=$(COMMIT) \
	-X $(MODULE)/internal/buildinfo.Date=$(DATE)

# §8.5: CGO is never required to build driftwatch. The race detector does need
# it, so TEST_FLAGS deliberately does not inherit this.
export CGO_ENABLED := 0

.PHONY: help
help: ## Show this help
	@echo 'driftwatch - make targets'
	@echo ''
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*?## / {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ''

.PHONY: build
build: ## Build both binaries into bin/
	@mkdir -p $(BIN_DIR)
	go build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/ ./cmd/...
	@ls -1 $(BIN_DIR)

.PHONY: lint
lint: ## Run golangci-lint and check formatting with gofumpt
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found — run 'make install-tools'"; exit 1; }
	golangci-lint run --timeout 5m
	@command -v gofumpt >/dev/null 2>&1 || { \
		echo "gofumpt not found — run 'make install-tools'"; exit 1; }
	@out=$$(gofumpt -l -d . | grep -v 'zz_generated' || true); \
	if [ -n "$$out" ]; then \
		echo "$$out"; \
		echo ''; \
		echo "gofumpt found unformatted files — run 'make fmt'"; \
		exit 1; \
	fi
	@echo 'lint: ok'

# Generated files are excluded, and not as a convenience. gofumpt strips the
# redundant import alias controller-gen emits, controller-gen puts it back on
# the next `make generate`, and the two would take turns failing CI forever.
# Generated code is the generator's output, not ours to format.
.PHONY: fmt
fmt: ## Format all Go files with gofumpt, except generated ones
	@command -v gofumpt >/dev/null 2>&1 || { \
		echo "gofumpt not found — run 'make install-tools'"; exit 1; }
	@find . -name '*.go' -not -name 'zz_generated.*' -not -path './.git/*' \
		-exec gofumpt -w {} +

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: test
test: ## Run the unit suite with the race detector and coverage
	CGO_ENABLED=1 go test -race -covermode=atomic -coverprofile=cover.out ./...

.PHONY: cover
cover: ## Report coverage per package, excluding generated code
	@test -f cover.out || { echo "run 'make test' first"; exit 1; }
	@# zz_generated.deepcopy.go is controller-gen's output: 400 lines of
	@# mechanical DeepCopyInto that no meaningful test exercises directly. Left
	@# in, it drags api/v1alpha1 from 94% to 56% and the number stops carrying
	@# information about the code anyone wrote.
	@grep -v 'zz_generated' cover.out >cover.filtered.out
	@go tool cover -func=cover.filtered.out | tail -1
	@rm -f cover.filtered.out

.PHONY: test-fault
test-fault: ## Run the fault scenario matrix (PRD section 15), all 60 rows
	@bash hack/verify-fault-matrix.sh
	go test -count=1 -v ./test/faults/

.PHONY: test-fault-repeat
test-fault-repeat: ## Run the fault matrix 20 times and report any flake
	@bash hack/repeat-tests.sh 20 ./test/faults/

.PHONY: test-integration
test-integration: ## Run the integration suite against real Redis 6 and 7 (needs Docker)
	@docker version --format '{{.Server.Version}}' >/dev/null 2>&1 || \
		{ echo "Docker is not reachable; the integration suite needs a running daemon"; exit 1; }
	@echo "Docker $$(docker version --format '{{.Server.Version}}')"
	CGO_ENABLED=1 go test -tags=integration -race -timeout=25m ./pkg/target/...

.PHONY: test-controller
test-controller: envtest ## Run the CRD and controller suites against envtest
	KUBEBUILDER_ASSETS="$$($(ENVTEST) use $(ENVTEST_K8S_VERSION) -p path)" \
		CGO_ENABLED=1 go test -race -count=1 -timeout=10m ./api/... ./internal/controller/...

.PHONY: manifests
manifests: controller-gen ## Regenerate the CRD, RBAC and webhook manifests
	@CONTROLLER_GEN=$(CONTROLLER_GEN) bash hack/verify-crd-docs.sh --write
	@bash hack/sync-helm-crd.sh --write

.PHONY: generate
generate: controller-gen ## Regenerate deepcopy functions
	$(CONTROLLER_GEN) object paths=./api/v1alpha1/...

.PHONY: verify-manifests
verify-manifests: controller-gen ## Fail if the committed manifests drift from the Go types
	@CONTROLLER_GEN=$(CONTROLLER_GEN) bash hack/verify-codegen.sh
	@CONTROLLER_GEN=$(CONTROLLER_GEN) bash hack/verify-crd-docs.sh
	@bash hack/sync-helm-crd.sh
	@bash hack/verify-helm-rbac.sh

.PHONY: helm-lint
helm-lint: ## Lint and render the chart with the default, dev and prod values
	@command -v helm >/dev/null 2>&1 || { echo "helm not found"; exit 1; }
	helm lint deploy/helm/driftwatch
	helm lint deploy/helm/driftwatch -f deploy/helm/driftwatch/values-dev.yaml
	helm lint deploy/helm/driftwatch -f deploy/helm/driftwatch/values-prod.yaml
	@# Linting only parses; templating is what proves the manifests come out.
	@helm template driftwatch deploy/helm/driftwatch >/dev/null
	@helm template driftwatch deploy/helm/driftwatch \
		-f deploy/helm/driftwatch/values-dev.yaml >/dev/null
	@helm template driftwatch deploy/helm/driftwatch -n driftwatch-system \
		-f deploy/helm/driftwatch/values-prod.yaml >/dev/null
	@echo 'helm-lint: ok'

.PHONY: docker-build
docker-build: ## Build the container image
	docker build \
		--build-arg VERSION=$(VERSION) \
		--build-arg COMMIT=$(COMMIT) \
		--build-arg DATE=$(DATE) \
		-t $(IMG) .

.PHONY: install
install: manifests ## Install the CRD into the current cluster
	kubectl apply -k config/crd

.PHONY: uninstall
uninstall: ## Remove the CRD from the current cluster
	kubectl delete -k config/crd --ignore-not-found

.PHONY: deploy
deploy: manifests ## Deploy the manager into the current cluster
	kubectl apply -k config/default

.PHONY: undeploy
undeploy: ## Remove the manager from the current cluster
	kubectl delete -k config/default --ignore-not-found

.PHONY: kind-up
kind-up: ## Create a Kind cluster and install the CRD
	kind create cluster --name $(KIND_CLUSTER)
	$(MAKE) install

.PHONY: kind-load
kind-load: docker-build ## Build the image and load it into the Kind cluster
	kind load docker-image $(IMG) --name $(KIND_CLUSTER)

.PHONY: kind-down
kind-down: ## Delete the Kind cluster
	kind delete cluster --name $(KIND_CLUSTER)

.PHONY: controller-gen
controller-gen: ## Install the pinned controller-gen
	@command -v controller-gen >/dev/null 2>&1 || test -x "$(CONTROLLER_GEN)" || \
		go install sigs.k8s.io/controller-tools/cmd/controller-gen@$(CONTROLLER_GEN_VERSION)

.PHONY: envtest
envtest: ## Install setup-envtest and fetch the API server binaries
	@command -v setup-envtest >/dev/null 2>&1 || test -x "$(ENVTEST)" || \
		go install sigs.k8s.io/controller-runtime/tools/setup-envtest@$(ENVTEST_VERSION)
	@"$(ENVTEST)" use $(ENVTEST_K8S_VERSION) -p path >/dev/null

COMPOSE := docker compose -f deploy/demo/docker-compose.yaml

# The e2e suite needs no arguments and no environment. §14.5 requires it to work
# from a clean clone with only Docker and Go, so the cluster, both images, the
# CRD and the manager are all brought up by the suite itself.
E2E_TIMEOUT ?= 25m

.PHONY: e2e
e2e: ## Kind up, build, load, run all 8 scenarios, tear down
	@docker version --format '{{.Server.Version}}' >/dev/null 2>&1 || \
		{ echo "Docker is not reachable; make e2e needs a running daemon"; exit 1; }
	go test -tags=e2e -timeout=$(E2E_TIMEOUT) -v ./test/e2e/...

.PHONY: e2e-keep
e2e-keep: ## Run the suite and leave the cluster standing for debugging
	DRIFTWATCH_E2E_KEEP=1 $(MAKE) e2e

.PHONY: e2e-reuse
e2e-reuse: ## Reuse an existing cluster, for fast local iteration
	DRIFTWATCH_E2E_REUSE_CLUSTER=1 $(MAKE) e2e

.PHONY: e2e-break
e2e-break: ## Fail one scenario on purpose, to inspect the diagnostics dump
	DRIFTWATCH_E2E_REUSE_CLUSTER=1 DRIFTWATCH_E2E_BREAK=1 \
		go test -tags=e2e -timeout=$(E2E_TIMEOUT) -v \
		--ginkgo.focus='E1 HappyPath' ./test/e2e/... || true
	@echo ''
	@echo 'The dump is under test/e2e/_artifacts/. That failure was deliberate.'

.PHONY: test-interop
test-interop: ## Prove wire compatibility with real libzmq (needs python3 + pyzmq)
	@python3 -c 'import zmq' 2>/dev/null || python -c 'import zmq' 2>/dev/null || { \
		echo 'pyzmq not found — install it with: python -m pip install pyzmq'; exit 1; }
	go test -tags=interop -timeout=5m -v ./test/interop/

.PHONY: demo
demo: ## Bring up the whole stack: redis, publisher, materializer, driftwatch, prometheus, grafana
	@docker version --format '{{.Server.Version}}' >/dev/null 2>&1 || \
		{ echo "Docker is not reachable; make demo needs a running daemon"; exit 1; }
	@bash hack/prom-rules.sh
	$(COMPOSE) up -d --build --wait
	@echo ''
	@echo '  Grafana      http://localhost:3000   (opens on the dashboard)'
	@echo '  Prometheus   http://localhost:9090'
	@echo '  driftwatch   http://localhost:9091/metrics'
	@echo ''
	@echo '  Give it ~30s to fill the graphs, then:'
	@echo '    make demo-inject-drift    # watch divergent keys rise, then recover'
	@echo '    make demo-down            # tear it all down'
	@echo ''

.PHONY: demo-inject-drift
demo-inject-drift: ## Delete keys from Redis behind driftwatch's back, so drift appears and then resolves
	@bash hack/demo-inject-drift.sh

.PHONY: demo-logs
demo-logs: ## Follow the demo's logs
	$(COMPOSE) logs -f

.PHONY: demo-down
demo-down: ## Tear the demo down, volumes and all
	$(COMPOSE) down -v --remove-orphans

.PHONY: soak
soak: ## Run the soak test (DURATION=60m by default)
	@docker version --format '{{.Server.Version}}' >/dev/null 2>&1 || \
		{ echo "Docker is not reachable; the soak needs a running daemon"; exit 1; }
	DRIFTWATCH_SOAK_DURATION=$(or $(DURATION),60m) \
		go test -tags=soak -timeout=$(or $(SOAK_TIMEOUT),120m) -v -run TestSoak ./test/soak/

.PHONY: install-tools
install-tools: controller-gen envtest ## Install the pinned development tools into $(go env GOPATH)/bin
	GOLANGCI_VERSION=$(GOLANGCI_VERSION) ./hack/install-tools.sh

.PHONY: clean
clean: ## Remove build and test output
	rm -rf $(BIN_DIR) dist cover.out bench.txt test/e2e/_artifacts
	go clean -cache -testcache
	@echo 'clean: ok'
