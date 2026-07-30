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
	@out=$$(gofumpt -l -d .); \
	if [ -n "$$out" ]; then \
		echo "$$out"; \
		echo ''; \
		echo "gofumpt found unformatted files — run 'make fmt'"; \
		exit 1; \
	fi
	@echo 'lint: ok'

.PHONY: fmt
fmt: ## Format all Go files with gofumpt
	@command -v gofumpt >/dev/null 2>&1 || { \
		echo "gofumpt not found — run 'make install-tools'"; exit 1; }
	gofumpt -w .

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: test
test: ## Run the unit suite with the race detector and coverage
	CGO_ENABLED=1 go test -race -covermode=atomic -coverprofile=cover.out ./...

.PHONY: install-tools
install-tools: ## Install the pinned development tools into $(go env GOPATH)/bin
	GOLANGCI_VERSION=$(GOLANGCI_VERSION) ./hack/install-tools.sh

.PHONY: clean
clean: ## Remove build and test output
	rm -rf $(BIN_DIR) dist cover.out bench.txt test/e2e/_artifacts
	go clean -cache -testcache
	@echo 'clean: ok'
