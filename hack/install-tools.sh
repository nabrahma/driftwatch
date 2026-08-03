#!/usr/bin/env bash
# Installs the pinned development tools into $(go env GOPATH)/bin.
#
# Versions are pinned so that a lint failure locally is a lint failure in CI.
# Bumping one is a normal change; bumping golangci-lint may require a
# .golangci.yml update, so do it deliberately.
set -euo pipefail

GOLANGCI_VERSION="${GOLANGCI_VERSION:-v1.64.8}"
GOFUMPT_VERSION="${GOFUMPT_VERSION:-v0.7.0}"
BENCHSTAT_VERSION="${BENCHSTAT_VERSION:-v0.0.0-20260709024250-82a0b07e230d}"

GOBIN="$(go env GOPATH)/bin"

install_tool() {
  local name="$1" pkg="$2"
  echo "==> ${name}  (${pkg})"
  go install "${pkg}"
}

echo "installing into ${GOBIN}"
echo

install_tool golangci-lint "github.com/golangci/golangci-lint/cmd/golangci-lint@${GOLANGCI_VERSION}"
install_tool gofumpt       "mvdan.cc/gofumpt@${GOFUMPT_VERSION}"
install_tool benchstat     "golang.org/x/perf/cmd/benchstat@${BENCHSTAT_VERSION}"

echo
echo "done. Ensure ${GOBIN} is on your PATH."
echo
echo "controller-gen and setup-envtest are installed by the Makefile into"
echo "./bin, pinned there rather than here so that the version the targets use"
echo "and the version they check are the same one; see docs/DECISIONS.md"
echo "ADR-0007. kind and ginkgo are installed by the e2e target for the same"
echo "reason."
