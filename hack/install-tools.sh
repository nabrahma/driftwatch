#!/usr/bin/env bash
# Installs the pinned development tools into $(go env GOPATH)/bin.
#
# Versions are pinned so that a lint failure locally is a lint failure in CI.
# Bumping one is a normal change; bumping golangci-lint may require a
# .golangci.yml update, so do it deliberately.
set -euo pipefail

GOLANGCI_VERSION="${GOLANGCI_VERSION:-v1.64.8}"
GOFUMPT_VERSION="${GOFUMPT_VERSION:-v0.7.0}"

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

echo
echo "done. Ensure ${GOBIN} is on your PATH."
echo
echo "Tools for later phases (controller-gen, setup-envtest, kind, ginkgo,"
echo "benchstat) are added to this script by the phase that first needs them;"
echo "see docs/DECISIONS.md ADR-0007."
