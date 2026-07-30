#!/usr/bin/env bash
# Verifies that generated deepcopy code and CRD manifests are up to date.
#
# Real implementation lands in Phase 6 together with the DriftCheck types and
# controller-gen. Until the API types carry kubebuilder markers there is nothing
# to regenerate, so this exits 0 with an explicit SKIP rather than pretending to
# have checked something. See docs/DECISIONS.md ADR-0007.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! grep -rqs '+kubebuilder:' api/; then
  echo "verify-codegen: SKIP — api/ carries no kubebuilder markers yet (Phase 6)"
  exit 0
fi

echo "verify-codegen: api/ has kubebuilder markers but this check is not"
echo "implemented. Implement it now — see PRD §17.1."
exit 1
