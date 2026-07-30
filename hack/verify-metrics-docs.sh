#!/usr/bin/env bash
# Verifies that docs/METRICS.md matches the metrics declared in pkg/metrics.
#
# Real implementation lands in Phase 5 with pkg/metrics. Until a metric is
# declared there is nothing to compare, so this exits 0 with an explicit SKIP
# rather than pretending to have checked something.
# See docs/DECISIONS.md ADR-0007.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! grep -rqs 'driftwatch_' pkg/metrics/; then
  echo "verify-metrics-docs: SKIP — pkg/metrics declares no metrics yet (Phase 5)"
  exit 0
fi

echo "verify-metrics-docs: pkg/metrics declares metrics but this check is not"
echo "implemented. Implement it now — see PRD §17.1 and §21.5."
exit 1
