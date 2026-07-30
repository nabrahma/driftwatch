#!/usr/bin/env bash
# Verifies that every row of the fault scenario matrix (PRD §15) has a test.
#
# Real implementation lands in Phase 4 with test/faults. Until those files
# contain tests there is nothing to cross-check, so this exits 0 with an
# explicit SKIP rather than pretending to have checked something.
# See docs/DECISIONS.md ADR-0007.
set -euo pipefail

cd "$(dirname "$0")/.."

if ! grep -rqs --include='*_test.go' 'func Test' test/faults/; then
  echo "verify-fault-matrix: SKIP — test/faults has no tests yet (Phase 4)"
  exit 0
fi

echo "verify-fault-matrix: test/faults has tests but this check is not"
echo "implemented. Implement it now — see PRD §17.1 and §15."
exit 1
