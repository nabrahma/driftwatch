#!/usr/bin/env bash
# Verifies that docs/METRICS.md matches the metrics declared in pkg/metrics.
#
# The names in that file are the public contract: dashboards, recording rules
# and alerts all query them by string, and every one of them breaks silently
# when a metric is renamed. Generating the documentation from the registry and
# failing CI on a mismatch is what makes a quiet rename impossible to land.
#
# Usage:
#   hack/verify-metrics-docs.sh            # check, exit 1 on drift
#   hack/verify-metrics-docs.sh --write    # regenerate the file
#
# See docs/DECISIONS.md ADR-0007 and PRD §17.1.
set -euo pipefail

cd "$(dirname "$0")/.."

DOC=docs/METRICS.md
GENERATED=$(mktemp)
trap 'rm -f "$GENERATED"' EXIT

go run ./hack/metricsdoc >"$GENERATED"

if [ "${1:-}" = "--write" ]; then
  cp "$GENERATED" "$DOC"
  echo "verify-metrics-docs: wrote $DOC ($(grep -c '^### ' "$DOC") metrics)"
  exit 0
fi

if [ ! -f "$DOC" ]; then
  echo "verify-metrics-docs: $DOC does not exist" >&2
  echo "run: hack/verify-metrics-docs.sh --write" >&2
  exit 1
fi

# Compare with CRLF stripped, so a checkout on Windows does not fail the check
# for a reason that has nothing to do with the metrics.
if diff -u <(tr -d '\r' <"$DOC") <(tr -d '\r' <"$GENERATED"); then
  echo "verify-metrics-docs: ok ($(grep -c '^### ' "$DOC") metrics documented)"
  exit 0
fi

echo "" >&2
echo "verify-metrics-docs: $DOC is out of date with pkg/metrics/registry.go" >&2
echo "run: hack/verify-metrics-docs.sh --write" >&2
exit 1
