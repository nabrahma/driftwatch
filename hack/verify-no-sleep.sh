#!/usr/bin/env bash
# Fails if time.Sleep appears in a test that should be using the injected clock.
#
# Why this exists: PRD §23 A2. A suite that sleeps is slow, then flaky, then
# unrun, then rotten. §16.4 requires the fake clock instead.
#
# Allowed by design:
#   test/e2e   — drives a real Kubernetes cluster; use gomega Eventually, but
#                genuine waits on external systems cannot be faked away.
#   test/soak  — the point of the test is real elapsed time.
set -euo pipefail

cd "$(dirname "$0")/.."

SEARCH_PATHS=(pkg internal api test/faults test/harness)

present=()
for p in "${SEARCH_PATHS[@]}"; do
  [[ -d "$p" ]] && present+=("$p")
done

if [[ ${#present[@]} -eq 0 ]]; then
  echo "verify-no-sleep: no source trees present yet — nothing to check"
  exit 0
fi

hits="$(grep -rn --include='*.go' -E '(^|[^[:alnum:]_.])time\.Sleep\(' "${present[@]}" || true)"

if [[ -n "$hits" ]]; then
  echo "verify-no-sleep: time.Sleep is not allowed here (PRD §16.4, §23 A2)."
  echo "Use the injected clock (pkg/clock) and its fake implementation instead."
  echo
  echo "$hits"
  exit 1
fi

echo "verify-no-sleep: ok"
