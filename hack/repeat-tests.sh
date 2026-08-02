#!/usr/bin/env bash
# Runs a package's tests N times and reports every run that failed.
#
# A test that fails once in fifty runs is worse than no test: it trains everyone
# who sees it to re-run CI rather than to read the failure, and by the time it
# matters nobody believes it. The fault matrix in particular is only worth
# having if its answers are the same every time, so §20 Phase 6 requires twenty
# consecutive clean runs and requires the result to be recorded rather than
# claimed.
#
# Usage:
#   hack/repeat-tests.sh [runs] [package...]
#
#   hack/repeat-tests.sh                  # 20 runs of ./test/faults/
#   hack/repeat-tests.sh 50 ./pkg/...     # 50 runs of another package
set -euo pipefail

cd "$(dirname "$0")/.."

RUNS=${1:-20}
shift || true
PACKAGES=("${@:-./test/faults/}")

echo "repeat-tests: ${RUNS} consecutive runs of ${PACKAGES[*]}"
echo "go: $(go version)"
echo "started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

failures=0
slowest=0
total_start=$(date +%s)

for i in $(seq 1 "$RUNS"); do
  start=$(date +%s%N)

  # -count=1 defeats the test cache. Without it, runs two onward would report
  # the first run's result and twenty runs would prove exactly one thing.
  if output=$(go test -count=1 "${PACKAGES[@]}" 2>&1); then
    status="ok  "
  else
    status="FAIL"
    failures=$((failures + 1))
  fi

  elapsed_ms=$(( ($(date +%s%N) - start) / 1000000 ))
  if [ "$elapsed_ms" -gt "$slowest" ]; then
    slowest=$elapsed_ms
  fi

  printf 'run %2d/%d  %s  %6dms\n' "$i" "$RUNS" "$status" "$elapsed_ms"

  if [ "$status" = "FAIL" ]; then
    echo "$output" | sed 's/^/    /'
  fi
done

echo ""
echo "finished: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "wall time: $(( $(date +%s) - total_start ))s"
echo "slowest run: ${slowest}ms"

if [ "$failures" -ne 0 ]; then
  echo "result: ${failures}/${RUNS} runs FAILED" >&2
  exit 1
fi

echo "result: ${RUNS}/${RUNS} runs passed, no flakes"
