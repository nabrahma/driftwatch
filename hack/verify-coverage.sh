#!/usr/bin/env bash
# Fails if any package is below its §16.9 coverage floor.
#
# §16.9 says "CI fails below target". Until this script existed it did not: the
# unit job produced cover.out, uploaded it as an artifact, and nothing read the
# number. A threshold nobody enforces is a threshold nobody meets, and the
# packages that had quietly drifted under were pkg/check and pkg/sweeper — the
# two where a gap matters most.
#
# Usage:
#   hack/verify-coverage.sh [cover.out]
#
# Coverage is a floor, not a goal. §16.9 is explicit that a 95%-covered package
# with no property tests is worse than an 85%-covered one with them, so nothing
# here rewards going above the floor and nothing should be tested only to move
# this number.
set -euo pipefail

cd "$(dirname "$0")/.."

PROFILE="${1:-cover.out}"

if [ ! -f "$PROFILE" ]; then
  echo "verify-coverage: $PROFILE not found — run 'make test' first" >&2
  exit 1
fi

MODULE=github.com/nabrahma/driftwatch

# The §16.9 table, verbatim. Package path (without the module prefix), then the
# minimum percentage.
THRESHOLDS="
pkg/event:95
pkg/codec:95
pkg/seqtrack:95
pkg/projection:95
pkg/differ:95
pkg/oracle:90
pkg/sweeper:90
pkg/lag:90
pkg/explain:90
pkg/target:85
pkg/source:85
pkg/check:90
pkg/metrics:90
pkg/clock:90
internal/controller:80
internal/cli:70
"

OVERALL_MIN=88

# controller-gen's zz_generated.deepcopy.go is 400 lines of mechanical
# DeepCopyInto that no meaningful test exercises directly. Left in, it drags
# api/v1alpha1 from 94% to 56% and the number stops carrying information about
# code anyone wrote. Same exclusion as `make cover`.
FILTERED="$(mktemp)"
trap 'rm -f "$FILTERED" "$FUNCS"' EXIT
grep -v 'zz_generated' "$PROFILE" >"$FILTERED"

FUNCS="$(mktemp)"
go tool cover -func="$FILTERED" >"$FUNCS"

# pkg_coverage sums the statements in one package and reports its percentage.
#
# `go tool cover -func` reports per function, not per package, so the package
# figure has to be recomputed. Averaging the per-function percentages would be
# wrong — it weights a one-line getter the same as a 90-line applier — so this
# recovers statement counts from the profile itself.
pkg_coverage() {
  local pkg="$1"
  awk -v pkg="$MODULE/$pkg/" '
    # Profile lines look like:
    #   path/to/file.go:12.34,56.78 numStatements count
    $0 ~ /^mode:/ { next }
    {
      split($0, parts, ":")
      file = parts[1]
      if (index(file, pkg) != 1) next
      # Only files directly in the package, not in a subpackage.
      rest = substr(file, length(pkg) + 1)
      if (index(rest, "/") != 0) next
      n = $2
      hits = $3
      total += n
      if (hits > 0) covered += n
    }
    END {
      if (total == 0) { print "none"; exit }
      printf "%.1f", (covered / total) * 100
    }
  ' "$FILTERED"
}

failed=0
printf '%-24s %8s %8s\n' PACKAGE COVERAGE FLOOR
printf '%-24s %8s %8s\n' ------------------------ -------- --------

for row in $THRESHOLDS; do
  pkg="${row%%:*}"
  min="${row##*:}"
  got="$(pkg_coverage "$pkg")"

  if [ "$got" = "none" ]; then
    printf '%-24s %8s %8s  no profile data\n' "$pkg" "-" "$min%"
    echo "::error::$pkg has no coverage data in $PROFILE"
    failed=1
    continue
  fi

  # Integer comparison in shell; awk does the float.
  if awk -v g="$got" -v m="$min" 'BEGIN { exit !(g + 0 < m + 0) }'; then
    printf '%-24s %8s %8s  BELOW\n' "$pkg" "$got%" "$min%"
    echo "::error file=$pkg::coverage $got% is below the §16.9 floor of $min%"
    failed=1
  else
    printf '%-24s %8s %8s  ok\n' "$pkg" "$got%" "$min%"
  fi
done

overall="$(tail -1 "$FUNCS" | awk '{ gsub("%","",$NF); print $NF }')"
echo
if awk -v g="$overall" -v m="$OVERALL_MIN" 'BEGIN { exit !(g + 0 < m + 0) }'; then
  printf 'overall %s%%, floor %s%%  BELOW\n' "$overall" "$OVERALL_MIN"
  echo "::error::overall coverage $overall% is below the §16.9 floor of $OVERALL_MIN%"
  failed=1
else
  printf 'overall %s%%, floor %s%%  ok\n' "$overall" "$OVERALL_MIN"
fi

if [ "$failed" -ne 0 ]; then
  echo
  echo "verify-coverage: below target" >&2
  exit 1
fi

echo
echo 'verify-coverage: ok'
