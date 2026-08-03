#!/usr/bin/env bash
# Verifies that every row of the fault scenario matrix (§15) has a test.
#
# The matrix is a specification of correctness under failure: sixty rows, each
# naming a fault and the behaviour driftwatch must exhibit. A row with no test
# is a row where the implementation can do whatever it likes, and the gap is
# invisible — the table still reads as though it were covered.
#
# This makes it self-enforcing. It reflects over the test names in test/faults/
# and fails if any row from 1 to 60 lacks a TestFault<NN>_<Name>.
#
# See §15, §17.1 and docs/DECISIONS.md ADR-0007.
set -euo pipefail

cd "$(dirname "$0")/.."

readonly FIRST_ROW=1
readonly LAST_ROW=60
readonly DIR=test/faults

if [ ! -d "$DIR" ]; then
  echo "verify-fault-matrix: $DIR does not exist" >&2
  exit 1
fi

# The row numbers that have a test. The two-digit form is required so that
# sorting a test list orders it by matrix row.
covered=$(grep -rhoE 'func TestFault[0-9]{2}_' "$DIR" | grep -oE '[0-9]{2}' | sort -u)

missing=()
for row in $(seq -w "$FIRST_ROW" "$LAST_ROW"); do
  if ! grep -qx "$row" <<<"$covered"; then
    missing+=("$row")
  fi
done

# A test claiming a row outside the matrix is a copy-paste slip, and it would
# otherwise sit there looking like coverage.
stray=()
for row in $covered; do
  if [ "$((10#$row))" -lt "$FIRST_ROW" ] || [ "$((10#$row))" -gt "$LAST_ROW" ]; then
    stray+=("$row")
  fi
done

# Two tests pointing at one row means some other row is uncovered while the
# count still looks right.
duplicates=$(grep -rhoE 'func TestFault[0-9]{2}_' "$DIR" \
  | grep -oE '[0-9]{2}' | sort | uniq -d || true)

status=0

if [ ${#missing[@]} -gt 0 ]; then
  echo "verify-fault-matrix: ${#missing[@]} matrix rows have no test:" >&2
  printf '  §15 row %s\n' "${missing[@]}" >&2
  echo "" >&2
  echo "Each row needs a test named TestFault<NN>_<Name> in $DIR." >&2
  status=1
fi

if [ ${#stray[@]} -gt 0 ]; then
  echo "verify-fault-matrix: tests claim rows outside 1..$LAST_ROW: ${stray[*]}" >&2
  status=1
fi

if [ -n "$duplicates" ]; then
  echo "verify-fault-matrix: more than one test claims these rows:" >&2
  printf '  §15 row %s\n' $duplicates >&2
  status=1
fi

if [ "$status" -ne 0 ]; then
  exit "$status"
fi

count=$(wc -w <<<"$covered" | tr -d ' ')
echo "verify-fault-matrix: ok (all $count rows of §15 have a test in $DIR)"
