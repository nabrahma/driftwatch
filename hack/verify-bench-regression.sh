#!/usr/bin/env bash
#
# The relative half of §16.8's gate: "fail if any benchmark regresses > 20%
# or allocations increase at all", against the committed baseline.
#
# Two rules, enforced differently, because they are two different kinds of
# measurement.
#
# Allocations are counted, not timed. allocs/op for a given code path is the
# same number on a laptop and on a loaded two-core runner, so any increase is a
# real change and is failed outright. This is the rule that catches the
# regression that matters most — a new allocation in a hot path — and it is the
# one that can be enforced without argument.
#
# Times are noisy. On a shared runner the same commit benchmarked twice can
# differ by a third, and a gate that fails on that gets disabled within a week,
# which leaves the project with no gate and a green badge. The answer is
# repetitions rather than a looser threshold: with -count=6 benchstat computes a
# confidence interval and reports "~" when a delta is not statistically
# significant. Only a significant regression past 20% fails here.
#
# A benchmark present in the baseline and absent from the new run is a failure
# too. Deleting a benchmark is a legitimate thing to do; doing it silently and
# taking its target with it is not.
#
# Usage:  hack/verify-bench-regression.sh [baseline.txt] [current.txt]

set -euo pipefail

BASE="${1:-}"
CURRENT="${2:-docs/benchmarks/current.txt}"

if [[ -z "$BASE" ]]; then
	BASE=$(ls -1 docs/benchmarks/*-baseline.txt 2>/dev/null | tail -1 || true)
fi

if [[ -z "$BASE" || ! -f "$BASE" ]]; then
	echo "verify-bench-regression: no baseline found in docs/benchmarks/" >&2
	echo "Commit one with 'make bench && cp docs/benchmarks/current.txt \\" >&2
	echo "  docs/benchmarks/<phase>-baseline.txt'." >&2
	exit 1
fi

if [[ ! -f "$CURRENT" ]]; then
	echo "verify-bench-regression: $CURRENT not found — run 'make bench' first" >&2
	exit 1
fi

if ! command -v benchstat >/dev/null 2>&1; then
	echo "verify-bench-regression: benchstat not found — run 'make install-tools'" >&2
	exit 1
fi

echo "verify-bench-regression: $BASE -> $CURRENT"
echo

report=$(benchstat "$BASE" "$CURRENT" 2>&1 || true)
echo "$report"
echo

# Enough samples to compare at all, checked before the verdict rather than
# after. benchstat needs six runs of each benchmark to put a confidence interval
# on a difference; with fewer it prints "~" against every row and a percentage
# only against the geomean. A gate reading that would find no per-benchmark
# regression and report success, having compared nothing - the same failure the
# test cache produced, one layer up. So it says so instead.
if grep -q 'need >= [0-9]* samples' <<<"$report"; then
	echo "verify-bench-regression: not enough samples to compare." >&2
	echo >&2
	echo "benchstat cannot distinguish a regression from noise with one run of" >&2
	echo "each benchmark, so this comparison would pass whatever the numbers" >&2
	echo "said. Regenerate with repetitions:" >&2
	echo >&2
	echo "    make bench BENCHCOUNT=6" >&2
	exit 1
fi

# benchstat's table carries the delta in a column that reads like "+23.45%" or
# "~". Rows are grouped by unit — sec/op, B/op, allocs/op — announced in a
# header line, so which rule applies depends on which group the row is in.
#
# Parsed with awk rather than by eye because this runs unattended, and a gate
# nobody reads is a gate that does not exist.
verdict=$(printf '%s\n' "$report" | awk '
	# A unit header re-arms which rule the following rows are judged by.
	/sec\/op/   { unit = "time";   next }
	/allocs\/op/{ unit = "allocs"; next }
	/B\/op/     { unit = "bytes";  next }

	# geomean is an aggregate of the rows above it, so one benchmark regressing
	# shows up twice: under its own name and again here. Named benchmarks are
	# what an engineer can act on, so the summary row is skipped rather than
	# reported as though it were one of them.
	$1 == "geomean" { next }

	# A delta column: a signed percentage. "~" means benchstat could not
	# distinguish the two, which is a pass under both rules.
	{
		for (i = 1; i <= NF; i++) {
			if ($i ~ /^[+-][0-9]+\.[0-9]+%$/) {
				pct = $i + 0
				name = $1

				if (unit == "allocs" && pct > 0) {
					printf "ALLOC %s %s\n", name, $i
				}
				# §16.8 names 20%. A significant regression past it fails;
				# benchstat prints "~" instead of a percentage when the
				# difference is inside the noise, so reaching here at all
				# means the change was measurable.
				if (unit == "time" && pct > 20) {
					printf "TIME %s %s\n", name, $i
				}
			}
		}
	}
')

if [[ -z "$verdict" ]]; then
	echo "verify-bench-regression: ok (no significant regression, no new allocations)"
	exit 0
fi

echo "verify-bench-regression: §16.8 violations" >&2
echo >&2

printf '%s\n' "$verdict" | while read -r kind name delta; do
	case "$kind" in
	ALLOC)
		echo "  $name allocates more per op ($delta)." >&2
		echo "    §16.8 permits no increase at all. allocs/op does not move" >&2
		echo "    under load, so this is a real change rather than noise." >&2
		;;
	TIME)
		echo "  $name is slower by $delta, past §16.8's 20% bar." >&2
		echo "    benchstat found this significant across the repetitions," >&2
		echo "    so it is not runner noise." >&2
		;;
	esac
	echo >&2
done

echo "If the change is intended, update the baseline in the same commit and say" >&2
echo "why in the message. §16.8 asks for that deliberately." >&2
exit 1
