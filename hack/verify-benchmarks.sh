#!/usr/bin/env bash
#
# Asserts the absolute performance targets in PRD §16.8 against the output of
# `make bench`.
#
# This is the half of §16.8's gate that does not need a baseline. §16.8 states
# both an absolute target per benchmark and a relative rule against a committed
# baseline, and they answer different questions:
#
#   absolute   is driftwatch fast enough to do the job the README claims?
#   relative   did this commit make it worse?
#
# A relative gate alone would let performance decay to nothing one acceptable
# step at a time. An absolute gate alone would not notice a 19% regression. The
# benchstat gate in CI covers the second; this covers the first, and it is the
# one that backs the numbers printed in the README.
#
# Targets are asserted with the units §16.8 states them in. Where the PRD gives
# a rate per core and the benchmark reports ns/op, the conversion is written out
# rather than pre-computed, so a reader can check it against the PRD without
# doing arithmetic.
#
# Usage:  hack/verify-benchmarks.sh [docs/benchmarks/current.txt]

set -euo pipefail

FILE="${1:-docs/benchmarks/current.txt}"

if [[ ! -f "$FILE" ]]; then
	echo "verify-benchmarks: $FILE not found — run 'make bench' first" >&2
	exit 1
fi

failures=0
checked=0
missing=()

# nsPerOp NAME — the ns/op column for a benchmark, or empty if it is absent.
#
# Go writes the name with a -N suffix for GOMAXPROCS, so the match is anchored
# on the name followed by either that suffix or whitespace. Without the anchor,
# BenchmarkOracleGet would also match BenchmarkOracleGetMany.
nsPerOp() {
	grep -hoE "^$1(-[0-9]+)?[[:space:]]+[0-9]+[[:space:]]+[0-9.]+ ns/op" "$FILE" |
		tail -1 | grep -oE "[0-9.]+ ns/op" | cut -d' ' -f1
}

# allocsPerOp NAME — the allocs/op column, or empty if absent.
allocsPerOp() {
	grep -hoE "^$1(-[0-9]+)?[[:space:]].*[0-9]+ allocs/op" "$FILE" |
		tail -1 | grep -oE "[0-9]+ allocs/op" | cut -d' ' -f1
}

# require NAME DESCRIPTION ACTUAL LIMIT COMPARISON
#
# COMPARISON is "below" or "at-most". Both read as the PRD phrases them, which
# is why the target column is quoted verbatim in each call below: a threshold
# that has drifted from the document it claims to enforce is worse than none.
require() {
	local name="$1" what="$2" actual="$3" limit="$4" cmp="$5"

	if [[ -z "$actual" ]]; then
		missing+=("$name")
		return
	fi

	checked=$((checked + 1))

	local ok
	ok=$(awk -v a="$actual" -v l="$limit" 'BEGIN { print (a <= l) ? "yes" : "no" }')

	if [[ "$ok" == "yes" ]]; then
		printf '  ok    %-28s %s %s (target %s %s)\n' \
			"$name" "$actual" "$what" "$cmp" "$limit"
	else
		printf '  FAIL  %-28s %s %s (target %s %s)\n' \
			"$name" "$actual" "$what" "$cmp" "$limit" >&2
		failures=$((failures + 1))
	fi
}

echo "verify-benchmarks: asserting PRD §16.8 targets against $FILE"
echo

# --- Throughput targets -----------------------------------------------------
#
# §16.8 states these as events or ops per second per core. A benchmark reporting
# ns/op on one core is the reciprocal: 500k/sec/core is 1e9/5e5 = 2000 ns/op.

require BenchmarkCodecJSONDecode "ns/op" \
	"$(nsPerOp BenchmarkCodecJSONDecode)" 2000 "below" # > 500k events/sec/core
require BenchmarkSeqTrackObserve "ns/op" \
	"$(nsPerOp BenchmarkSeqTrackObserve)" 200 "below" # > 5M ops/sec/core
require BenchmarkProjectionApply "ns/op" \
	"$(nsPerOp BenchmarkProjectionApply)" 500 "below" # > 2M ops/sec/core
require BenchmarkOracleApply "ns/op" \
	"$(nsPerOp BenchmarkOracleApply)" 2000 "below" # > 500k ops/sec/core
require BenchmarkOracleGet "ns/op" \
	"$(nsPerOp BenchmarkOracleGet)" 500 "below" # > 2M ops/sec/core

# --- Latency targets --------------------------------------------------------

require BenchmarkSettledKeys1M "ns/op" \
	"$(nsPerOp BenchmarkSettledKeys1M)" 50000000 "below" # < 50ms
require BenchmarkMarkSuspectAll1M "ns/op" \
	"$(nsPerOp BenchmarkMarkSuspectAll1M)" 1000000 "below" # < 1ms

# --- Allocation targets -----------------------------------------------------
#
# §16.8 asks for "< 3 allocs/op" on the decoder and "0 allocs/op steady state"
# on the tracker. The second is the stricter claim and the one worth having: a
# per-event allocation in the hot path is what turns a GC pause into dropped
# events under load.

require BenchmarkCodecJSONDecode "allocs/op" \
	"$(allocsPerOp BenchmarkCodecJSONDecode)" 2 "at-most" # < 3
require BenchmarkSeqTrackObserve "allocs/op" \
	"$(allocsPerOp BenchmarkSeqTrackObserve)" 0 "at-most" # 0 steady state

# GetMany500 is stated per key rather than per op, because the op is a batch of
# 500. 5 allocs/key is 2500 for the batch.
require BenchmarkGetMany500 "allocs/op" \
	"$(allocsPerOp BenchmarkGetMany500)" 2500 "at-most" # < 5 allocs/key

echo
if [[ ${#missing[@]} -gt 0 ]]; then
	echo "verify-benchmarks: ${#missing[@]} benchmark(s) named by §16.8 are not in $FILE:" >&2
	printf '  %s\n' "${missing[@]}" >&2
	echo >&2
	echo "A target with no benchmark behind it is a claim nobody is checking." >&2
	exit 1
fi

if [[ $failures -gt 0 ]]; then
	echo "verify-benchmarks: $failures of $checked targets not met" >&2
	exit 1
fi

echo "verify-benchmarks: ok ($checked targets met)"
