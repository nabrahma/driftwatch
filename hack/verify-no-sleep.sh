#!/usr/bin/env bash
# Fails if time.Sleep appears anywhere outside test/e2e and test/soak.
#
# Why this exists: §23 A2. A suite that sleeps is slow, then flaky, then
# unrun, then rotten. §16.4 requires the injected fake clock instead, and this
# check is a release gate because the sweeper and the lag
# estimator are the two packages where sleeping would be most tempting and most
# corrosive — every one of their behaviours is defined in terms of elapsed time.
#
# The scan is repo-wide rather than a list of directories, so a new package
# cannot quietly fall outside it.
#
# Allowed by design, and only these:
#   test/e2e   — drives a real Kubernetes cluster; prefer gomega Eventually,
#                but genuine waits on external systems cannot be faked away.
#   test/soak  — real elapsed time is the subject of the test.
set -euo pipefail

cd "$(dirname "$0")/.."

EXEMPT_DIRS=(
	"test/e2e"
	"test/soak"
)

# Build the exclusion list for ripgrep-style pruning with plain find/grep, so
# this runs the same on a CI runner as on a laptop with no extra tooling.
mapfile -t candidates < <(
	find . -type f -name '*.go' \
		-not -path './.git/*' \
		-not -path './vendor/*' \
		| sed 's|^\./||' \
		| sort
)

violations=()
for file in "${candidates[@]}"; do
	exempt=false
	for dir in "${EXEMPT_DIRS[@]}"; do
		case "$file" in
		"$dir"/*) exempt=true ;;
		esac
	done
	if [[ "$exempt" == true ]]; then
		continue
	fi

	# Match time.Sleep( but not a longer identifier ending in it, and not a
	# mention inside a comment about the rule itself.
	if hits="$(grep -nE '(^|[^[:alnum:]_.])time\.Sleep\(' "$file" | grep -v '^[0-9]*:[[:space:]]*//' || true)"; then
		if [[ -n "$hits" ]]; then
			while IFS= read -r line; do
				violations+=("$file:$line")
			done <<<"$hits"
		fi
	fi
done

if [[ ${#violations[@]} -gt 0 ]]; then
	echo "verify-no-sleep: time.Sleep is not allowed outside ${EXEMPT_DIRS[*]}"
	echo
	echo "Use the injected clock (pkg/clock) and its fake instead. A test that"
	echo "sleeps is slow, then flaky, then unrun (§16.4, §23 A2)."
	echo
	printf '  %s\n' "${violations[@]}"
	exit 1
fi

echo "verify-no-sleep: ok (${#candidates[@]} Go files scanned, exempt: ${EXEMPT_DIRS[*]})"
