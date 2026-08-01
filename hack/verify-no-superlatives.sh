#!/usr/bin/env bash
# Fails if an unbacked superlative appears anywhere in the repository.
#
# §23 A9 is blunt about why: a README that says "production-grade" and
# "blazing-fast" is a README whose author is selling rather than measuring. Every
# one of these words is a claim with no number behind it, and a reader who
# notices that stops trusting the numbers that *are* there.
#
# The rule is easy to agree with and easy to violate six months later, which is
# what makes it worth a script rather than a paragraph in CONTRIBUTING.
set -euo pipefail

cd "$(dirname "$0")/.."

# The list. Each is a promise about maturity or speed that only means something
# with a measurement attached — at which point the measurement can be written
# instead.
WORDS='production-grade|enterprise|institutional|blazing|robust|seamless|cutting-edge|world-class|battle-tested|bulletproof'

# docs/PRD.md is the specification this project was built from and is quoted
# rather than authored; this script itself necessarily contains the list.
EXCLUDE='docs/PRD\.md|hack/verify-no-superlatives\.sh'

hits=$(
  grep -rinE "$WORDS" \
    --include='*.md' --include='*.go' --include='*.yaml' --include='*.yml' \
    --include='*.json' --include='*.sh' . 2>/dev/null |
    grep -vE "$EXCLUDE" || true
)

if [ -n "$hits" ]; then
  echo "verify-no-superlatives: unbacked superlatives found" >&2
  echo "" >&2
  echo "$hits" >&2
  echo "" >&2
  echo "Replace each with the measured number, or delete it." >&2
  exit 1
fi

echo "verify-no-superlatives: ok (none found)"
