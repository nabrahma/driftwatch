#!/usr/bin/env bash
# Verifies that the Helm chart grants exactly what config/rbac/role.yaml does.
#
# The chart cannot include a file from outside itself, so the rules in
# templates/rbac.yaml are a second copy of the ClusterRole generated from the
# kubebuilder markers. Two copies of a permission set is precisely the kind of
# thing that drifts: a marker gets added for a new resource, the generated file
# is regenerated, and the chart quietly keeps granting the old set — so the
# operator installed by Helm fails at runtime with a Forbidden that the
# kustomize install never sees.
#
# So this renders the chart and diffs the rules against the generated file. It
# compares rules only, not names or labels, because those legitimately differ:
# the chart namespaces its role by release.
set -euo pipefail

cd "$(dirname "$0")/.."

CHART=deploy/helm/driftwatch
GENERATED=config/rbac/role.yaml

for tool in helm; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "verify-helm-rbac: $tool not found" >&2
    exit 1
  }
done

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# firstRules prints the first `rules:` block in a stream of YAML documents, and
# stops at the document separator that ends it.
#
# The chart renders three roles into one file: the manager ClusterRole, then the
# namespaced leader-election Role. Only the first has a counterpart in the
# generated file — leader-election leases are deliberately not in the markers,
# see the comment on them in internal/controller/driftcheck_controller.go.
firstRules() {
  awk '
    done { next }
    /^rules:/ { collecting = 1 }
    /^---[[:space:]]*$/ && collecting { done = 1; next }
    collecting { print }
  '
}

helm template driftwatch "$CHART" \
  --set rbac.scope=cluster \
  --show-only templates/rbac.yaml 2>/dev/null |
  firstRules >"$WORK/chart-rules.yaml"

firstRules <"$GENERATED" >"$WORK/generated-rules.yaml"

if [ ! -s "$WORK/chart-rules.yaml" ]; then
  echo "verify-helm-rbac: the chart rendered no ClusterRole rules at all" >&2
  exit 1
fi

normalize() {
  # Strip indentation differences and blank lines: the chart indents its list
  # with two spaces and controller-gen does not, and neither is wrong.
  tr -d '\r' <"$1" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | grep -v '^$'
}

if diff -u <(normalize "$WORK/generated-rules.yaml") <(normalize "$WORK/chart-rules.yaml"); then
  echo "verify-helm-rbac: ok (the chart grants exactly what the markers do)"
  exit 0
fi

echo "" >&2
echo "verify-helm-rbac: $CHART/templates/rbac.yaml disagrees with $GENERATED" >&2
echo "Update the chart's rules to match, or the Helm install will hit a" >&2
echo "Forbidden at runtime that the kustomize install never sees." >&2
exit 1
