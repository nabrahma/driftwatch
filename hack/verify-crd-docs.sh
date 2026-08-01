#!/usr/bin/env bash
# Verifies that every property in the generated CRD carries a description, and
# that the committed manifests match the Go types.
#
# `kubectl explain driftcheck.spec.policy.settlementWindow` renders the schema's
# description and nothing else. A field with no description is one an operator
# has to read the Go source to configure, which is not a documented API — and
# §20 Phase 7 makes descriptions on every field an exit criterion. Enforcing it
# here is what stops the next field added from quietly arriving undocumented.
#
# The second half catches the other failure: manifests committed from an older
# version of the types, so `kubectl apply -f config/crd/` installs a schema that
# rejects a spec the binary accepts.
#
# Usage:
#   hack/verify-crd-docs.sh            # check, exit 1 on a problem
#   hack/verify-crd-docs.sh --write    # regenerate the manifests
set -euo pipefail

cd "$(dirname "$0")/.."

CONTROLLER_GEN="${CONTROLLER_GEN:-$(go env GOPATH)/bin/controller-gen}"

if [ ! -x "$CONTROLLER_GEN" ] && ! command -v controller-gen >/dev/null 2>&1; then
  echo "verify-crd-docs: controller-gen not found" >&2
  echo "run: go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.17.2" >&2
  exit 1
fi
[ -x "$CONTROLLER_GEN" ] || CONTROLLER_GEN=controller-gen

generate() {
  local crd_out="$1" rbac_out="$2" webhook_out="$3"
  "$CONTROLLER_GEN" \
    crd:generateEmbeddedObjectMeta=false \
    paths=./api/v1alpha1/... \
    output:crd:artifacts:config="$crd_out"
  "$CONTROLLER_GEN" \
    rbac:roleName=driftwatch-manager \
    webhook \
    paths=./internal/controller/... paths=./api/v1alpha1/... \
    output:rbac:artifacts:config="$rbac_out" \
    output:webhook:artifacts:config="$webhook_out"
}

if [ "${1:-}" = "--write" ]; then
  generate config/crd/bases config/rbac config/webhook
  go run ./hack/crddoc config/crd/bases/*.yaml
  echo "verify-crd-docs: wrote config/crd/bases, config/rbac and config/webhook"
  exit 0
fi

# Inside the repo rather than in the system temp directory, and that is not a
# stylistic choice. controller-gen's output rules are colon-separated markers —
# output:crd:artifacts:config=<path> — so a Windows temp path like
# C:/Users/... splits on its own drive letter and the generator rejects the
# whole marker with a usage dump. A relative path has no colon in it anywhere.
WORK=.verify-crd-docs
rm -rf "$WORK"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/crd" "$WORK/rbac" "$WORK/webhook"

generate "$WORK/crd" "$WORK/rbac" "$WORK/webhook"

# Descriptions first: it is the more useful failure to report, and it is true of
# the freshly generated schema whether or not the committed one is stale.
go run ./hack/crddoc "$WORK"/crd/*.yaml

drift=0
for dir in crd/bases rbac webhook; do
  src="$WORK/$(basename "$dir")"
  [ "$dir" = "crd/bases" ] && src="$WORK/crd"

  for generated in "$src"/*.yaml; do
    committed="config/$dir/$(basename "$generated")"
    if [ ! -f "$committed" ]; then
      echo "verify-crd-docs: $committed is missing" >&2
      drift=1
      continue
    fi
    # CRLF stripped, so a checkout on Windows does not fail for a reason that
    # has nothing to do with the schema.
    if ! diff -u <(tr -d '\r' <"$committed") <(tr -d '\r' <"$generated"); then
      echo "verify-crd-docs: $committed is out of date" >&2
      drift=1
    fi
  done
done

if [ "$drift" -ne 0 ]; then
  echo "" >&2
  echo "verify-crd-docs: committed manifests do not match the Go types" >&2
  echo "run: hack/verify-crd-docs.sh --write" >&2
  exit 1
fi

echo "verify-crd-docs: ok (manifests match the types, every property documented)"
