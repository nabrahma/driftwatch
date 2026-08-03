#!/usr/bin/env bash
# Verifies that the generated deepcopy code matches the API types.
#
# This was once a placeholder that failed the moment api/ grew its
# first kubebuilder marker, which is what got it written rather than forgotten.
#
# What it guards is not obvious from outside. controller-runtime deep-copies
# every object on its way out of the informer cache, so a DeepCopyInto that
# misses a newly added map or pointer field hands two callers the same backing
# memory. The controller then mutates an object the cache is still holding, and
# a later reconcile reads a spec nobody applied. Nothing errors; the symptom is
# a controller that occasionally acts on configuration that does not exist.
#
# The CRD, RBAC and webhook manifests are checked by hack/verify-crd-docs.sh.
#
# Usage:
#   hack/verify-codegen.sh            # check, exit 1 on drift
#   hack/verify-codegen.sh --write    # regenerate
set -euo pipefail

cd "$(dirname "$0")/.."

CONTROLLER_GEN="${CONTROLLER_GEN:-$(go env GOPATH)/bin/controller-gen}"

if [ ! -x "$CONTROLLER_GEN" ] && ! command -v controller-gen >/dev/null 2>&1; then
  echo "verify-codegen: controller-gen not found" >&2
  echo "run: go install sigs.k8s.io/controller-tools/cmd/controller-gen@v0.17.2" >&2
  exit 1
fi
[ -x "$CONTROLLER_GEN" ] || CONTROLLER_GEN=controller-gen

mapfile -t generated < <(find api -name 'zz_generated.*.go' | sort)

if [ "${#generated[@]}" -eq 0 ]; then
  echo "verify-codegen: api/ carries kubebuilder markers but no generated code" >&2
  echo "run: make generate" >&2
  exit 1
fi

if [ "${1:-}" = "--write" ]; then
  "$CONTROLLER_GEN" object paths=./api/v1alpha1/...
  echo "verify-codegen: regenerated ${#generated[@]} file(s)"
  exit 0
fi

# The object generator writes next to the source rather than into a directory it
# is told about, so the committed files are moved aside and put back.
BACKUP=.verify-codegen
rm -rf "$BACKUP"
mkdir -p "$BACKUP"

restore() {
  for file in "${generated[@]}"; do
    if [ -f "$BACKUP/$(basename "$file")" ]; then
      cp "$BACKUP/$(basename "$file")" "$file"
    fi
  done
  rm -rf "$BACKUP"
}
trap restore EXIT

for file in "${generated[@]}"; do
  cp "$file" "$BACKUP/$(basename "$file")"
done

"$CONTROLLER_GEN" object paths=./api/v1alpha1/...

drift=0
for file in "${generated[@]}"; do
  # CRLF stripped, so a checkout on Windows does not fail for a reason that has
  # nothing to do with the types.
  if ! diff -u <(tr -d '\r' <"$BACKUP/$(basename "$file")") <(tr -d '\r' <"$file"); then
    echo "verify-codegen: $file is out of date" >&2
    drift=1
  fi
done

# A new API package whose deepcopy was never committed would otherwise pass
# silently, because the loop above only walks what is already there.
mapfile -t after < <(find api -name 'zz_generated.*.go' | sort)
if [ "${#after[@]}" -ne "${#generated[@]}" ]; then
  echo "verify-codegen: generation produced files that are not committed" >&2
  printf '  %s\n' "${after[@]}" >&2
  drift=1
fi

if [ "$drift" -ne 0 ]; then
  echo "" >&2
  echo "verify-codegen: generated code does not match the Go types" >&2
  echo "run: make generate" >&2
  exit 1
fi

echo "verify-codegen: ok (${#generated[@]} generated file(s) match the types)"
