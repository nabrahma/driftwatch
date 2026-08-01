#!/usr/bin/env bash
# Makes the demo's Redis disagree with the events that built it.
#
# What it does is delete a slice of the keyspace directly, behind driftwatch's
# back. That models the three things that actually happen in production — a
# materializer that failed a batch of writes, an eviction under memory
# pressure, or somebody running a command they should not have — and from
# driftwatch's side all three look the same: the store no longer holds what the
# event stream says it should.
#
# It then heals on its own. The publisher keeps emitting, and each deleted key
# comes back the next time an event touches it, so the divergence count rises
# within a couple of sweeps and decays back to zero over the following minute.
# That arc — red, then green, without anybody intervening — is the demo.
#
# What it deliberately does not do is stop the publisher or pause the
# materializer. Either would produce a number that goes up and stays up, which
# proves detection but not resolution, and resolution is the harder half.
set -euo pipefail

cd "$(dirname "$0")/.."

# Git Bash rewrites anything that looks like a Unix path in an argument into a
# Windows one, so `docker exec redis sh -c '...'` arrives inside the container
# as C:/Program Files/Git/... and fails with a baffling "no such file". Off for
# this script; harmless everywhere else.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

REDIS=driftwatch-demo-redis
COUNT="${1:-400}"

if ! docker inspect "$REDIS" >/dev/null 2>&1; then
  echo "demo-inject-drift: $REDIS is not running — start it with 'make demo'" >&2
  exit 1
fi

before=$(docker exec "$REDIS" redis-cli DBSIZE | tr -d '\r')

echo "demo-inject-drift: Redis holds $before keys; deleting $COUNT of them"

# SCAN rather than KEYS, and a bounded slice rather than FLUSHDB. FLUSHDB would
# be a more dramatic graph and a worse demonstration: it wipes the keyspace, so
# the recovery takes as long as a full republish and the coverage ratio
# collapses at the same time, which muddles the one thing the demo is showing.
docker exec "$REDIS" sh -c "
  redis-cli --scan --pattern 'block:*' --count 200 |
    head -n $COUNT |
    xargs -r redis-cli DEL >/dev/null
"

after=$(docker exec "$REDIS" redis-cli DBSIZE | tr -d '\r')

echo "demo-inject-drift: Redis now holds $after keys ($((before - after)) removed)"
echo ""
echo "Watch it at http://localhost:3000"
echo ""
echo "  Within ~10s   the sweep finds the keys missing and raises candidates."
echo "  Within ~15s   a second read confirms them; 'Confirmed divergent keys'"
echo "                goes red and the category breakdown shows missing_in_target."
echo "  Over ~60s     the publisher touches each key again, the materializer"
echo "                writes it back, and the count decays to zero."
echo ""
echo "The two-phase confirmation is why the number does not spike instantly:"
echo "driftwatch will not report a key it has only seen disagree once."
