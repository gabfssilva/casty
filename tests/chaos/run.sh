#!/usr/bin/env bash
# Usage: NODES=100 CHAOS_WRITERS=50 CHAOS_RATE=2000 tests/chaos/run.sh [pytest args]
# CHAOS_WRITERS: concurrent writers (default 20); CHAOS_RATE: ops/s cap (0 = no cap)
# CHAOS_ACTORS: distinct actors in the baseline (default 50); CHAOS_DURATION: seconds of load (default 15)
# CHAOS_NETEM: netem spec for all nodes (default "delay 50ms 10ms")
# CHAOS_NETEM_SLOW: netem spec for the slow minority (default "delay 400ms 100ms loss 10%")
set -euo pipefail
cd "$(dirname "$0")"
NODES="${NODES:-100}"

cleanup() {
  docker ps -aq --filter label=casty.chaos.spawned | xargs -r docker rm -f >/dev/null 2>&1 || true
  docker compose down -t 1
}
trap cleanup EXIT

docker compose build
docker compose up -d seed
docker compose up -d --scale node=$((NODES - 1)) node
docker compose run --rm driver "$@"
