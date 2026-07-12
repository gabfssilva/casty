#!/usr/bin/env bash
# Usage: NODES=100 CHAOS_WRITERS=50 CHAOS_RATE=2000 tests/chaos/run.sh [pytest args]
# CHAOS_WRITERS: writers concorrentes (default 20); CHAOS_RATE: teto de ops/s (0 = sem teto)
# CHAOS_ACTORS: atores distintos no baseline (default 50); CHAOS_DURATION: segundos de carga (default 15)
# CHAOS_NETEM: spec netem p/ todos os nós (default "delay 50ms 10ms")
# CHAOS_NETEM_SLOW: spec netem p/ a minoria lenta (default "delay 400ms 100ms loss 10%")
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
