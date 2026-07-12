#!/usr/bin/env bash
# Performance sweep over cluster size x concurrency x actor count.
# Brings up each cluster size once, then fires every load config against the
# standing cluster (only the NODES axis pays a re-scale). Appends one JSON line
# per point to $OUT; full per-run logs go to $LOGDIR.
set -uo pipefail

SCRATCH="$(cd "$(dirname "$0")" && pwd)"
OUT="${OUT:-$SCRATCH/sweep_results.jsonl}"
LOGDIR="${LOGDIR:-$SCRATCH/sweep_logs}"
DURATION="${DURATION:-30}"

NODES_LIST=(${NODES_LIST:-15 50 100})
WRITERS_LIST=(${WRITERS_LIST:-50 250})
ACTORS_LIST=(${ACTORS_LIST:-1 25 100 250 500 1000 2500 5000 20000})

cd "$SCRATCH/.."
mkdir -p "$LOGDIR"
: > "$OUT"

cleanup() {
  docker ps -aq --filter label=casty.chaos.spawned | xargs -r docker rm -f >/dev/null 2>&1 || true
  docker compose down -t 1 >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[build] $(date +%T)"
docker compose build >/dev/null

TOTAL=$(( ${#NODES_LIST[@]} * ${#WRITERS_LIST[@]} * ${#ACTORS_LIST[@]} ))
DONE=0

for N in "${NODES_LIST[@]}"; do
  echo "[cluster] scaling to $N nodes  $(date +%T)"
  docker compose up -d seed >/dev/null 2>&1
  docker compose up -d --scale node=$((N - 1)) node >/dev/null 2>&1
  # let the fresh cluster converge before the first load point
  sleep 5
  for W in "${WRITERS_LIST[@]}"; do
    for A in "${ACTORS_LIST[@]}"; do
      DONE=$((DONE + 1))
      TAG="n${N}_w${W}_a${A}"
      LOG="$LOGDIR/$TAG.log"
      printf '[%d/%d] %s  %s ... ' "$DONE" "$TOTAL" "$TAG" "$(date +%T)"
      docker compose run --rm \
        -e SWEEP_WRITERS="$W" -e SWEEP_ACTORS="$A" -e SWEEP_RATE=0 -e SWEEP_DURATION="$DURATION" \
        --entrypoint python driver -m tests.chaos.sweep_single >"$LOG" 2>&1
      if grep -q '^SWEEP_RESULT ' "$LOG"; then
        grep '^SWEEP_RESULT ' "$LOG" | sed 's/^SWEEP_RESULT //' | tee -a "$OUT" \
          | python3 -c 'import sys,json; d=json.load(sys.stdin); print(f"{d[\"ops_per_sec\"]:.0f} ops/s  p50={d[\"p50_ms\"]}ms p99={d[\"p99_ms\"]}ms  err={d[\"err_pct\"]}%")'
      else
        echo "FAILED (see $LOG)"
        tail -3 "$LOG" | sed 's/^/    /'
      fi
    done
  done
  docker compose down -t 1 >/dev/null 2>&1
done

echo "[done] $(date +%T)  ->  $OUT"
