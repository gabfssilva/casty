"""One load point for the performance sweep: run WRITERS concurrent writers
against ACTORS distinct replicated counters for DURATION seconds against the
standing compose cluster, then emit a single JSON line with throughput, error
count and latency percentiles. No consistency check (the sweep measures
performance, not correctness), so it stays cheap even at 20k actors.

Run inside the driver container:
    docker compose run --rm \
      -e SWEEP_WRITERS=250 -e SWEEP_ACTORS=1000 -e SWEEP_RATE=0 -e SWEEP_DURATION=30 \
      --entrypoint python driver -m tests.chaos.sweep_single
"""

from __future__ import annotations

import asyncio
import json
import os

import uvloop

import casty
from tests.chaos import docker_ctl
from tests.chaos.harness import CHAOS_CONFIG, Counter, wait_views

WRITERS = int(os.environ.get("SWEEP_WRITERS", "50"))
ACTORS = int(os.environ.get("SWEEP_ACTORS", "100"))
RATE = float(os.environ.get("SWEEP_RATE", "0"))  # target ops/s; 0 = uncapped
DURATION = float(os.environ.get("SWEEP_DURATION", "30"))


async def main() -> None:
    dc = docker_ctl.client()
    members = docker_ctl.members(dc)
    addrs = [docker_ctl.addr_of(c) for c in members]
    await wait_views(addrs, len(members), timeout=300.0)

    client = await casty.connect([docker_ctl.seed_addr(dc)], config=CHAOS_CONFIG)
    keys = [f"sw-{i}" for i in range(ACTORS)]
    loop = asyncio.get_running_loop()

    stop = asyncio.Event()
    pace = WRITERS / RATE if RATE > 0 else 0.0
    acked = attempted = errors = 0
    latencies: list[float] = []
    next_i = 0

    async def writer() -> None:
        nonlocal acked, attempted, errors, next_i
        while not stop.is_set():
            key = keys[next_i % len(keys)]
            next_i += 1
            attempted += 1
            t0 = loop.time()
            try:
                await client.actor(Counter, key).add(1)
                acked += 1
                latencies.append(loop.time() - t0)
            except casty.CastyError:
                errors += 1
                await asyncio.sleep(0.1)
            if pace:
                await asyncio.sleep(pace)

    # Warm-up window: touching a cold actor triggers activation across its
    # replicas, so the first hit on each key is far slower than steady state.
    # We time the whole run but report activation cost separately.
    t_start = loop.time()
    tasks = [asyncio.create_task(writer()) for _ in range(WRITERS)]
    await asyncio.sleep(DURATION)
    stop.set()
    await asyncio.gather(*tasks)
    elapsed = loop.time() - t_start
    await client.close()

    latencies.sort()

    def pct(p: float) -> float:
        if not latencies:
            return 0.0
        idx = min(int(len(latencies) * p), len(latencies) - 1)
        return latencies[idx] * 1000.0

    result = {
        "nodes": len(members),
        "writers": WRITERS,
        "actors": ACTORS,
        "rate": RATE,
        "duration": round(elapsed, 2),
        "acked": acked,
        "attempted": attempted,
        "errors": errors,
        "err_pct": round(100.0 * errors / attempted, 3) if attempted else 0.0,
        "ops_per_sec": round(acked / elapsed, 1) if elapsed else 0.0,
        "p50_ms": round(pct(0.50), 2),
        "p95_ms": round(pct(0.95), 2),
        "p99_ms": round(pct(0.99), 2),
        "max_ms": round(latencies[-1] * 1000.0, 2) if latencies else 0.0,
        "mean_ms": round(sum(latencies) / len(latencies) * 1000.0, 2) if latencies else 0.0,
    }
    print("SWEEP_RESULT " + json.dumps(result), flush=True)


if __name__ == "__main__":
    uvloop.run(main())
