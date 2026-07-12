from __future__ import annotations

import asyncio
import os

import pytest

import casty
from tests.chaos import docker_ctl
from tests.chaos.harness import CHAOS_CONFIG, Counter, Workload, eventually, wait_views

pytestmark = pytest.mark.asyncio

ACTORS = int(os.environ.get("CHAOS_ACTORS", "50"))
DURATION = float(os.environ.get("CHAOS_DURATION", "15"))


async def test_baseline_load_with_consistency_check() -> None:
    """Pure load, no injected failure, with the three configurable axes:
    CHAOS_WRITERS (concurrency), CHAOS_ACTORS (distinct actors) and
    CHAOS_RATE (target ops/s; 0 = maximum capacity), for CHAOS_DURATION seconds.

    Consistency at the end: a key with no write error must have a value EXACTLY
    equal to the acks; a key with an error stays in the range [acked, attempted]."""
    dc = docker_ctl.client()
    all_members = docker_ctl.members(dc)
    addrs = [docker_ctl.addr_of(c) for c in all_members]
    await wait_views(addrs, len(all_members), timeout=300.0)

    client = await casty.connect([docker_ctl.seed_addr(dc)], config=CHAOS_CONFIG)
    keys = [f"base-{i}" for i in range(ACTORS)]
    workload = Workload(client, keys)
    workload.start()
    await asyncio.sleep(DURATION)
    await workload.stop()

    write_errors = sum(workload.attempted[k] - workload.acked[k] for k in keys)
    print(f"\nbaseline: {workload.report()} | {ACTORS} actors | {write_errors} write errors")

    try:
        for key in keys:
            lo, hi = workload.acked[key], workload.attempted[key]

            async def check(k: str = key, lo: int = lo, hi: int = hi) -> None:
                value = await client.actor(Counter, k).read()
                if lo == hi:  # no error on this key: exact equality
                    assert value == lo, f"{k}: {value} != {lo} acks"
                else:
                    assert lo <= value <= hi, f"{k}: {value} outside [{lo}, {hi}]"

            await eventually(check)
    finally:
        await client.close()
    assert workload.total_acked > 0
