from __future__ import annotations

import random

import pytest

import casty
from tests.chaos import docker_ctl
from tests.chaos.harness import (
    CHAOS_CONFIG,
    Counter,
    Workload,
    eventually,
    verify_counters,
    wait_views,
)

pytestmark = pytest.mark.asyncio


async def test_mass_kill_under_load_converges_and_serves() -> None:
    dc = docker_ctl.client()
    all_members = docker_ctl.members(dc)
    addrs = {c.id: docker_ctl.addr_of(c) for c in all_members}
    n = len(all_members)
    await wait_views(list(addrs.values()), n, timeout=300.0)

    client = await casty.connect([docker_ctl.seed_addr(dc)], config=CHAOS_CONFIG)
    workload = Workload(client, [f"mass-{i}" for i in range(50)])
    workload.start()
    try:
        victims = random.sample(docker_ctl.killable(dc), k=max(1, int(n * 0.3)))
        victim_ids = {v.id for v in victims}
        acked_before_kill = workload.total_acked
        for victim in victims:
            victim.kill()
        workload.mark(f"kill of {len(victims)} nodes")

        survivor_addrs = [addr for cid, addr in addrs.items() if cid not in victim_ids]
        await wait_views(survivor_addrs, n - len(victims))
        workload.mark("converged")
    finally:
        await workload.stop()

    print(f"\nmass_kill: {workload.report()}")
    assert workload.total_acked > acked_before_kill  # the load survived the kill
    try:
        await verify_counters(client, workload)
    finally:
        await client.close()


async def test_sequential_kills_lose_no_acked_state() -> None:
    dc = docker_ctl.client()
    all_members = docker_ctl.members(dc)
    addrs = {c.id: docker_ctl.addr_of(c) for c in all_members}
    n = len(all_members)
    await wait_views(list(addrs.values()), n, timeout=300.0)

    client = await casty.connect([docker_ctl.seed_addr(dc)], config=CHAOS_CONFIG)
    try:
        keys = [f"seq-{i}" for i in range(100)]
        for i, key in enumerate(keys):
            assert await client.actor(Counter, key).add(i + 1) == i + 1

        for _ in range(3):
            victim = random.choice(docker_ctl.killable(dc))
            victim.kill()
            n -= 1
            del addrs[victim.id]
            await wait_views(list(addrs.values()), n)

            for i, key in enumerate(keys):
                expected = i + 1

                async def check(k: str = key, want: int = expected) -> int:
                    value = await client.actor(Counter, k).read()
                    assert value == want
                    return value

                await eventually(check)
    finally:
        await client.close()
