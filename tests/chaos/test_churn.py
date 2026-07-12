from __future__ import annotations

import asyncio
import random

import pytest

import casty
from tests.chaos import docker_ctl
from tests.chaos.harness import CHAOS_CONFIG, Workload, verify_counters, wait_views

pytestmark = pytest.mark.asyncio

CYCLES = 5


async def test_continuous_churn_under_load_loses_no_acked_increments() -> None:
    dc = docker_ctl.client()
    all_members = docker_ctl.members(dc)
    n = len(all_members)
    await wait_views([docker_ctl.addr_of(c) for c in all_members], n, timeout=300.0)

    client = await casty.connect([docker_ctl.seed_addr(dc)], config=CHAOS_CONFIG)
    workload = Workload(client, [f"churn-{i}" for i in range(50)])
    workload.start()
    try:
        for cycle in range(CYCLES):
            docker_ctl.spawn_node(dc)
            stable = [
                c
                for c in docker_ctl.killable(dc)
                if docker_ctl.SPAWNED_LABEL not in c.labels
            ]
            random.choice(stable).kill()
            workload.mark(f"ciclo {cycle + 1}")
            await asyncio.sleep(5.0)
    finally:
        await workload.stop()

    print(f"\nchurn: {workload.report()}")
    survivors = docker_ctl.members(dc)
    await wait_views([docker_ctl.addr_of(c) for c in survivors], len(survivors))
    try:
        await verify_counters(client, workload)
    finally:
        await client.close()
