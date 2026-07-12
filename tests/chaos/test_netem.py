from __future__ import annotations

import asyncio
import os
import random

import pytest

import casty
from tests.chaos import docker_ctl
from tests.chaos.harness import CHAOS_CONFIG, Workload, verify_counters, wait_views

pytestmark = pytest.mark.asyncio

# specs netem completos, ex.: "delay 200ms 50ms", "delay 20ms loss 5%", "rate 1mbit"
NETEM_ALL = os.environ.get("CHAOS_NETEM", "delay 50ms 10ms")
NETEM_SLOW = os.environ.get("CHAOS_NETEM_SLOW", "delay 400ms 100ms loss 10%")


async def test_latency_everywhere_under_load_stays_available() -> None:
    dc = docker_ctl.client()
    all_members = docker_ctl.members(dc)
    addrs = [docker_ctl.addr_of(c) for c in all_members]
    n = len(all_members)
    await wait_views(addrs, n, timeout=300.0)

    client = await casty.connect([docker_ctl.seed_addr(dc)], config=CHAOS_CONFIG)
    workload = Workload(client, [f"lat-{i}" for i in range(50)])
    workload.start()
    try:
        acked_before = workload.total_acked
        for container in all_members:
            docker_ctl.netem_apply(container, NETEM_ALL)
        workload.mark(f"netem [{NETEM_ALL}]")

        # latência sustentada: escreve-se mais devagar, mas escreve-se — e
        # ninguém pode ser declarado morto
        await asyncio.sleep(10.0)
        assert workload.total_acked > acked_before
        await wait_views(addrs, n, timeout=60.0)
    finally:
        for container in all_members:
            docker_ctl.netem_clear(container)
        workload.mark("limpo")
        await workload.stop()

    print(f"\nlatency: {workload.report()}")
    try:
        await verify_counters(client, workload)
    finally:
        await client.close()


async def test_slow_lossy_minority_under_load_does_not_take_cluster_down() -> None:
    dc = docker_ctl.client()
    all_members = docker_ctl.members(dc)
    addrs = [docker_ctl.addr_of(c) for c in all_members]
    n = len(all_members)
    await wait_views(addrs, n, timeout=300.0)

    client = await casty.connect([docker_ctl.seed_addr(dc)], config=CHAOS_CONFIG)
    workload = Workload(client, [f"slow-{i}" for i in range(50)])
    workload.start()
    slow = random.sample(docker_ctl.killable(dc), k=max(1, n // 10))
    try:
        acked_before = workload.total_acked
        for container in slow:
            docker_ctl.netem_apply(container, NETEM_SLOW)
        workload.mark(f"netem em {len(slow)} nós [{NETEM_SLOW}]")

        await asyncio.sleep(10.0)
        assert workload.total_acked > acked_before
    finally:
        for container in slow:
            docker_ctl.netem_clear(container)
        workload.mark("limpo")
        await workload.stop()

    print(f"\nslow_lossy: {workload.report()}")
    try:
        await verify_counters(client, workload)
        # degradação removida: todos voltam a ser vistos como vivos
        await wait_views(addrs, n, timeout=120.0)
    finally:
        await client.close()
