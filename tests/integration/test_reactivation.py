"""Failover reactivation on a real cluster: a replicated, infinite-idle actor
that was active when its node died comes back on the new owner without a call."""

from __future__ import annotations

import asyncio
import dataclasses
from collections.abc import Callable

import pytest

import casty
from tests.integration.actors import FAST_CONFIG, kill_node, owner_of, start_nodes, stop_all

pytestmark = pytest.mark.asyncio

CONFIG = dataclasses.replace(FAST_CONFIG, reactivation_retry=0.2)

pump_events: list[str] = []


@casty.actor(name="itest.Pump", replicas=3, idle_timeout=float("inf"))
class Pump:
    value: int = 0

    @casty.activate
    async def _up(self) -> None:
        pump_events.append(f"activate:{casty.context().key}")

    async def add(self, n: int) -> int:
        self.value += n
        return self.value

    async def read(self) -> int:
        return self.value

    async def retire(self) -> None:
        casty.context().deactivate()


async def until(check: Callable[[], bool], timeout: float = 10.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition did not hold in time")


def active_on(nodes: list[casty.Node], key: str) -> casty.Node | None:
    return next((n for n in nodes if n._host.is_active("itest.Pump", key)), None)


async def test_kill_reactivates_on_the_new_owner_without_a_call() -> None:
    nodes = await start_nodes(4, config=CONFIG)
    key = "kill"
    try:
        pump_events.clear()
        assert await nodes[0].actor(Pump, key).add(3) == 3
        owner = owner_of(nodes, "itest.Pump", key)
        survivors = [n for n in nodes if n is not owner]
        await kill_node(owner)
        # the activate hook runs again with no call ever sent
        await until(lambda: pump_events.count(f"activate:{key}") == 2, timeout=15.0)
        assert active_on(survivors, key) is not None
        assert await survivors[0].actor(Pump, key).read() == 3
    finally:
        await stop_all(nodes)


async def test_graceful_stop_hands_the_activity_off() -> None:
    nodes = await start_nodes(4, config=CONFIG)
    key = "drain"
    try:
        assert await nodes[0].actor(Pump, key).add(1) == 1
        owner = owner_of(nodes, "itest.Pump", key)
        survivors = [n for n in nodes if n is not owner]
        await owner.close()
        await until(lambda: active_on(survivors, key) is not None, timeout=15.0)
        assert await survivors[0].actor(Pump, key).read() == 1
    finally:
        await stop_all(nodes)


async def test_deactivate_ends_the_activity_for_good() -> None:
    nodes = await start_nodes(4, config=CONFIG)
    key = "retired"
    try:
        await nodes[0].actor(Pump, key).add(1)
        await nodes[0].actor(Pump, key).retire()
        await until(lambda: active_on(nodes, key) is None)
        bystander = next(
            n for n in nodes if n is not owner_of(nodes, "itest.Pump", key)
        )
        await kill_node(bystander)  # a view change must not resurrect it
        survivors = [n for n in nodes if n is not bystander]
        await asyncio.sleep(2.0)
        assert active_on(survivors, key) is None
    finally:
        await stop_all(nodes)
