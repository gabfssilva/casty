"""Schedules across failover (spec 04 §10.3 item 7): an eligible actor re-arms
its schedule in the activate hook from replicated state, and ticks resume on
the new owner after the old one is killed."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses

import pytest

import casty
from casty.errors import CastyError
from tests.integration.actors import FAST_CONFIG, kill_node, owner_of, start_nodes, stop_all

pytestmark = pytest.mark.asyncio

CONFIG = dataclasses.replace(FAST_CONFIG, reactivation_retry=0.2)


@casty.actor(name="itest.Ticker", replicas=3, idle_timeout=float("inf"))
class Ticker:
    ticks: int = 0
    interval: float | None = None  # durable decision; the schedule is its transient effect

    @casty.activate
    async def _up(self) -> None:
        if self.interval is not None:
            casty.context().schedule(self._tick, every=self.interval)

    async def start(self, every: float) -> None:
        self.interval = every
        casty.context().schedule(self._tick, every=every)

    async def read(self) -> int:
        return self.ticks

    async def _tick(self) -> None:
        self.ticks += 1


async def read_above(actor: Ticker, floor: int, timeout: float = 15.0) -> int:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        with contextlib.suppress(CastyError):
            ticks = await actor.read()
            if ticks > floor:
                return ticks
        await asyncio.sleep(0.1)
    raise AssertionError(f"ticks never rose above {floor}")


async def test_schedule_resumes_on_the_new_owner_after_kill() -> None:
    nodes = await start_nodes(4, config=CONFIG)
    key = "failover"
    try:
        await nodes[0].actor(Ticker, key).start(0.1)
        before = await read_above(nodes[0].actor(Ticker, key), 0)
        owner = owner_of(nodes, "itest.Ticker", key)
        survivors = [n for n in nodes if n is not owner]
        await kill_node(owner)
        # reactivation re-runs the activate hook, which re-arms from state
        after = await read_above(survivors[0].actor(Ticker, key), before)
        assert after > before
        assert await read_above(survivors[0].actor(Ticker, key), after) > after  # still ticking
    finally:
        await stop_all(nodes)
