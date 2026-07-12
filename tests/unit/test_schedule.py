from __future__ import annotations

import asyncio

import pytest

import casty
from casty.actors.registry import info_of
from casty.errors import ReentrancyError
from tests.unit.cluster import Cluster

pytestmark = pytest.mark.asyncio


@casty.actor(name="ltest.Ticker", idle_timeout=float("inf"))
class Ticker:
    ticks: int = 0
    slow: float = 0.0

    async def start(self, every: float) -> None:
        casty.context().schedule(self._tick, every=every)

    async def start_once(self, after: float, label: str) -> None:
        casty.context().schedule(self._tick, after=after, name=label)

    async def read(self) -> int:
        return self.ticks

    async def _tick(self) -> None:
        self.ticks += 1
        if self.slow:
            await asyncio.sleep(self.slow)


@casty.actor(name="ltest.SlowTicker", idle_timeout=float("inf"))
class SlowTicker:
    ticks: int = 0

    async def start(self) -> None:
        casty.context().schedule(self._tick, every=0.05)

    async def read(self) -> int:
        return self.ticks

    async def _tick(self) -> None:
        self.ticks += 1
        await asyncio.sleep(0.2)  # handler slower than the period


@casty.actor(name="ltest.CancelTicker", idle_timeout=float("inf"))
class CancelTicker:
    ticks: int = 0
    handle: casty.Schedule | None = casty.transient()

    async def start(self) -> None:
        self.handle = casty.context().schedule(self._tick, every=0.05)

    async def cancel(self) -> None:
        assert self.handle is not None
        self.handle.cancel()
        self.handle.cancel()  # idempotent

    async def restart(self, bump: int) -> None:
        # same default name (_tick): replaces
        casty.context().schedule(self._tick, bump, every=0.05)

    async def read(self) -> int:
        return self.ticks

    async def _tick(self, bump: int = 1) -> None:
        self.ticks += bump


@casty.actor(name="ltest.StopTicker", idle_timeout=float("inf"), supervisor=lambda *_: casty.STOP)
class StopTicker:
    deactivated: bool = False

    async def start(self) -> None:
        casty.context().schedule(self._tick, every=0.05)

    async def _tick(self) -> None:
        raise RuntimeError("tick kaboom")


@casty.actor(name="ltest.IdleTicker", idle_timeout=0.2)
class IdleTicker:
    activations: int = 0

    @casty.activate
    async def _up(self) -> None:
        self.activations += 1

    async def start(self) -> None:
        casty.context().schedule(self._tick, every=10.0)  # armed but never fires in time

    async def touch(self) -> int:
        return self.activations

    async def _tick(self) -> None:
        pass


@casty.actor(name="ltest.SelfCall", idle_timeout=float("inf"))
class SelfCall:
    error: str = ""

    async def start(self) -> None:
        casty.context().schedule(self._tick, after=0.01)

    async def read(self) -> str:
        return self.error

    async def _tick(self) -> None:
        try:
            await casty.context().actor(SelfCall, casty.context().key).read()
        except ReentrancyError:
            self.error = "reentrancy"


@casty.actor(name="unit.STicker", replicas=3, idle_timeout=float("inf"))
class STicker:
    value: int = 0

    async def start(self) -> None:
        casty.context().schedule(self._tick, every=0.03)

    async def read(self) -> int:
        return self.value

    async def _tick(self) -> None:
        self.value += 1


STICKER = info_of(STicker)
assert STICKER is not None


async def test_periodic_tick_mutates_state() -> None:
    async with casty.local() as system:
        actor = system.actor(Ticker, "periodic")
        await actor.start(0.05)
        await asyncio.sleep(0.3)
        assert await actor.read() >= 3


async def test_fixed_delay_no_pileup() -> None:
    async with casty.local() as system:
        actor = system.actor(SlowTicker, "slow")
        await actor.start()
        await asyncio.sleep(0.6)
        # period 0.05 + handler 0.2 => at most ~3 ticks in 0.6s; pile-up would give ~12
        assert await actor.read() <= 4


async def test_one_shot_fires_once_and_respects_after() -> None:
    async with casty.local() as system:
        actor = system.actor(Ticker, "oneshot")
        await actor.start_once(0.2, "later")
        await asyncio.sleep(0.05)
        assert await actor.read() == 0  # `after` not elapsed yet
        await asyncio.sleep(0.4)
        assert await actor.read() == 1  # fired exactly once


async def test_cancel_and_replace() -> None:
    async with casty.local() as system:
        actor = system.actor(CancelTicker, "cancel")
        await actor.start()
        await asyncio.sleep(0.12)
        await actor.cancel()
        ticks = await actor.read()
        await asyncio.sleep(0.15)
        assert await actor.read() == ticks  # cancelled: no further ticks

        await actor.restart(100)
        await actor.restart(1000)  # same name: replaces, no double ticking
        await asyncio.sleep(0.12)
        delta = await actor.read() - ticks
        assert delta >= 1000
        assert delta % 1000 == 0  # only the 1000-bump schedule is live


async def test_raising_tick_goes_through_supervisor() -> None:
    async with casty.local() as system:
        actor = system.actor(StopTicker, "boom")
        await actor.start()
        await asyncio.sleep(0.2)
        assert not system._host.is_active("ltest.StopTicker", "boom")  # STOP ended it


async def test_idle_deactivation_kills_schedule() -> None:
    async with casty.local() as system:
        actor = system.actor(IdleTicker, "idle")
        await actor.start()
        await asyncio.sleep(0.5)  # > idle_timeout; schedule (every=10) never blocked it
        assert await actor.touch() == 1  # fresh activation, schedule gone


async def test_tick_self_proxy_is_reentrancy() -> None:
    async with casty.local() as system:
        actor = system.actor(SelfCall, "self")
        await actor.start()
        await asyncio.sleep(0.2)
        assert await actor.read() == "reentrancy"


@casty.actor(name="ltest.Emitter", idle_timeout=float("inf"))
class Emitter:
    log: list[str] = casty.transient(factory=list)

    @casty.activate
    async def _up(self) -> None:
        self.log = []

    async def run(self) -> None:
        casty.context().emit(self._second, "b")
        self.log.append("a")  # rest of the current handler runs before the emit

    async def retire_then_emit(self) -> None:
        casty.context().deactivate()
        casty.context().emit(self._second, "dropped")

    async def read(self) -> list[str]:
        return self.log

    async def _second(self, tag: str) -> None:
        self.log.append(tag)


async def test_emit_runs_after_the_current_handler() -> None:
    async with casty.local() as system:
        actor = system.actor(Emitter, "order")
        await actor.run()
        await asyncio.sleep(0.05)
        assert await actor.read() == ["a", "b"]


async def test_emit_is_dropped_when_the_activation_closes() -> None:
    async with casty.local() as system:
        actor = system.actor(Emitter, "closing")
        await actor.retire_then_emit()
        await asyncio.sleep(0.05)
        assert await actor.read() == []  # fresh activation: the emit died with the old one


async def test_replicated_tick_commits() -> None:
    cluster = Cluster(5)
    owner = cluster.owner(STICKER, "k")
    await owner.call(STICKER, "k", "start")
    deadline = asyncio.get_running_loop().time() + 2.0
    while asyncio.get_running_loop().time() < deadline:
        if await owner.call(STICKER, "k", "read") >= 2:
            break
        await asyncio.sleep(0.02)
    for node in cluster.replicas(STICKER, "k"):
        assert "value" in node.pages(STICKER, "k")  # every tick replicated


async def test_tick_rolls_back_under_unavailable_quorum_and_keeps_running() -> None:
    cluster = Cluster(5)
    owner = cluster.owner(STICKER, "k")
    await owner.call(STICKER, "k", "start")
    deadline = asyncio.get_running_loop().time() + 2.0
    while asyncio.get_running_loop().time() < deadline:
        if await owner.call(STICKER, "k", "read") >= 1:
            break
        await asyncio.sleep(0.02)
    for node in cluster.replicas(STICKER, "k"):
        if node is not owner:
            cluster.down.add(node.node_id)
    committed = await owner.call(STICKER, "k", "read")
    await asyncio.sleep(0.2)  # several tick attempts, each rolled back
    assert await owner.call(STICKER, "k", "read") == committed
    cluster.down.clear()
    deadline = asyncio.get_running_loop().time() + 2.0
    while asyncio.get_running_loop().time() < deadline:
        if await owner.call(STICKER, "k", "read") > committed:
            return
        await asyncio.sleep(0.02)
    raise AssertionError("schedule did not resume after the quorum healed")
