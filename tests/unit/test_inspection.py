"""Spec 11 step 1: events, hub, emission points and local scope, in-process."""

from __future__ import annotations

import asyncio

import pytest

import casty
from casty import inspection
from casty.local import Local

pytestmark = pytest.mark.asyncio


@casty.actor(name="unit.SpyCounter", idle_timeout=0.2)
class SpyCounter:
    value: int = 0

    async def add(self, n: int) -> int:
        self.value += n
        return self.value

    async def boom(self) -> None:
        raise RuntimeError("kaboom")


def _local(interceptor: inspection.Interceptor | None = None) -> Local:
    system = casty.local(interceptor=interceptor)
    assert isinstance(system, Local)
    return system


async def test_events_in_order_with_contiguous_seq() -> None:
    system = _local()
    try:
        async with system.spy(SpyCounter, key="k") as sub:
            counter = system.actor(SpyCounter, "k")
            assert await counter.add(1) == 1
            assert await counter.add(2) == 3
            got: list[inspection.SpyEvent] = []
            while len(got) < 5:
                got.append(await anext(sub))
            assert isinstance(got[0], inspection.Activated)
            assert isinstance(got[1], inspection.Received)
            assert isinstance(got[2], inspection.Completed)
            assert isinstance(got[3], inspection.Received)
            assert isinstance(got[4], inspection.Completed)
            assert [e.seq for e in got] == [1, 2, 3, 4, 5]
            assert got[1].method == "add"
            assert got[2].changed is None  # single-copy: no delta computed
    finally:
        await system.close()


async def test_deactivated_carries_reason() -> None:
    system = _local()
    try:
        async with system.spy(SpyCounter) as sub:
            await system.actor(SpyCounter, "idle-key").add(1)
            while True:
                event = await anext(sub)
                if isinstance(event, inspection.Deactivated):
                    assert event.reason == "idle"
                    break
    finally:
        await system.close()


async def test_failed_carries_directive_and_error() -> None:
    system = _local()
    try:
        async with system.spy(SpyCounter, key="f") as sub:
            with pytest.raises(casty.ActorFailedError):
                await system.actor(SpyCounter, "f").boom()
            while True:
                event = await anext(sub)
                if isinstance(event, inspection.Failed):
                    assert event.method == "boom"
                    assert "kaboom" in event.error
                    assert event.directive == "keep"
                    break
    finally:
        await system.close()


async def test_slow_subscriber_gets_lag_and_actor_is_unaffected() -> None:
    system = _local()
    try:
        hub = system._hub
        sub = hub.register(inspection.Selector(actor="unit.SpyCounter", key="s"))
        counter = system.actor(SpyCounter, "s")
        # never consume: buffer (1024) + hub overflow must fold into `dropped`
        total = 600
        for _ in range(total):
            await counter.add(1)
        assert await counter.add(1) == total + 1  # throughput/result unaffected
        while hub._events:  # let the drain task catch up
            await asyncio.sleep(0)
        first = await anext(sub)
        # 1 Activated + (total+1) * 2 handler events were emitted; buffer holds 1024
        assert isinstance(first, inspection.Lag)
        assert first.dropped == 1 + (total + 1) * 2 - 1024
        hub.unregister(sub)
    finally:
        await system.close()


async def test_interceptor_sees_every_event_and_raises_are_swallowed() -> None:
    seen: list[inspection.SpyEvent] = []

    def hook(event: inspection.SpyEvent) -> None:
        seen.append(event)
        raise RuntimeError("interceptor bug")

    system = _local(interceptor=hook)
    try:
        counter = system.actor(SpyCounter, "i")
        assert await counter.add(1) == 1  # handler unaffected by the raise
        kinds = [type(e) for e in seen]
        assert kinds == [inspection.Activated, inspection.Received, inspection.Completed]
    finally:
        await system.close()


async def test_nothing_armed_emits_nothing() -> None:
    system = _local()
    try:
        host = system._host
        assert not host._observed()
        counter = system.actor(SpyCounter, "quiet")
        await counter.add(1)
        activation = host._activations[("unit.SpyCounter", "quiet")]
        assert activation.seq == 0  # no event was ever constructed
    finally:
        await system.close()


async def test_payloads_arm_globally_and_decode() -> None:
    system = _local()
    try:
        async with system.spy(SpyCounter, key="p", payloads=True) as sub:
            await system.actor(SpyCounter, "p").add(7)
            while True:
                event = await anext(sub)
                if isinstance(event, inspection.Received):
                    assert inspection.decode(event) == [7]
                if isinstance(event, inspection.Completed):
                    assert inspection.decode(event) == 7
                    break
    finally:
        await system.close()


async def test_state_of_local() -> None:
    system = _local()
    try:
        assert await system.state_of(SpyCounter, "st") is None
        await system.actor(SpyCounter, "st").add(5)
        assert await system.state_of(SpyCounter, "st") == {"value": 5}
    finally:
        await system.close()


async def test_selector_filters_by_actor_and_key() -> None:
    system = _local()
    try:
        async with system.spy(SpyCounter, key="match-*") as sub:
            await system.actor(SpyCounter, "other").add(1)
            await system.actor(SpyCounter, "match-1").add(1)
            event = await anext(sub)
            assert isinstance(event, inspection.Activated)
            assert event.key == "match-1"
    finally:
        await system.close()
