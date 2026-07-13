from __future__ import annotations

import asyncio

import pytest

import casty
from tests.integration.actors import (
    FAST_CONFIG,
    kill_node,
    owner_of,
    start_nodes,
    stop_all,
    wait_view,
)

pytestmark = pytest.mark.asyncio


async def test_barrier_releases_when_all_parties_arrive() -> None:
    systems = await start_nodes(3)
    client: casty.Client | None = None
    try:
        barriers = [system.barrier("sync", parties=4) for system in systems]
        client = await casty.connect([systems[0].member.addr], config=FAST_CONFIG)
        barriers.append(client.barrier("sync", parties=4))

        waiters = [asyncio.create_task(b.wait(timeout=10.0)) for b in barriers[:3]]
        await asyncio.sleep(0.3)
        assert not any(w.done() for w in waiters)  # three arrived, the fourth is missing
        assert await barriers[3].waiting() == 3

        await barriers[3].wait(timeout=10.0)  # the last party releases everyone
        await asyncio.gather(*waiters)
    finally:
        if client is not None:
            await client.close()
        await stop_all(systems)


async def test_barrier_is_cyclic() -> None:
    systems = await start_nodes(3)
    try:
        barrier: casty.Barrier = systems[0].barrier("rounds", parties=2)
        other: casty.Barrier = systems[1].barrier("rounds", parties=2)
        for _ in range(3):
            await asyncio.gather(barrier.wait(timeout=5.0), other.wait(timeout=5.0))
        assert await barrier.waiting() == 0
    finally:
        await stop_all(systems)


async def test_barrier_wait_times_out_and_withdraws() -> None:
    systems = await start_nodes(3)
    try:
        barrier: casty.Barrier = systems[0].barrier("late", parties=2)
        with pytest.raises(casty.CastyTimeoutError):
            await barrier.wait(timeout=0.3)
        assert await barrier.waiting() == 0  # the timed-out arrival was withdrawn

        # the barrier is not left one short: two fresh parties still release
        await asyncio.gather(barrier.wait(timeout=5.0), barrier.wait(timeout=5.0))
    finally:
        await stop_all(systems)


async def test_barrier_survives_owner_death() -> None:
    systems = await start_nodes(3)
    try:
        info = systems[0].barrier("durable", parties=2)._info
        owner = owner_of(systems, info.wire_name, "durable:0")
        survivors = [n for n in systems if n is not owner]

        barrier: casty.Barrier = survivors[0].barrier("durable", parties=2)
        waiter = asyncio.create_task(barrier.wait(timeout=30.0))
        await asyncio.sleep(0.3)
        assert not waiter.done()

        await kill_node(owner)
        await wait_view(survivors, 2)

        # the arrival survived failover: one more party releases the round
        await survivors[1].barrier("durable", parties=2).wait(timeout=30.0)
        await waiter
    finally:
        await stop_all(systems)
