from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

import casty
from casty.errors import QuorumUnavailableError
from tests.integration.actors import partition, start_nodes, stop_all, wait_view

pytestmark = pytest.mark.asyncio


async def test_counter_full_api_across_nodes_and_client() -> None:
    systems = await start_nodes(3)
    client: casty.Client | None = None
    try:
        hits: casty.Counter = systems[0].counter("hits")
        for i in range(10):
            await systems[i % 3].counter("hits").add(i)
        assert await hits.get() == sum(range(10))

        # same counter seen from another system
        hits_b: casty.Counter = systems[1].counter("hits")
        await hits_b.add(5)
        assert await hits.get() == sum(range(10)) + 5

        # and from a lite member
        from tests.integration.actors import FAST_CONFIG

        client = await casty.connect([systems[0].member.addr], config=FAST_CONFIG)
        hits_c: casty.Counter = client.counter("hits")
        await hits_c.add(3)
        assert await hits.get() == sum(range(10)) + 5 + 3

        await hits.reset()
        assert await hits.get() == 0
    finally:
        if client is not None:
            await client.close()
        await stop_all(systems)


async def test_counter_concurrent_adds_sum_correctly() -> None:
    systems = await start_nodes(3)
    try:
        total: casty.Counter = systems[0].counter("concurrent", stripes=8)
        n = 200
        delta = 3
        await asyncio.gather(*(total.add(delta) for _ in range(n)))
        assert await total.get() == n * delta
    finally:
        await stop_all(systems)


async def test_counter_survives_owner_death() -> None:
    systems = await start_nodes(3)
    try:
        total: casty.Counter = systems[0].counter("durable-counter", stripes=4)
        for i in range(12):
            await total.add(i)
        expected = sum(range(12))
        victim = systems[0]
        survivors = systems[1:]
        from tests.integration.actors import kill_node

        await kill_node(victim)
        await wait_view(survivors, 2)
        total_b: casty.Counter = survivors[0].counter("durable-counter", stripes=4)
        deadline = asyncio.get_running_loop().time() + 10.0
        recovered: int | None = None
        while True:
            try:
                recovered = await total_b.get()
                break
            except casty.CastyError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.1)
        assert recovered == expected
    finally:
        await stop_all(systems)


async def test_counter_add_fenced_in_minority() -> None:
    systems = await start_nodes(3)
    heal: Callable[[], None] | None = None
    try:
        total: casty.Counter = systems[0].counter("fenced-counter", stripes=1)
        await total.add(1)
        info = systems[0].counter("fenced-counter", stripes=1)._info
        ring = systems[0]._ring
        assert ring is not None
        owner_id = ring.owner(f"{info.wire_name}/fenced-counter:0")
        owner = next(n for n in systems if n.node_id == owner_id)
        majority = [n for n in systems if n is not owner]

        heal = await partition([owner], majority)
        await wait_view(majority, 2)

        owner_counter: casty.Counter = owner.counter("fenced-counter", stripes=1)
        with pytest.raises(QuorumUnavailableError):
            await owner_counter.add(2)

        majority_counter: casty.Counter = majority[0].counter("fenced-counter", stripes=1)
        await majority_counter.add(3)
        assert await majority_counter.get() == 4
    finally:
        if heal is not None:
            heal()
        await stop_all(systems)


async def test_counter_stripes_distribute_over_nodes() -> None:
    systems = await start_nodes(3)
    try:
        spread: casty.Counter = systems[0].counter("spread", stripes=16)
        for _i in range(16):
            await spread.add(1)
        hosting = [
            sum(1 for (wire, _key) in system._host._activations if "CounterShard" in wire)
            for system in systems
        ]
        assert sum(hosting) > 1
        assert sum(1 for count in hosting if count > 0) > 1, hosting
    finally:
        await stop_all(systems)
