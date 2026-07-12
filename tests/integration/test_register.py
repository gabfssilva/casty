from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

import casty
from casty.errors import QuorumUnavailableError
from tests.integration.actors import partition, start_nodes, stop_all, wait_view

pytestmark = pytest.mark.asyncio


@casty.message(name="itest.RegisterSku")
class Sku:
    code: str
    price: float


async def test_register_full_api_across_nodes_and_client() -> None:
    nodes = await start_nodes(3)
    client: casty.Client | None = None
    try:
        ref: casty.Register[Sku] = nodes[0].register("leader")
        assert await ref.get() is None

        await ref.set(Sku(code="sku-1", price=1.0))
        assert await ref.get() == Sku(code="sku-1", price=1.0)

        # same register seen from another node
        ref_b: casty.Register[Sku] = nodes[1].register("leader")
        assert await ref_b.get() == Sku(code="sku-1", price=1.0)

        old = await ref_b.get_and_set(Sku(code="sku-2", price=2.0))
        assert old == Sku(code="sku-1", price=1.0)
        assert await ref.get() == Sku(code="sku-2", price=2.0)

        # and from a lite member
        from tests.integration.actors import FAST_CONFIG

        client = await casty.connect([nodes[0].member.addr], config=FAST_CONFIG)
        ref_c: casty.Register[Sku] = client.register("leader")
        assert await ref_c.get() == Sku(code="sku-2", price=2.0)
        await ref_c.set(Sku(code="sku-3", price=3.0))
        assert await ref.get() == Sku(code="sku-3", price=3.0)
    finally:
        if client is not None:
            await client.close()
        await stop_all(nodes)


async def test_register_compare_and_set() -> None:
    nodes = await start_nodes(3)
    try:
        ref: casty.Register[int] = nodes[0].register("cas")
        assert await ref.get() is None

        # expected=None matches the initial empty value
        assert await ref.compare_and_set(None, 1)
        assert await ref.get() == 1

        # mismatch fails and leaves the value untouched
        assert not await ref.compare_and_set(99, 2)
        assert await ref.get() == 1

        # match succeeds
        assert await ref.compare_and_set(1, 2)
        assert await ref.get() == 2
    finally:
        await stop_all(nodes)


async def test_register_compare_and_set_contention() -> None:
    nodes = await start_nodes(3)
    try:
        ref: casty.Register[int] = nodes[0].register("contended")
        await ref.set(0)

        results = await asyncio.gather(
            *(ref.compare_and_set(0, i) for i in range(1, 6))
        )
        assert sum(1 for r in results if r) == 1

        final = await ref.get()
        assert final is not None and 1 <= final <= 5
    finally:
        await stop_all(nodes)


async def test_register_survives_owner_death() -> None:
    nodes = await start_nodes(3)
    try:
        ref: casty.Register[int] = nodes[0].register("durable-register")
        await ref.set(42)
        victim = nodes[0]
        survivors = nodes[1:]
        from tests.integration.actors import kill_node

        await kill_node(victim)
        await wait_view(survivors, 2)
        ref_b: casty.Register[int] = survivors[0].register("durable-register")
        deadline = asyncio.get_running_loop().time() + 10.0
        recovered: int | None = None
        while True:
            try:
                recovered = await ref_b.get()
                break
            except casty.CastyError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.1)
        assert recovered == 42
    finally:
        await stop_all(nodes)


async def test_register_set_fenced_in_minority() -> None:
    nodes = await start_nodes(3)
    heal: Callable[[], None] | None = None
    try:
        ref: casty.Register[int] = nodes[0].register("fenced-register")
        await ref.set(1)
        info = nodes[0].register("fenced-register")._info
        ring = nodes[0]._ring
        assert ring is not None
        owner_id = ring.owner(f"{info.wire_name}/fenced-register:0")
        owner = next(n for n in nodes if n.node_id == owner_id)
        majority = [n for n in nodes if n is not owner]

        heal = await partition([owner], majority)
        await wait_view(majority, 2)

        owner_ref: casty.Register[int] = owner.register("fenced-register")
        with pytest.raises(QuorumUnavailableError):
            await owner_ref.set(2)

        majority_ref: casty.Register[int] = majority[0].register("fenced-register")
        await majority_ref.set(3)
        assert await majority_ref.get() == 3
    finally:
        if heal is not None:
            heal()
        await stop_all(nodes)
