from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

import casty
from casty.errors import QuorumUnavailableError
from tests.integration.actors import partition, start_nodes, stop_all, wait_view

pytestmark = pytest.mark.asyncio


@casty.message(name="itest.Sku")
class Sku:
    code: str
    price: float


async def test_map_full_api_across_nodes_and_client() -> None:
    nodes = await start_nodes(3)
    client: casty.Client | None = None
    try:
        stock: casty.Map[str, int] = nodes[0].map("stock")
        for i in range(40):
            await stock.put(f"sku-{i}", i)
        assert await stock.size() == 40
        assert await stock.get("sku-7") == 7
        assert await stock.get("missing") is None
        assert await stock.contains("sku-7")
        assert await stock.remove("sku-7")
        assert not await stock.remove("sku-7")
        assert not await stock.contains("sku-7")
        assert await stock.size() == 39

        # same map seen from another node
        stock_b: casty.Map[str, int] = nodes[1].map("stock")
        assert await stock_b.get("sku-8") == 8

        # and from a lite member
        from tests.integration.actors import FAST_CONFIG

        client = await casty.connect([nodes[0].member.addr], config=FAST_CONFIG)
        stock_c: casty.Map[str, int] = client.map("stock")
        assert await stock_c.get("sku-9") == 9
        await stock_c.put("sku-9", 90)
        assert await stock.get("sku-9") == 90

        items = dict(await stock.items())
        assert items["sku-9"] == 90 and len(items) == 39

        await stock.clear()
        assert await stock.size() == 0
    finally:
        if client is not None:
            await client.close()
        await stop_all(nodes)


async def test_map_message_values_and_distribution() -> None:
    nodes = await start_nodes(3)
    try:
        catalog: casty.Map[str, Sku] = nodes[0].map("catalog", shards=8)
        for i in range(16):
            await catalog.put(f"sku-{i}", Sku(code=f"sku-{i}", price=float(i)))
        loaded = await catalog.get("sku-3")
        assert loaded == Sku(code="sku-3", price=3.0)
        # shards spread over more than one node
        hosting = [
            sum(1 for (wire, _key) in node._host._activations if "MapShard" in wire)
            for node in nodes
        ]
        assert sum(hosting) > 1
        assert sum(1 for count in hosting if count > 0) > 1, hosting
    finally:
        await stop_all(nodes)


async def test_map_survives_owner_death() -> None:
    nodes = await start_nodes(3)
    try:
        stock: casty.Map[str, int] = nodes[0].map("durable-map", shards=4)
        for i in range(12):
            await stock.put(f"k{i}", i)
        victim = nodes[0]
        survivors = nodes[1:]
        from tests.integration.actors import kill_node

        await kill_node(victim)
        await wait_view(survivors, 2)
        stock_b: casty.Map[str, int] = survivors[0].map("durable-map", shards=4)
        deadline = asyncio.get_running_loop().time() + 10.0
        recovered: dict[str, int | None] = {}
        while True:
            try:
                recovered = {f"k{i}": await stock_b.get(f"k{i}") for i in range(12)}
                break
            except casty.CastyError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.1)
        assert recovered == {f"k{i}": i for i in range(12)}
    finally:
        await stop_all(nodes)


async def test_map_put_fenced_in_minority() -> None:
    nodes = await start_nodes(3)
    heal: Callable[[], None] | None = None
    try:
        stock: casty.Map[str, int] = nodes[0].map("fenced-map", shards=1)
        await stock.put("k", 1)
        info = nodes[0].map("fenced-map", shards=1)._info
        ring = nodes[0]._ring
        assert ring is not None
        owner_id = ring.owner(f"{info.wire_name}/fenced-map:0")
        owner = next(n for n in nodes if n.node_id == owner_id)
        majority = [n for n in nodes if n is not owner]

        heal = await partition([owner], majority)
        await wait_view(majority, 2)

        owner_map: casty.Map[str, int] = owner.map("fenced-map", shards=1)
        with pytest.raises(QuorumUnavailableError):
            await owner_map.put("k", 2)

        majority_map: casty.Map[str, int] = majority[0].map("fenced-map", shards=1)
        await majority_map.put("k", 3)
        assert await majority_map.get("k") == 3
    finally:
        if heal is not None:
            heal()
        await stop_all(nodes)
