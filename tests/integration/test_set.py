from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

import casty
from casty.errors import QuorumUnavailableError
from tests.integration.actors import partition, start_nodes, stop_all, wait_view

pytestmark = pytest.mark.asyncio


@casty.message(name="itest.Tag")
class Tag:
    label: str
    weight: float


async def test_set_full_api_across_nodes_and_client() -> None:
    nodes = await start_nodes(3)
    client: casty.Client | None = None
    try:
        colors: casty.Set[str] = nodes[0].set("colors")
        for i in range(40):
            await colors.add(f"color-{i}")
        assert await colors.size() == 40
        assert await colors.contains("color-7")
        assert not await colors.contains("missing")
        assert await colors.add("color-7") is False
        assert await colors.remove("color-7")
        assert not await colors.remove("color-7")
        assert not await colors.contains("color-7")
        assert await colors.size() == 39

        # same set seen from another node
        colors_b: casty.Set[str] = nodes[1].set("colors")
        assert await colors_b.contains("color-8")

        # and from a lite member
        from tests.integration.actors import FAST_CONFIG

        client = await casty.connect([nodes[0].member.addr], config=FAST_CONFIG)
        colors_c: casty.Set[str] = client.set("colors")
        assert await colors_c.contains("color-9")
        assert await colors_c.add("color-90")
        assert await colors.contains("color-90")

        items = set(await colors.items())
        assert "color-90" in items and len(items) == 40

        await colors.clear()
        assert await colors.size() == 0
    finally:
        if client is not None:
            await client.close()
        await stop_all(nodes)


async def test_set_message_values_and_distribution() -> None:
    nodes = await start_nodes(3)
    try:
        tags: casty.Set[Tag] = nodes[0].set("tags", shards=8)
        for i in range(16):
            await tags.add(Tag(label=f"tag-{i}", weight=float(i)))
        assert await tags.contains(Tag(label="tag-3", weight=3.0))
        # shards spread over more than one node
        hosting = [
            sum(1 for (wire, _key) in node._host._activations if "SetShard" in wire)
            for node in nodes
        ]
        assert sum(hosting) > 1
        assert sum(1 for count in hosting if count > 0) > 1, hosting
    finally:
        await stop_all(nodes)


async def test_set_survives_owner_death() -> None:
    nodes = await start_nodes(3)
    try:
        colors: casty.Set[str] = nodes[0].set("durable-set", shards=4)
        for i in range(12):
            await colors.add(f"c{i}")
        victim = nodes[0]
        survivors = nodes[1:]
        from tests.integration.actors import kill_node

        await kill_node(victim)
        await wait_view(survivors, 2)
        colors_b: casty.Set[str] = survivors[0].set("durable-set", shards=4)
        deadline = asyncio.get_running_loop().time() + 10.0
        recovered: dict[str, bool] = {}
        while True:
            try:
                recovered = {f"c{i}": await colors_b.contains(f"c{i}") for i in range(12)}
                break
            except casty.CastyError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.1)
        assert recovered == {f"c{i}": True for i in range(12)}
    finally:
        await stop_all(nodes)


async def test_set_add_fenced_in_minority() -> None:
    nodes = await start_nodes(3)
    heal: Callable[[], None] | None = None
    try:
        colors: casty.Set[str] = nodes[0].set("fenced-set", shards=1)
        await colors.add("c")
        info = nodes[0].set("fenced-set", shards=1)._info
        ring = nodes[0]._ring
        assert ring is not None
        owner_id = ring.owner(f"{info.wire_name}/fenced-set:0")
        owner = next(n for n in nodes if n.node_id == owner_id)
        majority = [n for n in nodes if n is not owner]

        heal = await partition([owner], majority)
        await wait_view(majority, 2)

        owner_set: casty.Set[str] = owner.set("fenced-set", shards=1)
        with pytest.raises(QuorumUnavailableError):
            await owner_set.add("d")

        majority_set: casty.Set[str] = majority[0].set("fenced-set", shards=1)
        await majority_set.add("d")
        assert await majority_set.contains("d")
    finally:
        if heal is not None:
            heal()
        await stop_all(nodes)


async def test_set_algebra() -> None:
    nodes = await start_nodes(3)
    try:
        a: casty.Set[int] = nodes[0].set("set-a")
        b: casty.Set[int] = nodes[0].set("set-b")
        for i in range(10):
            await a.add(i)
        for i in range(5, 15):
            await b.add(i)

        assert await a.union(b) == set(range(15))
        assert await a.intersection(b) == set(range(5, 10))
        assert await a.difference(b) == set(range(5))
    finally:
        await stop_all(nodes)
