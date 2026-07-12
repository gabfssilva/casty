from __future__ import annotations

import asyncio
from collections.abc import Callable

import pytest

import casty
from casty.errors import QuorumUnavailableError
from tests.integration.actors import partition, start_nodes, stop_all, wait_view

pytestmark = pytest.mark.asyncio


@casty.message(name="itest.MultiMapTag")
class Tag:
    label: str
    weight: float


async def test_multimap_full_api_across_nodes_and_client() -> None:
    nodes = await start_nodes(3)
    client: casty.Client | None = None
    try:
        groups: casty.MultiMap[str, int] = nodes[0].multimap("groups")
        assert await groups.put("evens", 2)
        assert await groups.put("evens", 4)
        assert await groups.put("odds", 1)
        assert not await groups.put("evens", 2)

        assert sorted(await groups.get("evens")) == [2, 4]
        assert await groups.get("missing") == []
        assert await groups.contains("evens", 2)
        assert not await groups.contains("evens", 3)

        assert await groups.remove("evens", 2)
        assert not await groups.remove("evens", 2)
        assert sorted(await groups.get("evens")) == [4]

        assert await groups.size() == 2

        assert await groups.remove_key("evens") == 1
        assert await groups.get("evens") == []
        assert await groups.size() == 1

        # same multimap seen from another node
        groups_b: casty.MultiMap[str, int] = nodes[1].multimap("groups")
        assert await groups_b.get("odds") == [1]

        # and from a lite member
        from tests.integration.actors import FAST_CONFIG

        client = await casty.connect([nodes[0].member.addr], config=FAST_CONFIG)
        groups_c: casty.MultiMap[str, int] = client.multimap("groups")
        assert await groups_c.put("odds", 3)
        assert sorted(await groups.get("odds")) == [1, 3]

        await groups.clear()
        assert await groups.size() == 0
    finally:
        if client is not None:
            await client.close()
        await stop_all(nodes)


async def test_multimap_duplicate_put_does_not_inflate_size() -> None:
    nodes = await start_nodes(3)
    try:
        mm: casty.MultiMap[str, str] = nodes[0].multimap("dup")
        assert await mm.put("k", "v")
        assert not await mm.put("k", "v")
        assert not await mm.put("k", "v")
        assert await mm.size() == 1
    finally:
        await stop_all(nodes)


async def test_multimap_message_values_and_distribution() -> None:
    nodes = await start_nodes(3)
    try:
        catalog: casty.MultiMap[str, Tag] = nodes[0].multimap("catalog", shards=8)
        for i in range(16):
            await catalog.put(f"item-{i}", Tag(label=f"tag-{i}", weight=float(i)))
            await catalog.put(f"item-{i}", Tag(label=f"tag-{i}-extra", weight=float(i) + 0.5))
        loaded = sorted(tag.label for tag in await catalog.get("item-3"))
        assert loaded == ["tag-3", "tag-3-extra"]
        # shards spread over more than one node
        hosting = [
            sum(1 for (wire, _key) in node._host._activations if "MultiMapShard" in wire)
            for node in nodes
        ]
        assert sum(hosting) > 1
        assert sum(1 for count in hosting if count > 0) > 1, hosting
    finally:
        await stop_all(nodes)


async def test_multimap_survives_owner_death() -> None:
    nodes = await start_nodes(3)
    try:
        mm: casty.MultiMap[str, int] = nodes[0].multimap("durable-multimap", shards=4)
        for i in range(12):
            await mm.put(f"k{i}", i)
            await mm.put(f"k{i}", i * 100 + 1000)
        victim = nodes[0]
        survivors = nodes[1:]
        from tests.integration.actors import kill_node

        await kill_node(victim)
        await wait_view(survivors, 2)
        mm_b: casty.MultiMap[str, int] = survivors[0].multimap("durable-multimap", shards=4)
        deadline = asyncio.get_running_loop().time() + 10.0
        recovered: dict[str, list[int]] = {}
        while True:
            try:
                recovered = {f"k{i}": sorted(await mm_b.get(f"k{i}")) for i in range(12)}
                break
            except casty.CastyError:
                if asyncio.get_running_loop().time() > deadline:
                    raise
                await asyncio.sleep(0.1)
        assert recovered == {f"k{i}": sorted([i, i * 100 + 1000]) for i in range(12)}
    finally:
        await stop_all(nodes)


async def test_multimap_put_fenced_in_minority() -> None:
    nodes = await start_nodes(3)
    heal: Callable[[], None] | None = None
    try:
        mm: casty.MultiMap[str, int] = nodes[0].multimap("fenced-multimap", shards=1)
        await mm.put("k", 1)
        info = nodes[0].multimap("fenced-multimap", shards=1)._info
        ring = nodes[0]._ring
        assert ring is not None
        owner_id = ring.owner(f"{info.wire_name}/fenced-multimap:0")
        owner = next(n for n in nodes if n.node_id == owner_id)
        majority = [n for n in nodes if n is not owner]

        heal = await partition([owner], majority)
        await wait_view(majority, 2)

        owner_mm: casty.MultiMap[str, int] = owner.multimap("fenced-multimap", shards=1)
        with pytest.raises(QuorumUnavailableError):
            await owner_mm.put("k", 2)

        majority_mm: casty.MultiMap[str, int] = majority[0].multimap("fenced-multimap", shards=1)
        await majority_mm.put("k", 3)
        assert sorted(await majority_mm.get("k")) == [1, 3]
    finally:
        if heal is not None:
            heal()
        await stop_all(nodes)
