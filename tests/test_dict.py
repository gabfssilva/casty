from __future__ import annotations

import asyncio

from casty import ActorSystem
from casty.cluster.coordinator import (
    shard_coordinator_actor,
    LeastShardStrategy,
)
from casty.cluster.region import shard_region_actor
from casty.cluster.state import NodeAddress
from casty.distributed import Dict
from casty.distributed.dict import map_entity


async def test_dict_put_and_get() -> None:
    """Get missing -> None, put -> old None, get -> value, put again -> old value."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="dmap-test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({self_node}),
            ),
            "coordinator",
        )

        region_ref = system.spawn(
            shard_region_actor(
                self_node=self_node,
                coordinator=coord_ref,
                entity_factory=map_entity,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        dmap: Dict[str, int] = Dict(
            gateway=system, region_ref=region_ref, name="my-map", timeout=5.0
        )

        # Get missing key -> None
        val = await dmap.get("x")
        assert val is None

        # Put new key -> old value is None
        old = await dmap.put("x", 42)
        assert old is None

        # Get existing key -> value
        val = await dmap.get("x")
        assert val == 42

        # Put existing key -> old value returned
        old = await dmap.put("x", 99)
        assert old == 42

        # Confirm new value
        val = await dmap.get("x")
        assert val == 99


async def test_dict_delete_and_contains() -> None:
    """Put, contains -> True, contains missing -> False, delete -> True, contains -> False, delete again -> False."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="dmap-contains-test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({self_node}),
            ),
            "coordinator",
        )

        region_ref = system.spawn(
            shard_region_actor(
                self_node=self_node,
                coordinator=coord_ref,
                entity_factory=map_entity,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        dmap: Dict[str, str] = Dict(
            gateway=system, region_ref=region_ref, name="map", timeout=5.0
        )

        # Put a key
        await dmap.put("key1", "hello")

        # Contains existing key -> True
        assert await dmap.contains("key1") is True

        # Contains missing key -> False
        assert await dmap.contains("missing") is False

        # Delete existing key -> True
        deleted = await dmap.delete("key1")
        assert deleted is True

        # Contains after delete -> False
        assert await dmap.contains("key1") is False

        # Delete again -> False (was already gone)
        deleted = await dmap.delete("key1")
        assert deleted is False

        # Re-use deleted key — entity is re-spawned from scratch
        await dmap.put("key1", "world")
        val = await dmap.get("key1")
        assert val == "world"
