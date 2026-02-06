from __future__ import annotations

import asyncio

from casty import ActorSystem
from casty._shard_coordinator_actor import (
    shard_coordinator_actor,
    LeastShardStrategy,
)
from casty._shard_region_actor import shard_region_actor
from casty.cluster_state import NodeAddress
from casty.distributed import Set
from casty.distributed._set import set_entity


async def test_set_add_contains_remove() -> None:
    """Empty size=0, contains=False, add=True, add again=False, contains=True,
    size=1, add more, size=3, remove=True, remove again=False, size=2."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="dset-test") as system:
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
                entity_factory=set_entity,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        s: Set[str] = Set(
            system=system, region_ref=region_ref, name="my-set", timeout=2.0
        )

        # Empty set: size 0, contains returns False
        assert await s.size() == 0
        assert await s.contains("a") is False

        # Add "a" -> True (was not present)
        assert await s.add("a") is True

        # Add "a" again -> False (already present)
        assert await s.add("a") is False

        # Contains "a" -> True
        assert await s.contains("a") is True

        # Size -> 1
        assert await s.size() == 1

        # Add more elements
        await s.add("b")
        await s.add("c")

        # Size -> 3
        assert await s.size() == 3

        # Remove "b" -> True (was present)
        assert await s.remove("b") is True

        # Remove "b" again -> False (no longer present)
        assert await s.remove("b") is False

        # Size -> 2
        assert await s.size() == 2
