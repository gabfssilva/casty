from __future__ import annotations

import asyncio

from casty import ActorSystem, InMemoryJournal
from casty.cluster.coordinator import LeastShardStrategy, shard_coordinator_actor
from casty.cluster.region import shard_region_actor
from casty.cluster.state import NodeAddress
from casty.distributed import Set
from casty.distributed.set import persistent_set_entity


async def test_persistent_set_multinode_recovery() -> None:
    """Node A adds items, shuts down. Node B starts with same journal and sees them."""
    node_a = NodeAddress(host="127.0.0.1", port=25510)
    node_b = NodeAddress(host="127.0.0.1", port=25511)
    journal = InMemoryJournal()

    # Node A: add items
    async with ActorSystem(name="node-a") as system_a:
        entity_factory = persistent_set_entity(journal)
        coord_ref = system_a.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a}),
            ),
            "coordinator",
        )
        region_ref = system_a.spawn(
            shard_region_actor(
                self_node=node_a,
                coordinator=coord_ref,
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        sessions: Set[str] = Set(
            system=system_a, region_ref=region_ref, name="sessions"
        )
        await sessions.add("sess-1")
        await sessions.add("sess-2")
        await sessions.add("sess-3")
        assert await sessions.size() == 3

    # Node B: different node, same journal — should recover
    async with ActorSystem(name="node-b") as system_b:
        entity_factory = persistent_set_entity(journal)
        coord_ref = system_b.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_b}),
            ),
            "coordinator",
        )
        region_ref = system_b.spawn(
            shard_region_actor(
                self_node=node_b,
                coordinator=coord_ref,
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        sessions: Set[str] = Set(
            system=system_b, region_ref=region_ref, name="sessions"
        )
        assert await sessions.contains("sess-1") is True
        assert await sessions.contains("sess-2") is True
        assert await sessions.contains("sess-3") is True
        assert await sessions.size() == 3


async def test_persistent_set_remove_survives_recovery() -> None:
    """Remove items on Node A, verify they're gone on Node B."""
    node_a = NodeAddress(host="127.0.0.1", port=25510)
    node_b = NodeAddress(host="127.0.0.1", port=25511)
    journal = InMemoryJournal()

    # Node A: add 3, remove 1
    async with ActorSystem(name="node-a") as system_a:
        entity_factory = persistent_set_entity(journal)
        coord_ref = system_a.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a}),
            ),
            "coordinator",
        )
        region_ref = system_a.spawn(
            shard_region_actor(
                self_node=node_a,
                coordinator=coord_ref,
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        tags: Set[str] = Set(system=system_a, region_ref=region_ref, name="tags")
        await tags.add("python")
        await tags.add("rust")
        await tags.add("go")
        await tags.remove("rust")
        assert await tags.size() == 2

    # Node B: rust should still be gone
    async with ActorSystem(name="node-b") as system_b:
        entity_factory = persistent_set_entity(journal)
        coord_ref = system_b.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_b}),
            ),
            "coordinator",
        )
        region_ref = system_b.spawn(
            shard_region_actor(
                self_node=node_b,
                coordinator=coord_ref,
                entity_factory=entity_factory,
                num_shards=10,
            ),
            "shard-region",
        )
        await asyncio.sleep(0.1)

        tags: Set[str] = Set(system=system_b, region_ref=region_ref, name="tags")
        assert await tags.contains("python") is True
        assert await tags.contains("go") is True
        assert await tags.contains("rust") is False
        assert await tags.size() == 2
