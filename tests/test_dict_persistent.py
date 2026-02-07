from __future__ import annotations

import asyncio

from casty import ActorSystem, InMemoryJournal
from casty.shard_coordinator_actor import LeastShardStrategy, shard_coordinator_actor
from casty.shard_region_actor import shard_region_actor
from casty.cluster_state import NodeAddress
from casty.distributed import Dict
from casty.distributed.dict import persistent_map_entity


async def test_persistent_dict_multinode_recovery() -> None:
    """Node A writes data, shuts down. Node B starts with same journal and reads it."""
    node_a = NodeAddress(host="127.0.0.1", port=25510)
    node_b = NodeAddress(host="127.0.0.1", port=25511)
    journal = InMemoryJournal()

    # Node A: write data
    async with ActorSystem(name="node-a") as system_a:
        entity_factory = persistent_map_entity(journal)
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

        users: Dict[str, str] = Dict(
            system=system_a, region_ref=region_ref, name="users"
        )
        await users.put("alice", "Alice Smith")
        await users.put("bob", "Bob Jones")
        assert await users.get("alice") == "Alice Smith"

    # Node B: different node, same journal — should recover
    async with ActorSystem(name="node-b") as system_b:
        entity_factory = persistent_map_entity(journal)
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

        users: Dict[str, str] = Dict(
            system=system_b, region_ref=region_ref, name="users"
        )
        assert await users.get("alice") == "Alice Smith"
        assert await users.get("bob") == "Bob Jones"


async def test_persistent_dict_delete_survives_recovery() -> None:
    """Delete a key, recover on another node, key should still be gone."""
    node_a = NodeAddress(host="127.0.0.1", port=25510)
    node_b = NodeAddress(host="127.0.0.1", port=25511)
    journal = InMemoryJournal()

    # Node A: put then delete
    async with ActorSystem(name="node-a") as system_a:
        entity_factory = persistent_map_entity(journal)
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

        users: Dict[str, str] = Dict(
            system=system_a, region_ref=region_ref, name="users"
        )
        await users.put("alice", "Alice")
        await users.delete("alice")
        assert await users.contains("alice") is False

    # Node B: alice should still be gone
    async with ActorSystem(name="node-b") as system_b:
        entity_factory = persistent_map_entity(journal)
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

        users: Dict[str, str] = Dict(
            system=system_b, region_ref=region_ref, name="users"
        )
        assert await users.get("alice") is None
        assert await users.contains("alice") is False
