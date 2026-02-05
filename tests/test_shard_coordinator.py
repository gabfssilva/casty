# tests/test_shard_coordinator.py
from __future__ import annotations

import asyncio

from casty import ActorSystem
from casty.cluster_state import NodeAddress
from casty._shard_coordinator_actor import (
    shard_coordinator_actor,
    GetShardLocation,
    ShardLocation,
    UpdateTopology,
    LeastShardStrategy,
)


async def test_coordinator_allocates_shard_to_node() -> None:
    """Requesting a shard location allocates it to the available node."""
    node_a = NodeAddress(host="127.0.0.1", port=25520)

    async with ActorSystem(name="test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a}),
            ),
            "coordinator",
        )
        await asyncio.sleep(0.1)

        location: ShardLocation = await system.ask(
            coord_ref,
            lambda r: GetShardLocation(shard_id=42, reply_to=r),
            timeout=2.0,
        )
        assert location.shard_id == 42
        assert location.node == node_a


async def test_coordinator_distributes_evenly() -> None:
    """LeastShardStrategy distributes shards evenly across nodes."""
    node_a = NodeAddress(host="127.0.0.1", port=25520)
    node_b = NodeAddress(host="127.0.0.2", port=25520)

    async with ActorSystem(name="test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b}),
            ),
            "coordinator",
        )
        await asyncio.sleep(0.1)

        locations: list[ShardLocation] = []
        for shard_id in range(10):
            loc = await system.ask(
                coord_ref,
                lambda r, sid=shard_id: GetShardLocation(shard_id=sid, reply_to=r),
                timeout=2.0,
            )
            locations.append(loc)

        nodes_used = {loc.node for loc in locations}
        assert len(nodes_used) == 2  # Both nodes got shards

        counts = {node_a: 0, node_b: 0}
        for loc in locations:
            counts[loc.node] += 1
        assert abs(counts[node_a] - counts[node_b]) <= 1  # Balanced


async def test_coordinator_topology_update() -> None:
    """Adding a node via UpdateTopology makes it available for allocation."""
    node_a = NodeAddress(host="127.0.0.1", port=25520)
    node_b = NodeAddress(host="127.0.0.2", port=25520)

    async with ActorSystem(name="test") as system:
        coord_ref = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a}),
            ),
            "coordinator",
        )
        await asyncio.sleep(0.1)

        # Allocate some shards to node_a
        for shard_id in range(5):
            await system.ask(
                coord_ref,
                lambda r, sid=shard_id: GetShardLocation(shard_id=sid, reply_to=r),
                timeout=2.0,
            )

        # Add node_b
        coord_ref.tell(UpdateTopology(available_nodes=frozenset({node_a, node_b})))
        await asyncio.sleep(0.1)

        # New shard should go to node_b (fewer shards)
        loc = await system.ask(
            coord_ref,
            lambda r: GetShardLocation(shard_id=99, reply_to=r),
            timeout=2.0,
        )
        assert loc.node == node_b
