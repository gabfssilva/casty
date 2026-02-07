# tests/test_shard_coordinator.py
from __future__ import annotations

import asyncio

from casty import ActorSystem
from casty.cluster_state import NodeAddress
from casty.shard_coordinator_actor import (
    GetShardLocation,
    LeastShardStrategy,
    RegisterRegion,
    SetRole,
    ShardLocation,
    SyncAllocations,
    UpdateTopology,
    shard_coordinator_actor,
)
from casty.replication import ReplicationConfig


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
        coord_ref.tell(RegisterRegion(node=node_a))
        coord_ref.tell(SetRole(is_leader=True, leader_node=node_a))
        coord_ref.tell(SyncAllocations(allocations={}, epoch=0))
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
        coord_ref.tell(RegisterRegion(node=node_a))
        coord_ref.tell(RegisterRegion(node=node_b))
        coord_ref.tell(SetRole(is_leader=True, leader_node=node_a))
        coord_ref.tell(SyncAllocations(allocations={}, epoch=0))
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
    """Adding a node via UpdateTopology + RegisterRegion makes it available for allocation."""
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
        coord_ref.tell(RegisterRegion(node=node_a))
        coord_ref.tell(SetRole(is_leader=True, leader_node=node_a))
        coord_ref.tell(SyncAllocations(allocations={}, epoch=0))
        await asyncio.sleep(0.1)

        # Allocate some shards to node_a
        for shard_id in range(5):
            await system.ask(
                coord_ref,
                lambda r, sid=shard_id: GetShardLocation(shard_id=sid, reply_to=r),
                timeout=2.0,
            )

        # Add node_b (both topology update AND region registration)
        coord_ref.tell(UpdateTopology(available_nodes=frozenset({node_a, node_b})))
        coord_ref.tell(RegisterRegion(node=node_b))
        await asyncio.sleep(0.1)

        # New shard should go to node_b (fewer shards)
        loc = await system.ask(
            coord_ref,
            lambda r: GetShardLocation(shard_id=99, reply_to=r),
            timeout=2.0,
        )
        assert loc.node == node_b


async def test_coordinator_allocates_replicas() -> None:
    """With replication configured, coordinator allocates primary + replicas."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    node_c = NodeAddress(host="10.0.0.3", port=25520)

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b, node_c}),
                replication=ReplicationConfig(replicas=2),
            ),
            "coord",
        )
        coord.tell(RegisterRegion(node=node_a))
        coord.tell(RegisterRegion(node=node_b))
        coord.tell(RegisterRegion(node=node_c))
        coord.tell(SetRole(is_leader=True, leader_node=node_a))
        coord.tell(SyncAllocations(allocations={}, epoch=0))
        await asyncio.sleep(0.1)

        location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )

        assert location.node is not None
        assert len(location.replicas) == 2
        all_nodes = {location.node} | set(location.replicas)
        assert len(all_nodes) == 3  # primary + 2 replicas on different nodes


async def test_coordinator_no_replication_has_empty_replicas() -> None:
    """Without replication config, ShardLocation.replicas is empty."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a}),
            ),
            "coord",
        )
        coord.tell(RegisterRegion(node=node_a))
        coord.tell(SetRole(is_leader=True, leader_node=node_a))
        coord.tell(SyncAllocations(allocations={}, epoch=0))
        await asyncio.sleep(0.1)

        location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        assert location.replicas == ()
