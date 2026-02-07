# tests/test_coordinator_failover.py
from __future__ import annotations

import asyncio

from casty import ActorSystem
from casty.shard_coordinator_actor import (
    shard_coordinator_actor,
    GetShardLocation,
    LeastShardStrategy,
    NodeDown,
)
from casty.cluster_state import NodeAddress
from casty.replication import ReplicationConfig


async def test_coordinator_promotes_replica_on_node_down() -> None:
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
        await asyncio.sleep(0.1)

        location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        original_primary = location.node
        original_replicas = location.replicas

        # Simulate primary going down
        coord.tell(NodeDown(node=original_primary))
        await asyncio.sleep(0.1)

        # Re-query — should get a different primary (first replica)
        new_location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        assert new_location.node != original_primary
        assert new_location.node in original_replicas


async def test_coordinator_removes_failed_replica() -> None:
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
        await asyncio.sleep(0.1)

        location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        primary = location.node
        # Pick a replica to fail
        failed_replica = location.replicas[0]

        coord.tell(NodeDown(node=failed_replica))
        await asyncio.sleep(0.1)

        new_location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        # Primary should stay the same
        assert new_location.node == primary
        # Failed replica should be removed
        assert failed_replica not in new_location.replicas


async def test_coordinator_removes_shard_when_no_replicas() -> None:
    """When primary fails and there are no replicas, shard is deallocated."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b}),
                # No replication
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        original_primary = location.node

        coord.tell(NodeDown(node=original_primary))
        await asyncio.sleep(0.1)

        # Re-query — shard should be reallocated to remaining node
        new_location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        assert new_location.node != original_primary
