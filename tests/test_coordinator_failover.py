# tests/test_coordinator_failover.py
from __future__ import annotations

import asyncio

from casty import ActorSystem, Member, MemberStatus
from casty.shard_coordinator_actor import (
    GetShardLocation,
    LeastShardStrategy,
    RegisterRegion,
    shard_coordinator_actor,
)
from casty.cluster_state import NodeAddress
from casty.replication import ReplicationConfig, ShardAllocation
from casty.topology import TopologySnapshot


def make_snapshot(
    *,
    members: frozenset[Member],
    leader: NodeAddress | None,
    shard_allocations: dict[str, dict[int, ShardAllocation]] | None = None,
    allocation_epoch: int = 0,
    unreachable: frozenset[NodeAddress] | None = None,
) -> TopologySnapshot:
    return TopologySnapshot(
        members=members,
        leader=leader,
        shard_allocations=shard_allocations or {},
        allocation_epoch=allocation_epoch,
        unreachable=unreachable or frozenset(),
    )


def m(addr: NodeAddress, node_id: str) -> Member:
    return Member(address=addr, status=MemberStatus.up, roles=frozenset(), id=node_id)


async def test_coordinator_promotes_replica_on_node_down() -> None:
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    node_c = NodeAddress(host="10.0.0.3", port=25520)
    members = frozenset({m(node_a, "a"), m(node_b, "b"), m(node_c, "c")})

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b, node_c}),
                replication=ReplicationConfig(replicas=2),
                self_node=node_a,
            ),
            "coord",
        )
        coord.tell(RegisterRegion(node=node_a))
        coord.tell(RegisterRegion(node=node_b))
        coord.tell(RegisterRegion(node=node_c))
        coord.tell(make_snapshot(members=members, leader=node_a))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=5.0,
        )
        original_primary = location.node
        original_replicas = location.replicas

        # Simulate primary going down via unreachable in TopologySnapshot
        coord.tell(make_snapshot(  # type: ignore[arg-type]
            members=members,
            leader=node_a,
            unreachable=frozenset({original_primary}),
        ))
        await asyncio.sleep(0.1)

        # Re-query — should get a different primary (first replica)
        new_location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=5.0,
        )
        assert new_location.node != original_primary
        assert new_location.node in original_replicas


async def test_coordinator_removes_failed_replica() -> None:
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    node_c = NodeAddress(host="10.0.0.3", port=25520)
    members = frozenset({m(node_a, "a"), m(node_b, "b"), m(node_c, "c")})

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b, node_c}),
                replication=ReplicationConfig(replicas=2),
                self_node=node_a,
            ),
            "coord",
        )
        coord.tell(RegisterRegion(node=node_a))
        coord.tell(RegisterRegion(node=node_b))
        coord.tell(RegisterRegion(node=node_c))
        coord.tell(make_snapshot(members=members, leader=node_a))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=5.0,
        )
        primary = location.node
        # Pick a replica to fail
        failed_replica = location.replicas[0]

        coord.tell(make_snapshot(  # type: ignore[arg-type]
            members=members,
            leader=node_a,
            unreachable=frozenset({failed_replica}),
        ))
        await asyncio.sleep(0.1)

        new_location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=5.0,
        )
        # Primary should stay the same
        assert new_location.node == primary
        # Failed replica should be removed
        assert failed_replica not in new_location.replicas


async def test_coordinator_removes_shard_when_no_replicas() -> None:
    """When primary fails and there are no replicas, shard is deallocated."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    members = frozenset({m(node_a, "a"), m(node_b, "b")})

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b}),
                self_node=node_a,
            ),
            "coord",
        )
        coord.tell(RegisterRegion(node=node_a))
        coord.tell(RegisterRegion(node=node_b))
        coord.tell(make_snapshot(members=members, leader=node_a))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=5.0,
        )
        original_primary = location.node

        coord.tell(make_snapshot(  # type: ignore[arg-type]
            members=members,
            leader=node_a,
            unreachable=frozenset({original_primary}),
        ))
        await asyncio.sleep(0.1)

        # Re-query — shard should be reallocated to remaining node
        new_location = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=5.0,
        )
        assert new_location.node != original_primary
