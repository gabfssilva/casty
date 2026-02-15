from __future__ import annotations

import asyncio
from typing import Any

from casty import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    Member,
    MemberStatus,
    NodeAddress,
)
from casty.cluster.state import ShardAllocation
from casty.core.replication import ReplicationConfig
from casty.cluster.coordinator import (
    CoordinatorMsg,
    GetShardLocation,
    LeastShardStrategy,
    RegisterRegion,
    ShardLocation,
    shard_coordinator_actor,
)
from casty.cluster.topology import SubscribeTopology, TopologySnapshot
from casty.cluster.topology_actor import TopologyMsg, UpdateShardAllocations


SELF_NODE = NodeAddress(host="127.0.0.1", port=2551)
OTHER_NODE = NodeAddress(host="127.0.0.1", port=2552)


def make_snapshot(
    *,
    leader: NodeAddress | None = SELF_NODE,
    members: frozenset[Member] | None = None,
    unreachable: frozenset[NodeAddress] | None = None,
    shard_allocations: dict[str, dict[int, ShardAllocation]] | None = None,
    allocation_epoch: int = 0,
) -> TopologySnapshot:
    if members is None:
        members = frozenset(
            {
                Member(
                    address=SELF_NODE,
                    status=MemberStatus.up,
                    roles=frozenset(),
                    id="node-1",
                ),
                Member(
                    address=OTHER_NODE,
                    status=MemberStatus.up,
                    roles=frozenset(),
                    id="node-2",
                ),
            }
        )
    return TopologySnapshot(
        members=members,
        leader=leader,
        shard_allocations=shard_allocations or {},
        allocation_epoch=allocation_epoch,
        unreachable=unreachable or frozenset(),
        registry=frozenset(),
    )


def fake_topology_actor(
    updates: list[UpdateShardAllocations] | None = None,
) -> Behavior[TopologyMsg]:
    """Fake topology actor that captures subscriptions and allows manual snapshot pushes."""

    def active(
        subscriber: ActorRef[TopologySnapshot] | None,
    ) -> Behavior[Any]:
        async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
            match msg:
                case SubscribeTopology(reply_to=reply_to):
                    return active(reply_to)
                case TopologySnapshot() as snap:
                    if subscriber is not None:
                        subscriber.tell(snap)
                    return Behaviors.same()
                case UpdateShardAllocations() as update:
                    if updates is not None:
                        updates.append(update)
                    return Behaviors.same()
                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(None)


async def test_coordinator_transitions_to_leader_on_snapshot() -> None:
    """Receives TopologySnapshot where leader == self_node, transitions to leader."""
    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(fake_topology_actor(), "_topology")

        coord_ref: ActorRef[CoordinatorMsg] = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({SELF_NODE, OTHER_NODE}),
                shard_type="test",
                topology_ref=topo_ref,  # type: ignore[arg-type]
                self_node=SELF_NODE,
            ),
            "_coord",
        )
        coord_ref.tell(RegisterRegion(node=SELF_NODE))
        coord_ref.tell(RegisterRegion(node=OTHER_NODE))
        await asyncio.sleep(0.1)

        # Send snapshot where leader is self_node
        snapshot = make_snapshot(leader=SELF_NODE)
        coord_ref.tell(snapshot)  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        # Now the coordinator should be in leader mode — can allocate shards
        locations: list[ShardLocation] = []

        def location_collector() -> Behavior[ShardLocation]:
            async def receive(
                ctx: ActorContext[ShardLocation],
                msg: ShardLocation,
            ) -> Behavior[ShardLocation]:
                locations.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        loc_ref: ActorRef[ShardLocation] = system.spawn(
            location_collector(),
            "_loc",
        )
        coord_ref.tell(GetShardLocation(shard_id=0, reply_to=loc_ref))
        await asyncio.sleep(0.1)

        assert len(locations) == 1
        assert locations[0].shard_id == 0


async def test_coordinator_transitions_to_follower_on_snapshot() -> None:
    """Receives TopologySnapshot where leader != self_node, transitions to follower."""
    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(fake_topology_actor(), "_topology")

        coord_ref: ActorRef[CoordinatorMsg] = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({SELF_NODE, OTHER_NODE}),
                shard_type="test",
                topology_ref=topo_ref,  # type: ignore[arg-type]
                self_node=SELF_NODE,
            ),
            "_coord",
        )
        coord_ref.tell(RegisterRegion(node=SELF_NODE))
        await asyncio.sleep(0.1)

        # Snapshot where OTHER_NODE is leader
        allocs = {0: ShardAllocation(primary=OTHER_NODE, replicas=())}
        snapshot = make_snapshot(
            leader=OTHER_NODE,
            shard_allocations={"test": allocs},
            allocation_epoch=1,
        )
        coord_ref.tell(snapshot)  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        # Coordinator should be follower — can serve cached allocations
        locations: list[ShardLocation] = []

        def location_collector() -> Behavior[ShardLocation]:
            async def receive(
                ctx: ActorContext[ShardLocation],
                msg: ShardLocation,
            ) -> Behavior[ShardLocation]:
                locations.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        loc_ref: ActorRef[ShardLocation] = system.spawn(
            location_collector(),
            "_loc",
        )
        coord_ref.tell(GetShardLocation(shard_id=0, reply_to=loc_ref))
        await asyncio.sleep(0.1)

        assert len(locations) == 1
        assert locations[0].node == OTHER_NODE


async def test_coordinator_evicts_on_unreachable() -> None:
    """Leader receives snapshot with new unreachable node, promotes replicas."""
    async with ActorSystem(name="test") as system:
        updates: list[UpdateShardAllocations] = []
        topo_ref = system.spawn(fake_topology_actor(updates=updates), "_topology")

        coord_ref: ActorRef[CoordinatorMsg] = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({SELF_NODE, OTHER_NODE}),
                shard_type="test",
                topology_ref=topo_ref,  # type: ignore[arg-type]
                self_node=SELF_NODE,
                replication=ReplicationConfig(replicas=1),
            ),
            "_coord",
        )
        coord_ref.tell(RegisterRegion(node=SELF_NODE))
        coord_ref.tell(RegisterRegion(node=OTHER_NODE))
        await asyncio.sleep(0.1)

        # First: activate as leader
        snapshot1 = make_snapshot(leader=SELF_NODE)
        coord_ref.tell(snapshot1)  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        # Allocate a shard on OTHER_NODE
        locations: list[ShardLocation] = []

        def location_collector() -> Behavior[ShardLocation]:
            async def receive(
                ctx: ActorContext[ShardLocation],
                msg: ShardLocation,
            ) -> Behavior[ShardLocation]:
                locations.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        loc_ref: ActorRef[ShardLocation] = system.spawn(
            location_collector(),
            "_loc",
        )
        coord_ref.tell(GetShardLocation(shard_id=0, reply_to=loc_ref))
        await asyncio.sleep(0.1)

        # Now: send snapshot with OTHER_NODE unreachable
        snapshot2 = make_snapshot(
            leader=SELF_NODE,
            unreachable=frozenset({OTHER_NODE}),
        )
        coord_ref.tell(snapshot2)  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        # Should have published new allocations (eviction)
        assert len(updates) >= 2


async def test_coordinator_syncs_allocations_from_snapshot() -> None:
    """Follower receives snapshot with updated allocations, updates local state."""
    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(fake_topology_actor(), "_topology")

        coord_ref: ActorRef[CoordinatorMsg] = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({SELF_NODE, OTHER_NODE}),
                shard_type="test",
                topology_ref=topo_ref,  # type: ignore[arg-type]
                self_node=SELF_NODE,
            ),
            "_coord",
        )
        coord_ref.tell(RegisterRegion(node=SELF_NODE))
        await asyncio.sleep(0.1)

        # Activate as follower
        allocs_v1 = {0: ShardAllocation(primary=OTHER_NODE, replicas=())}
        snapshot1 = make_snapshot(
            leader=OTHER_NODE,
            shard_allocations={"test": allocs_v1},
            allocation_epoch=1,
        )
        coord_ref.tell(snapshot1)  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        # Send updated allocations
        allocs_v2 = {
            0: ShardAllocation(primary=SELF_NODE, replicas=()),
            1: ShardAllocation(primary=OTHER_NODE, replicas=()),
        }
        snapshot2 = make_snapshot(
            leader=OTHER_NODE,
            shard_allocations={"test": allocs_v2},
            allocation_epoch=2,
        )
        coord_ref.tell(snapshot2)  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        # Verify shard 0 now points to SELF_NODE
        locations: list[ShardLocation] = []

        def location_collector() -> Behavior[ShardLocation]:
            async def receive(
                ctx: ActorContext[ShardLocation],
                msg: ShardLocation,
            ) -> Behavior[ShardLocation]:
                locations.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        loc_ref: ActorRef[ShardLocation] = system.spawn(
            location_collector(),
            "_loc",
        )
        coord_ref.tell(GetShardLocation(shard_id=0, reply_to=loc_ref))
        await asyncio.sleep(0.1)

        assert len(locations) == 1
        assert locations[0].node == SELF_NODE
