# tests/test_replicated_topology.py
"""Replicated topology: ClusterState allocations, gossip merge, leader/follower coordinator."""
from __future__ import annotations

import asyncio
from typing import Any

from casty import ActorSystem, Behaviors, ClusterState, NodeAddress
from casty.gossip_actor import (
    GossipMessage,
    GossipMsg,
    GetClusterState,
    UpdateShardAllocations,
    gossip_actor,
)
from casty.shard_coordinator_actor import (
    CoordinatorMsg,
    GetShardLocation,
    LeastShardStrategy,
    NodeDown,
    PublishAllocations,
    SetRole,
    ShardLocation,
    SyncAllocations,
    shard_coordinator_actor,
)
from casty.cluster import ClusterCmd, cluster_actor, ClusterConfig, GetState, RegisterCoordinator
from casty.cluster_state import Member, MemberStatus
from casty.ref import ActorRef
from casty.replication import ShardAllocation
from casty.serialization import JsonSerializer, TypeRegistry


# --- ClusterState allocation tests ---


def test_cluster_state_default_allocations_empty() -> None:
    state = ClusterState()
    assert state.shard_allocations == {}
    assert state.allocation_epoch == 0


def test_cluster_state_with_allocations() -> None:
    node = NodeAddress(host="10.0.0.1", port=25520)
    alloc = ShardAllocation(primary=node)
    state = ClusterState().with_allocations("counters", {0: alloc}, epoch=1)
    assert state.shard_allocations["counters"][0] == alloc
    assert state.allocation_epoch == 1


def test_cluster_state_with_allocations_preserves_members() -> None:
    node = NodeAddress(host="10.0.0.1", port=25520)
    member = Member(address=node, status=MemberStatus.up, roles=frozenset())
    state = ClusterState().add_member(member)
    alloc = ShardAllocation(primary=node)
    new_state = state.with_allocations("counters", {0: alloc}, epoch=1)
    assert member in new_state.members
    assert new_state.shard_allocations["counters"][0] == alloc


def test_cluster_state_mutations_preserve_allocations() -> None:
    """add_member, update_status, mark_unreachable, mark_reachable all preserve allocations."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    alloc = ShardAllocation(primary=node_a)
    base = ClusterState().with_allocations("test", {0: alloc}, epoch=5)

    member = Member(address=node_b, status=MemberStatus.joining, roles=frozenset())
    s1 = base.add_member(member)
    assert s1.allocation_epoch == 5
    assert s1.shard_allocations["test"][0] == alloc

    s2 = s1.update_status(node_b, MemberStatus.up)
    assert s2.allocation_epoch == 5

    s3 = s2.mark_unreachable(node_b)
    assert s3.allocation_epoch == 5

    s4 = s3.mark_reachable(node_b)
    assert s4.allocation_epoch == 5


# --- Gossip merge tests ---


async def test_gossip_merge_higher_epoch_wins() -> None:
    """When merging gossip, the state with higher allocation_epoch wins."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)

    alloc_a = ShardAllocation(primary=node_a)
    alloc_b = ShardAllocation(primary=node_b)

    async with ActorSystem(name="test") as system:
        # Local gossip has epoch=1
        local_state = (
            ClusterState()
            .add_member(Member(address=node_a, status=MemberStatus.up, roles=frozenset()))
            .with_allocations("shard", {0: alloc_a}, epoch=1)
        )
        gossip_ref = system.spawn(
            gossip_actor(self_node=node_a, initial_state=local_state),
            "gossip",
        )
        await asyncio.sleep(0.1)

        # Remote state has epoch=3 — should win
        remote_state = (
            ClusterState()
            .add_member(Member(address=node_b, status=MemberStatus.up, roles=frozenset()))
            .with_allocations("shard", {0: alloc_b}, epoch=3)
        )
        gossip_ref.tell(GossipMessage(state=remote_state, from_node=node_b))
        await asyncio.sleep(0.1)

        result: ClusterState = await system.ask(
            gossip_ref,
            lambda r: GetClusterState(reply_to=r),
            timeout=2.0,
        )
        assert result.allocation_epoch == 3
        assert result.shard_allocations["shard"][0].primary == node_b


async def test_gossip_merge_lower_epoch_keeps_local() -> None:
    """When remote epoch is lower, local allocations are kept."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)

    alloc_a = ShardAllocation(primary=node_a)
    alloc_b = ShardAllocation(primary=node_b)

    async with ActorSystem(name="test") as system:
        local_state = (
            ClusterState()
            .add_member(Member(address=node_a, status=MemberStatus.up, roles=frozenset()))
            .with_allocations("shard", {0: alloc_a}, epoch=5)
        )
        gossip_ref = system.spawn(
            gossip_actor(self_node=node_a, initial_state=local_state),
            "gossip",
        )
        await asyncio.sleep(0.1)

        remote_state = (
            ClusterState()
            .add_member(Member(address=node_b, status=MemberStatus.up, roles=frozenset()))
            .with_allocations("shard", {0: alloc_b}, epoch=2)
        )
        gossip_ref.tell(GossipMessage(state=remote_state, from_node=node_b))
        await asyncio.sleep(0.1)

        result: ClusterState = await system.ask(
            gossip_ref,
            lambda r: GetClusterState(reply_to=r),
            timeout=2.0,
        )
        assert result.allocation_epoch == 5
        assert result.shard_allocations["shard"][0].primary == node_a


async def test_gossip_update_shard_allocations() -> None:
    """UpdateShardAllocations updates state when epoch is higher."""
    node = NodeAddress(host="10.0.0.1", port=25520)
    alloc = ShardAllocation(primary=node)

    async with ActorSystem(name="test") as system:
        initial = ClusterState().add_member(
            Member(address=node, status=MemberStatus.up, roles=frozenset())
        )
        gossip_ref = system.spawn(
            gossip_actor(self_node=node, initial_state=initial),
            "gossip",
        )
        await asyncio.sleep(0.1)

        gossip_ref.tell(UpdateShardAllocations(
            shard_type="counters",
            allocations={0: alloc, 1: alloc},
            epoch=1,
        ))
        await asyncio.sleep(0.1)

        result: ClusterState = await system.ask(
            gossip_ref,
            lambda r: GetClusterState(reply_to=r),
            timeout=2.0,
        )
        assert result.allocation_epoch == 1
        assert len(result.shard_allocations["counters"]) == 2


async def test_gossip_update_shard_allocations_stale_epoch_ignored() -> None:
    """UpdateShardAllocations with stale epoch is ignored."""
    node = NodeAddress(host="10.0.0.1", port=25520)
    alloc_old = ShardAllocation(primary=node)
    alloc_new = ShardAllocation(primary=node, replicas=(NodeAddress("10.0.0.2", 25520),))

    async with ActorSystem(name="test") as system:
        initial = (
            ClusterState()
            .add_member(Member(address=node, status=MemberStatus.up, roles=frozenset()))
            .with_allocations("counters", {0: alloc_new}, epoch=5)
        )
        gossip_ref = system.spawn(
            gossip_actor(self_node=node, initial_state=initial),
            "gossip",
        )
        await asyncio.sleep(0.1)

        # Try updating with lower epoch — should be ignored
        gossip_ref.tell(UpdateShardAllocations(
            shard_type="counters",
            allocations={0: alloc_old},
            epoch=3,
        ))
        await asyncio.sleep(0.1)

        result: ClusterState = await system.ask(
            gossip_ref,
            lambda r: GetClusterState(reply_to=r),
            timeout=2.0,
        )
        assert result.allocation_epoch == 5
        assert result.shard_allocations["counters"][0] == alloc_new


# --- Serialization round-trip ---


def test_serialization_shard_allocations_round_trip() -> None:
    """dict[str, dict[int, ShardAllocation]] serializes and deserializes correctly."""
    registry = TypeRegistry()
    registry.register_all(ClusterState, Member, ShardAllocation, NodeAddress, MemberStatus)
    serializer = JsonSerializer(registry)

    node = NodeAddress(host="10.0.0.1", port=25520)
    alloc = ShardAllocation(primary=node, replicas=(NodeAddress("10.0.0.2", 25520),))
    state = (
        ClusterState()
        .add_member(Member(address=node, status=MemberStatus.up, roles=frozenset()))
        .with_allocations("counters", {0: alloc, 1: alloc}, epoch=42)
    )

    data = serializer.serialize(state)
    restored: Any = serializer.deserialize(data)

    assert restored.allocation_epoch == 42
    assert len(restored.shard_allocations["counters"]) == 2
    assert restored.shard_allocations["counters"][0].primary == node
    assert restored.shard_allocations["counters"][0].replicas == (NodeAddress("10.0.0.2", 25520),)


# ===========================================================================
# Phase 2: Leader/Follower coordinator tests
# ===========================================================================


async def test_legacy_mode_no_shard_type_is_leader() -> None:
    """Without shard_type, coordinator acts as leader directly (backward compat)."""
    node = NodeAddress(host="127.0.0.1", port=25520)
    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node}),
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        loc: ShardLocation = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        assert loc.shard_id == 0
        assert loc.node == node


async def test_pending_buffers_until_set_role_leader() -> None:
    """With shard_type, coordinator buffers until SetRole(is_leader=True)."""
    node = NodeAddress(host="127.0.0.1", port=25520)
    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node}),
                shard_type="counters",
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        # Send a GetShardLocation — should be buffered
        async def _noop(_ctx: Any, _msg: Any) -> Any:
            return Behaviors.same()

        loc_ref: ActorRef[ShardLocation] = system.spawn(Behaviors.receive(_noop), "reply_sink")
        coord.tell(GetShardLocation(shard_id=0, reply_to=loc_ref))
        await asyncio.sleep(0.1)

        # Now set role to leader — buffered message should be drained
        coord.tell(SetRole(is_leader=True, leader_node=node))
        coord.tell(SyncAllocations(allocations={}, epoch=0))
        await asyncio.sleep(0.1)

        # Ask again — should work immediately
        loc: ShardLocation = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        assert loc.shard_id == 0
        assert loc.node == node


async def test_follower_serves_cached_allocations() -> None:
    """Follower serves known shard allocations from SyncAllocations."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)

    alloc = ShardAllocation(primary=node_a, replicas=(node_b,))

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b}),
                shard_type="counters",
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        # Set role to follower
        coord.tell(SetRole(is_leader=False, leader_node=node_a))
        await asyncio.sleep(0.1)

        # Sync allocations
        coord.tell(SyncAllocations(allocations={0: alloc}, epoch=1))
        await asyncio.sleep(0.1)

        # Now ask for the synced shard — should be served from cache
        loc: ShardLocation = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        assert loc.shard_id == 0
        assert loc.node == node_a
        assert loc.replicas == (node_b,)


async def test_follower_buffers_unknown_shards_drains_on_sync() -> None:
    """Follower buffers GetShardLocation for unknown shards, drains on SyncAllocations."""
    node = NodeAddress(host="10.0.0.1", port=25520)
    alloc = ShardAllocation(primary=node)

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node}),
                shard_type="counters",
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        # Set role to follower (no allocations yet)
        coord.tell(SetRole(is_leader=False, leader_node=node))
        await asyncio.sleep(0.1)

        # Ask for shard 5 — unknown, will be buffered
        reply_holder: list[ShardLocation] = []

        async def _capture(_ctx: Any, msg: Any) -> Any:
            if isinstance(msg, ShardLocation):
                reply_holder.append(msg)
            return Behaviors.same()

        sink = system.spawn(Behaviors.receive(_capture), "sink")
        coord.tell(GetShardLocation(shard_id=5, reply_to=sink))
        await asyncio.sleep(0.1)
        assert len(reply_holder) == 0  # Buffered, not answered

        # Sync with shard 5 allocation
        coord.tell(SyncAllocations(allocations={5: alloc}, epoch=1))
        await asyncio.sleep(0.2)

        assert len(reply_holder) == 1
        assert reply_holder[0].shard_id == 5
        assert reply_holder[0].node == node


async def test_leader_publishes_allocations() -> None:
    """Leader publishes allocations via publish_ref on new shard allocation."""
    node = NodeAddress(host="10.0.0.1", port=25520)
    published: list[PublishAllocations] = []

    async with ActorSystem(name="test") as system:
        async def _capture_publish(_ctx: Any, msg: Any) -> Any:
            if isinstance(msg, PublishAllocations):
                published.append(msg)
            return Behaviors.same()

        publish_sink: ActorRef[Any] = system.spawn(
            Behaviors.receive(_capture_publish), "pub_sink"
        )

        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node}),
                shard_type="counters",
                publish_ref=publish_sink,
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        # Activate as leader
        coord.tell(SetRole(is_leader=True, leader_node=node))
        coord.tell(SyncAllocations(allocations={}, epoch=0))
        await asyncio.sleep(0.1)

        # Allocate a shard
        loc: ShardLocation = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        await asyncio.sleep(0.1)

        assert loc.node == node
        assert len(published) == 1
        assert published[0].shard_type == "counters"
        assert published[0].epoch == 1
        assert 0 in published[0].allocations


async def test_leader_to_follower_demotion() -> None:
    """SetRole(is_leader=False) demotes a leader to follower."""
    node = NodeAddress(host="10.0.0.1", port=25520)

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node}),
                shard_type="counters",
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        # Start as leader
        coord.tell(SetRole(is_leader=True, leader_node=node))
        coord.tell(SyncAllocations(allocations={}, epoch=0))
        await asyncio.sleep(0.1)

        # Allocate shard 0
        loc: ShardLocation = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        assert loc.node == node

        # Demote to follower
        coord.tell(SetRole(is_leader=False, leader_node=None))
        await asyncio.sleep(0.1)

        # Should still serve cached shard 0
        loc2: ShardLocation = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        assert loc2.node == node




async def test_pending_waits_for_both_set_role_and_sync() -> None:
    """Pending coordinator waits for BOTH SetRole and SyncAllocations before activating."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    alloc = ShardAllocation(primary=node_a)

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b}),
                shard_type="counters",
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        # Send SetRole first — should NOT activate yet
        coord.tell(SetRole(is_leader=True, leader_node=node_a))
        await asyncio.sleep(0.1)

        # Buffer a request
        reply_holder: list[ShardLocation] = []

        async def _capture(_ctx: Any, msg: Any) -> Any:
            if isinstance(msg, ShardLocation):
                reply_holder.append(msg)
            return Behaviors.same()

        sink = system.spawn(Behaviors.receive(_capture), "sink")
        coord.tell(GetShardLocation(shard_id=0, reply_to=sink))
        await asyncio.sleep(0.1)
        # Still buffered — not yet activated
        assert len(reply_holder) == 0

        # Now send SyncAllocations with existing allocation — activates as leader
        coord.tell(SyncAllocations(allocations={0: alloc}, epoch=1))
        await asyncio.sleep(0.2)

        # Buffered request should have been served with the synced allocation
        assert len(reply_holder) == 1
        assert reply_holder[0].node == node_a  # Existing allocation preserved


# ===========================================================================
# Phase 3: Wiring via cluster_actor
# ===========================================================================


async def test_cluster_actor_sends_set_role_to_registered_coordinators() -> None:
    """cluster_actor sends SetRole to coordinators registered via RegisterCoordinator."""
    received_msgs: list[Any] = []

    async with ActorSystem(name="test") as system:
        # Create a spy coordinator that captures messages
        async def spy_coord(_ctx: Any, msg: Any) -> Any:
            received_msgs.append(msg)
            return Behaviors.same()

        coord_ref: ActorRef[CoordinatorMsg] = system.spawn(
            Behaviors.receive(spy_coord), "_coord-counters"
        )

        # Create cluster_actor
        cluster_ref: ActorRef[ClusterCmd] = system.spawn(
            cluster_actor(
                config=ClusterConfig(host="127.0.0.1", port=25520, seed_nodes=[]),
            ),
            "_cluster",
        )
        await asyncio.sleep(0.5)  # Allow gossip to produce state

        # Register coordinator via message
        cluster_ref.tell(RegisterCoordinator(  # type: ignore[arg-type]
            shard_name="counters",
            coord_ref=coord_ref,
        ))
        await asyncio.sleep(0.5)

        # Check that SetRole and SyncAllocations were sent immediately on registration
        set_role_msgs = [m for m in received_msgs if isinstance(m, SetRole)]
        sync_msgs = [m for m in received_msgs if isinstance(m, SyncAllocations)]
        assert len(set_role_msgs) > 0, "SetRole should have been sent"
        assert len(sync_msgs) > 0, "SyncAllocations should have been sent"
        # Since this is the only node, it should be leader
        assert set_role_msgs[-1].is_leader is True



async def test_cluster_actor_forwards_publish_allocations_to_gossip() -> None:
    """PublishAllocations from coordinator is forwarded to gossip as UpdateShardAllocations."""
    node = NodeAddress(host="127.0.0.1", port=25520)
    alloc = ShardAllocation(primary=node)

    async with ActorSystem(name="test") as system:
        cluster_ref: ActorRef[ClusterCmd] = system.spawn(
            cluster_actor(
                config=ClusterConfig(host="127.0.0.1", port=25520, seed_nodes=[]),
            ),
            "_cluster",
        )
        await asyncio.sleep(0.5)

        # Send PublishAllocations to cluster_actor
        cluster_ref.tell(PublishAllocations(  # type: ignore[arg-type]
            shard_type="counters",
            allocations={0: alloc},
            epoch=1,
        ))
        await asyncio.sleep(0.5)

        # Verify by reading gossip state
        state: ClusterState = await system.ask(
            cluster_ref,
            lambda r: GetState(reply_to=r),
            timeout=2.0,
        )
        # The gossip should have received the allocation via UpdateShardAllocations
        assert state.allocation_epoch == 1
        assert state.shard_allocations["counters"][0] == alloc


async def test_gossip_updates_state_via_update_shard_allocations() -> None:
    """UpdateShardAllocations from cluster_actor updates gossip state and propagates."""
    node = NodeAddress(host="10.0.0.1", port=25520)
    alloc = ShardAllocation(primary=node)

    async with ActorSystem(name="test") as system:
        initial_state = ClusterState().add_member(
            Member(address=node, status=MemberStatus.up, roles=frozenset())
        )
        gossip_ref: ActorRef[GossipMsg] = system.spawn(
            gossip_actor(self_node=node, initial_state=initial_state),
            "gossip",
        )
        await asyncio.sleep(0.1)

        # Send UpdateShardAllocations
        gossip_ref.tell(UpdateShardAllocations(
            shard_type="counters",
            allocations={0: alloc},
            epoch=1,
        ))
        await asyncio.sleep(0.1)

        # Verify state was updated
        state: ClusterState = await system.ask(
            gossip_ref,
            lambda r: GetClusterState(reply_to=r),
            timeout=2.0,
        )
        assert state.allocation_epoch == 1
        assert state.shard_allocations["counters"][0] == alloc

        # Update with higher epoch
        alloc2 = ShardAllocation(primary=node, replicas=(NodeAddress("10.0.0.2", 25520),))
        gossip_ref.tell(UpdateShardAllocations(
            shard_type="counters",
            allocations={0: alloc2},
            epoch=2,
        ))
        await asyncio.sleep(0.1)

        state2: ClusterState = await system.ask(
            gossip_ref,
            lambda r: GetClusterState(reply_to=r),
            timeout=2.0,
        )
        assert state2.allocation_epoch == 2
        assert state2.shard_allocations["counters"][0].replicas == (NodeAddress("10.0.0.2", 25520),)


# ===========================================================================
# Phase 5: Failover tests
# ===========================================================================


async def test_follower_promoted_to_leader_responds_immediately() -> None:
    """Follower promoted to leader can respond immediately with cached allocations."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    alloc = ShardAllocation(primary=node_a)

    async with ActorSystem(name="test") as system:
        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b}),
                shard_type="counters",
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        # Activate as follower with shard 0 allocated
        coord.tell(SetRole(is_leader=False, leader_node=node_a))
        coord.tell(SyncAllocations(allocations={0: alloc}, epoch=1))
        await asyncio.sleep(0.1)

        # Promote to leader
        coord.tell(SetRole(is_leader=True, leader_node=node_b))
        await asyncio.sleep(0.1)

        # Should serve known shard immediately
        loc: ShardLocation = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        assert loc.shard_id == 0
        assert loc.node == node_a

        # Should also allocate NEW shards (leader capability)
        loc2: ShardLocation = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=5, reply_to=r),
            timeout=2.0,
        )
        assert loc2.shard_id == 5
        assert loc2.node in {node_a, node_b}


async def test_follower_promotion_publishes_new_allocations() -> None:
    """Promoted follower publishes new allocations via publish_ref."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    published: list[PublishAllocations] = []

    async with ActorSystem(name="test") as system:
        async def capture_publish(_ctx: Any, msg: Any) -> Any:
            if isinstance(msg, PublishAllocations):
                published.append(msg)
            return Behaviors.same()

        publish_sink: ActorRef[Any] = system.spawn(
            Behaviors.receive(capture_publish), "pub_sink"
        )

        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b}),
                shard_type="counters",
                publish_ref=publish_sink,
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        # Start as follower
        coord.tell(SetRole(is_leader=False, leader_node=node_a))
        coord.tell(SyncAllocations(allocations={}, epoch=0))
        await asyncio.sleep(0.1)

        # Promote to leader
        coord.tell(SetRole(is_leader=True, leader_node=node_b))
        await asyncio.sleep(0.1)

        # Allocate a new shard
        loc: ShardLocation = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        await asyncio.sleep(0.1)

        assert loc.node in {node_a, node_b}
        assert len(published) == 1
        assert published[0].shard_type == "counters"
        assert published[0].epoch == 1


async def test_promoted_leader_handles_node_down() -> None:
    """Promoted leader handles NodeDown with replica promotion."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    node_c = NodeAddress(host="10.0.0.3", port=25520)

    # Shard 0 is on node_a with replica on node_b
    alloc = ShardAllocation(primary=node_a, replicas=(node_b,))
    published: list[PublishAllocations] = []

    async with ActorSystem(name="test") as system:
        async def capture_publish(_ctx: Any, msg: Any) -> Any:
            if isinstance(msg, PublishAllocations):
                published.append(msg)
            return Behaviors.same()

        publish_sink: ActorRef[Any] = system.spawn(
            Behaviors.receive(capture_publish), "pub_sink"
        )

        coord = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=frozenset({node_a, node_b, node_c}),
                shard_type="counters",
                publish_ref=publish_sink,
            ),
            "coord",
        )
        await asyncio.sleep(0.1)

        # Start as follower with existing allocation
        coord.tell(SetRole(is_leader=False, leader_node=node_a))
        coord.tell(SyncAllocations(allocations={0: alloc}, epoch=1))
        await asyncio.sleep(0.1)

        # Promote to leader (node_a went down, node_c takes over as leader)
        coord.tell(SetRole(is_leader=True, leader_node=node_c))
        await asyncio.sleep(0.1)

        # Report node_a down — replica node_b should be promoted to primary
        coord.tell(NodeDown(node=node_a))
        await asyncio.sleep(0.1)

        loc: ShardLocation = await system.ask(
            coord,
            lambda r: GetShardLocation(shard_id=0, reply_to=r),
            timeout=2.0,
        )
        assert loc.node == node_b


async def test_three_node_consistency_after_leader_change() -> None:
    """Three coordinators maintain consistent state through leader change."""
    node_a = NodeAddress(host="10.0.0.1", port=25520)
    node_b = NodeAddress(host="10.0.0.2", port=25520)
    node_c = NodeAddress(host="10.0.0.3", port=25520)
    nodes = frozenset({node_a, node_b, node_c})

    async with ActorSystem(name="test") as system:
        coord_a = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=nodes,
                shard_type="counters",
            ),
            "coord_a",
        )
        coord_b = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=nodes,
                shard_type="counters",
            ),
            "coord_b",
        )
        coord_c = system.spawn(
            shard_coordinator_actor(
                strategy=LeastShardStrategy(),
                available_nodes=nodes,
                shard_type="counters",
            ),
            "coord_c",
        )
        await asyncio.sleep(0.1)

        # A is leader, B and C are followers
        coord_a.tell(SetRole(is_leader=True, leader_node=node_a))
        coord_a.tell(SyncAllocations(allocations={}, epoch=0))
        coord_b.tell(SetRole(is_leader=False, leader_node=node_a))
        coord_b.tell(SyncAllocations(allocations={}, epoch=0))
        coord_c.tell(SetRole(is_leader=False, leader_node=node_a))
        coord_c.tell(SyncAllocations(allocations={}, epoch=0))
        await asyncio.sleep(0.1)

        # Leader allocates 10 shards
        allocations: dict[int, ShardAllocation] = {}
        for i in range(10):
            loc: ShardLocation = await system.ask(
                coord_a,
                lambda r, sid=i: GetShardLocation(shard_id=sid, reply_to=r),
                timeout=2.0,
            )
            allocations[loc.shard_id] = ShardAllocation(primary=loc.node, replicas=loc.replicas)
        await asyncio.sleep(0.1)

        # Sync allocations to followers (simulating gossip)
        final_epoch = 10
        coord_b.tell(SyncAllocations(allocations=allocations, epoch=final_epoch))
        coord_c.tell(SyncAllocations(allocations=allocations, epoch=final_epoch))
        await asyncio.sleep(0.1)

        # Promote B to leader
        coord_b.tell(SetRole(is_leader=True, leader_node=node_b))
        await asyncio.sleep(0.1)

        # B should serve all 10 shards consistently with A's allocations
        for i in range(10):
            loc_b: ShardLocation = await system.ask(
                coord_b,
                lambda r, sid=i: GetShardLocation(shard_id=sid, reply_to=r),
                timeout=2.0,
            )
            assert loc_b.node == allocations[i].primary, f"Shard {i} inconsistent after promotion"
