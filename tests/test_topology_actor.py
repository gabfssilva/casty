from __future__ import annotations

import asyncio
from dataclasses import dataclass

from casty import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    PhiAccrualFailureDetector,
)
from casty.cluster.topology_actor import (
    CheckAvailability,
    DownMember,
    GossipMessage,
    GossipTick,
    HeartbeatTick,
    JoinRequest,
    PromoteMember,
)
from casty.cluster.topology import (
    SubscribeTopology,
    TopologySnapshot,
    UnsubscribeTopology,
)
from casty.cluster.topology_actor import TopologyMsg, WaitForMembers, topology_actor


SELF_NODE = NodeAddress(host="127.0.0.1", port=2551)
OTHER_NODE = NodeAddress(host="127.0.0.1", port=2552)
THIRD_NODE = NodeAddress(host="127.0.0.1", port=2553)


def make_state(
    *members: Member,
    unreachable: frozenset[NodeAddress] | None = None,
) -> ClusterState:
    return ClusterState(
        members=frozenset(members),
        unreachable=unreachable or frozenset(),
    )


def self_member(status: MemberStatus = MemberStatus.up) -> Member:
    return Member(address=SELF_NODE, status=status, roles=frozenset(), id="node-1")


def other_member(status: MemberStatus = MemberStatus.up) -> Member:
    return Member(address=OTHER_NODE, status=status, roles=frozenset(), id="node-2")


def third_member(status: MemberStatus = MemberStatus.up) -> Member:
    return Member(address=THIRD_NODE, status=status, roles=frozenset(), id="node-3")


def make_topology_actor(
    state: ClusterState | None = None,
    detector: PhiAccrualFailureDetector | None = None,
) -> Behavior[TopologyMsg]:
    initial_state = state or make_state(self_member())
    return topology_actor(
        self_node=SELF_NODE,
        node_id="node-1",
        roles=frozenset(),
        initial_state=initial_state,
        detector=detector or PhiAccrualFailureDetector(threshold=8.0),
        system_name="test",
    )


@dataclass(frozen=True)
class CollectSnapshot:
    reply_to: ActorRef[TopologySnapshot]


def snapshot_collector() -> Behavior[TopologySnapshot]:
    """Actor that collects topology snapshots for test assertions."""

    def active(
        snapshots: tuple[TopologySnapshot, ...],
    ) -> Behavior[TopologySnapshot]:
        async def receive(
            ctx: ActorContext[TopologySnapshot],
            msg: TopologySnapshot,
        ) -> Behavior[TopologySnapshot]:
            return active((*snapshots, msg))

        return Behaviors.receive(receive)

    return active(())


async def test_topology_actor_publishes_on_subscribe() -> None:
    """Subscribe returns the current snapshot immediately."""
    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(make_topology_actor(), "_topology")

        snapshots: list[TopologySnapshot] = []

        def collector_behavior() -> Behavior[TopologySnapshot]:
            async def receive(
                ctx: ActorContext[TopologySnapshot],
                msg: TopologySnapshot,
            ) -> Behavior[TopologySnapshot]:
                snapshots.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        collector_ref: ActorRef[TopologySnapshot] = system.spawn(
            collector_behavior(),
            "collector",
        )
        topo_ref.tell(SubscribeTopology(reply_to=collector_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        assert len(snapshots) >= 1
        snap = snapshots[0]
        assert any(m.address == SELF_NODE for m in snap.members)


async def test_topology_actor_merges_gossip() -> None:
    """Receives GossipMessage, merges state, pushes to subscribers."""
    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(make_topology_actor(), "_topology")

        snapshots: list[TopologySnapshot] = []

        def collector_behavior() -> Behavior[TopologySnapshot]:
            async def receive(
                ctx: ActorContext[TopologySnapshot],
                msg: TopologySnapshot,
            ) -> Behavior[TopologySnapshot]:
                snapshots.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        collector_ref: ActorRef[TopologySnapshot] = system.spawn(
            collector_behavior(),
            "collector",
        )
        topo_ref.tell(SubscribeTopology(reply_to=collector_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        remote_state = make_state(
            self_member(),
            other_member(MemberStatus.joining),
        )
        topo_ref.tell(  # type: ignore[arg-type]
            GossipMessage(state=remote_state, from_node=OTHER_NODE),
        )
        await asyncio.sleep(0.1)

        assert len(snapshots) >= 2
        latest = snapshots[-1]
        member_addrs = {m.address for m in latest.members}
        assert OTHER_NODE in member_addrs


async def test_topology_actor_detects_unreachable() -> None:
    """Phi accrual marks node unreachable after missed heartbeats."""
    detector = PhiAccrualFailureDetector(
        threshold=1.0,
        first_heartbeat_estimate_ms=50.0,
        min_std_deviation_ms=10.0,
    )
    initial_state = make_state(self_member(), other_member())
    topo_ref_behavior = topology_actor(
        self_node=SELF_NODE,
        node_id="node-1",
        roles=frozenset(),
        initial_state=initial_state,
        detector=detector,
        system_name="test",
    )

    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(topo_ref_behavior, "_topology")

        snapshots: list[TopologySnapshot] = []

        def collector_behavior() -> Behavior[TopologySnapshot]:
            async def receive(
                ctx: ActorContext[TopologySnapshot],
                msg: TopologySnapshot,
            ) -> Behavior[TopologySnapshot]:
                snapshots.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        collector_ref: ActorRef[TopologySnapshot] = system.spawn(
            collector_behavior(),
            "collector",
        )
        topo_ref.tell(SubscribeTopology(reply_to=collector_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        # Record a heartbeat so the detector starts tracking
        node_key = f"{OTHER_NODE.host}:{OTHER_NODE.port}"
        detector.heartbeat(node_key)
        await asyncio.sleep(0.2)

        topo_ref.tell(CheckAvailability(reply_to=topo_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        unreachable_snaps = [s for s in snapshots if s.unreachable]
        assert len(unreachable_snaps) >= 1
        assert OTHER_NODE in unreachable_snaps[-1].unreachable


async def test_topology_actor_join_request() -> None:
    """Handles JoinRequest, adds member as joining, pushes snapshot."""
    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(make_topology_actor(), "_topology")

        snapshots: list[TopologySnapshot] = []

        def collector_behavior() -> Behavior[TopologySnapshot]:
            async def receive(
                ctx: ActorContext[TopologySnapshot],
                msg: TopologySnapshot,
            ) -> Behavior[TopologySnapshot]:
                snapshots.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        collector_ref: ActorRef[TopologySnapshot] = system.spawn(
            collector_behavior(),
            "collector",
        )
        topo_ref.tell(SubscribeTopology(reply_to=collector_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        topo_ref.tell(  # type: ignore[arg-type]
            JoinRequest(
                node=OTHER_NODE,
                roles=frozenset(),
                node_id="node-2",
            ),
        )
        await asyncio.sleep(0.1)

        assert len(snapshots) >= 2
        latest = snapshots[-1]
        joining = [m for m in latest.members if m.address == OTHER_NODE]
        assert len(joining) == 1
        assert joining[0].status == MemberStatus.joining


async def test_topology_actor_promotes_member() -> None:
    """Leader promotes joining -> up, pushes snapshot."""
    initial_state = make_state(
        self_member(),
        other_member(MemberStatus.joining),
    )

    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(
            make_topology_actor(state=initial_state),
            "_topology",
        )

        snapshots: list[TopologySnapshot] = []

        def collector_behavior() -> Behavior[TopologySnapshot]:
            async def receive(
                ctx: ActorContext[TopologySnapshot],
                msg: TopologySnapshot,
            ) -> Behavior[TopologySnapshot]:
                snapshots.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        collector_ref: ActorRef[TopologySnapshot] = system.spawn(
            collector_behavior(),
            "collector",
        )
        topo_ref.tell(SubscribeTopology(reply_to=collector_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        topo_ref.tell(PromoteMember(address=OTHER_NODE))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        assert len(snapshots) >= 2
        latest = snapshots[-1]
        promoted = [m for m in latest.members if m.address == OTHER_NODE]
        assert len(promoted) == 1
        assert promoted[0].status == MemberStatus.up


async def test_topology_actor_downs_member() -> None:
    """DownMember marks node down, removes from detector, pushes snapshot."""
    initial_state = make_state(self_member(), other_member())

    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(
            make_topology_actor(state=initial_state),
            "_topology",
        )

        snapshots: list[TopologySnapshot] = []

        def collector_behavior() -> Behavior[TopologySnapshot]:
            async def receive(
                ctx: ActorContext[TopologySnapshot],
                msg: TopologySnapshot,
            ) -> Behavior[TopologySnapshot]:
                snapshots.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        collector_ref: ActorRef[TopologySnapshot] = system.spawn(
            collector_behavior(),
            "collector",
        )
        topo_ref.tell(SubscribeTopology(reply_to=collector_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        topo_ref.tell(DownMember(address=OTHER_NODE))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        assert len(snapshots) >= 2
        latest = snapshots[-1]
        downed = [m for m in latest.members if m.address == OTHER_NODE]
        assert len(downed) == 1
        assert downed[0].status == MemberStatus.down


async def test_topology_actor_dedup_unreachable() -> None:
    """Same node unreachable twice does not produce duplicate push."""
    detector = PhiAccrualFailureDetector(
        threshold=1.0,
        first_heartbeat_estimate_ms=50.0,
        min_std_deviation_ms=10.0,
    )
    initial_state = make_state(self_member(), other_member())

    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(
            topology_actor(
                self_node=SELF_NODE,
                node_id="node-1",
                roles=frozenset(),
                initial_state=initial_state,
                detector=detector,
                system_name="test",
            ),
            "_topology",
        )

        snapshots: list[TopologySnapshot] = []

        def collector_behavior() -> Behavior[TopologySnapshot]:
            async def receive(
                ctx: ActorContext[TopologySnapshot],
                msg: TopologySnapshot,
            ) -> Behavior[TopologySnapshot]:
                snapshots.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        collector_ref: ActorRef[TopologySnapshot] = system.spawn(
            collector_behavior(),
            "collector",
        )
        topo_ref.tell(SubscribeTopology(reply_to=collector_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        # Record a heartbeat then let it expire
        node_key = f"{OTHER_NODE.host}:{OTHER_NODE.port}"
        detector.heartbeat(node_key)
        await asyncio.sleep(0.2)

        count_before = len(snapshots)
        topo_ref.tell(CheckAvailability(reply_to=topo_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)
        count_after_first = len(snapshots)

        topo_ref.tell(CheckAvailability(reply_to=topo_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)
        count_after_second = len(snapshots)

        # First check should produce a push, second should not
        assert count_after_first > count_before
        assert count_after_second == count_after_first


async def test_topology_actor_rejoin_clears_unreachable() -> None:
    """Rejoining node clears unreachable set, pushes snapshot."""
    initial_state = ClusterState(
        members=frozenset({self_member(), other_member()}),
        unreachable=frozenset({OTHER_NODE}),
    )

    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(
            topology_actor(
                self_node=SELF_NODE,
                node_id="node-1",
                roles=frozenset(),
                initial_state=initial_state,
                detector=PhiAccrualFailureDetector(threshold=8.0),
                system_name="test",
            ),
            "_topology",
        )

        snapshots: list[TopologySnapshot] = []

        def collector_behavior() -> Behavior[TopologySnapshot]:
            async def receive(
                ctx: ActorContext[TopologySnapshot],
                msg: TopologySnapshot,
            ) -> Behavior[TopologySnapshot]:
                snapshots.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        collector_ref: ActorRef[TopologySnapshot] = system.spawn(
            collector_behavior(),
            "collector",
        )
        topo_ref.tell(SubscribeTopology(reply_to=collector_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        # Rejoin via JoinRequest
        topo_ref.tell(  # type: ignore[arg-type]
            JoinRequest(
                node=OTHER_NODE,
                roles=frozenset(),
                node_id="node-2",
            ),
        )
        await asyncio.sleep(0.1)

        latest = snapshots[-1]
        assert OTHER_NODE not in latest.unreachable


async def test_topology_actor_unsubscribe() -> None:
    """After unsubscribe, no more pushes."""
    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(make_topology_actor(), "_topology")

        snapshots: list[TopologySnapshot] = []

        def collector_behavior() -> Behavior[TopologySnapshot]:
            async def receive(
                ctx: ActorContext[TopologySnapshot],
                msg: TopologySnapshot,
            ) -> Behavior[TopologySnapshot]:
                snapshots.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        collector_ref: ActorRef[TopologySnapshot] = system.spawn(
            collector_behavior(),
            "collector",
        )
        topo_ref.tell(SubscribeTopology(reply_to=collector_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        topo_ref.tell(UnsubscribeTopology(subscriber=collector_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        count = len(snapshots)
        # Trigger a state change
        topo_ref.tell(  # type: ignore[arg-type]
            JoinRequest(
                node=OTHER_NODE,
                roles=frozenset(),
                node_id="node-2",
            ),
        )
        await asyncio.sleep(0.1)

        assert len(snapshots) == count


async def test_topology_actor_gossip_tick_sends_to_peers() -> None:
    """GossipTick triggers gossip to random peer (no crash without transport)."""
    initial_state = make_state(self_member(), other_member())

    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(
            make_topology_actor(state=initial_state),
            "_topology",
        )
        # GossipTick without remote_transport should not crash
        topo_ref.tell(GossipTick())  # type: ignore[arg-type]
        await asyncio.sleep(0.1)


async def test_topology_actor_heartbeat_tick_sends_heartbeats() -> None:
    """HeartbeatTick triggers heartbeat to all members (no crash without transport)."""
    initial_state = make_state(self_member(), other_member())

    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(
            make_topology_actor(state=initial_state),
            "_topology",
        )
        topo_ref.tell(HeartbeatTick(members=frozenset({OTHER_NODE})))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)


async def test_topology_actor_waiters_notified() -> None:
    """WaitForMembers resolves when enough members are UP."""
    async with ActorSystem(name="test") as system:
        topo_ref = system.spawn(make_topology_actor(), "_topology")

        # Ask for 2 UP members (only 1 currently)
        result_future: asyncio.Future[ClusterState] = (
            asyncio.get_running_loop().create_future()
        )

        def waiter_behavior() -> Behavior[ClusterState]:
            async def receive(
                ctx: ActorContext[ClusterState],
                msg: ClusterState,
            ) -> Behavior[ClusterState]:
                if not result_future.done():
                    result_future.set_result(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        waiter_ref: ActorRef[ClusterState] = system.spawn(
            waiter_behavior(),
            "waiter",
        )
        topo_ref.tell(WaitForMembers(n=2, reply_to=waiter_ref))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)

        assert not result_future.done()

        # Now add a second UP member via gossip
        remote_state = make_state(
            self_member(),
            other_member(MemberStatus.up),
        )
        topo_ref.tell(  # type: ignore[arg-type]
            GossipMessage(state=remote_state, from_node=OTHER_NODE),
        )
        await asyncio.sleep(0.1)

        # Waiter should be notified
        result = await asyncio.wait_for(result_future, timeout=2.0)
        up_count = sum(1 for m in result.members if m.status == MemberStatus.up)
        assert up_count >= 2
