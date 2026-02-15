from __future__ import annotations

import asyncio
import logging
from typing import Any

from casty import (
    ActorContext,
    ActorRef,
    ActorSystem,
    Behavior,
    Behaviors,
    NodeAddress,
)
from casty.cluster.topology import SubscribeTopology, TopologySnapshot
from casty.cluster.topology_actor import TopologyMsg
from casty.cluster.singleton import singleton_manager_actor

from casty import Member, MemberStatus


SELF_NODE = NodeAddress(host="127.0.0.1", port=2551)
OTHER_NODE = NodeAddress(host="127.0.0.1", port=2552)


def make_snapshot(
    *,
    leader: NodeAddress | None = SELF_NODE,
    members: frozenset[Member] | None = None,
    unreachable: frozenset[NodeAddress] | None = None,
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
        shard_allocations={},
        allocation_epoch=0,
        unreachable=unreachable or frozenset(),
        registry=frozenset(),
    )


def fake_topology_actor() -> Behavior[TopologyMsg]:
    """Fake topology actor that captures subscriptions."""

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
                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(None)


async def test_singleton_activates_on_leader_snapshot() -> None:
    """Receives snapshot where leader == self_node, spawns child and forwards messages."""
    async with ActorSystem(name="test") as system:
        pongs: list[str] = []

        def pong_behavior() -> Behavior[Any]:
            async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                match msg:
                    case ("ping", reply_to):
                        reply_to.tell("pong")
                        return Behaviors.same()
                    case _:
                        return Behaviors.same()

            return Behaviors.receive(receive)

        def pong_collector() -> Behavior[str]:
            async def receive(ctx: ActorContext[str], msg: str) -> Behavior[str]:
                pongs.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        topo_ref = system.spawn(fake_topology_actor(), "_topology")

        mgr_ref = system.spawn(
            singleton_manager_actor(
                factory=pong_behavior,
                name="test-singleton",
                remote_transport=None,
                system_name="test",
                logger=logging.getLogger("test"),
                topology_ref=topo_ref,  # type: ignore[arg-type]
                self_node=SELF_NODE,
            ),
            "_singleton-test-singleton",
        )
        await asyncio.sleep(0.1)

        # Send snapshot where self_node is leader — should activate
        snapshot = make_snapshot(leader=SELF_NODE)
        mgr_ref.tell(snapshot)
        await asyncio.sleep(0.1)

        # Send a message to the singleton — should be forwarded to child
        collector_ref = system.spawn(pong_collector(), "_pong")
        mgr_ref.tell(("ping", collector_ref))
        await asyncio.sleep(0.1)

        assert len(pongs) == 1
        assert pongs[0] == "pong"


async def test_singleton_goes_standby_on_follower_snapshot() -> None:
    """Receives snapshot where leader != self_node, transitions to standby."""
    async with ActorSystem(name="test") as system:
        forwarded: list[Any] = []

        def capture_behavior() -> Behavior[Any]:
            async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                forwarded.append(msg)
                return Behaviors.same()

            return Behaviors.receive(receive)

        topo_ref = system.spawn(fake_topology_actor(), "_topology")

        mgr_ref = system.spawn(
            singleton_manager_actor(
                factory=lambda: capture_behavior(),
                name="test-singleton",
                remote_transport=None,
                system_name="test",
                logger=logging.getLogger("test"),
                topology_ref=topo_ref,  # type: ignore[arg-type]
                self_node=SELF_NODE,
            ),
            "_singleton-test-singleton",
        )
        await asyncio.sleep(0.1)

        # Send snapshot where OTHER_NODE is leader — should go standby
        snapshot = make_snapshot(leader=OTHER_NODE)
        mgr_ref.tell(snapshot)
        await asyncio.sleep(0.1)

        # In standby without remote_transport, messages go nowhere (no child spawned)
        # The singleton should NOT have spawned a child
        mgr_ref.tell("hello")
        await asyncio.sleep(0.1)

        # No child was spawned, so forwarded should be empty
        assert len(forwarded) == 0


async def test_singleton_transitions_active_to_standby() -> None:
    """Leader changes: stops child, goes to standby."""
    async with ActorSystem(name="test") as system:
        spawned: list[str] = []

        def tracked_behavior() -> Behavior[Any]:
            async def receive(ctx: ActorContext[Any], msg: Any) -> Behavior[Any]:
                return Behaviors.same()

            spawned.append("spawned")
            return Behaviors.receive(receive)

        topo_ref = system.spawn(fake_topology_actor(), "_topology")

        mgr_ref = system.spawn(
            singleton_manager_actor(
                factory=tracked_behavior,
                name="test-singleton",
                remote_transport=None,
                system_name="test",
                logger=logging.getLogger("test"),
                topology_ref=topo_ref,  # type: ignore[arg-type]
                self_node=SELF_NODE,
            ),
            "_singleton-test-singleton",
        )
        await asyncio.sleep(0.1)

        # Activate as leader
        snapshot1 = make_snapshot(leader=SELF_NODE)
        mgr_ref.tell(snapshot1)
        await asyncio.sleep(0.1)
        assert len(spawned) == 1

        # Demote — leader changes to OTHER_NODE
        snapshot2 = make_snapshot(leader=OTHER_NODE)
        mgr_ref.tell(snapshot2)
        await asyncio.sleep(0.1)

        # Re-promote — should spawn a new child
        snapshot3 = make_snapshot(leader=SELF_NODE)
        mgr_ref.tell(snapshot3)
        await asyncio.sleep(0.1)
        assert len(spawned) == 2
