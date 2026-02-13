"""Verify unreachable dedup in topology_actor.

The topology_actor internally deduplicates NodeUnreachable events from
the failure detector.  Stale gossip or repeated heartbeat misses should
NOT cause duplicate DownMember cascades.
"""

from __future__ import annotations

import asyncio
from typing import Any

from casty import ActorSystem, Behaviors
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    VectorClock,
)
from casty.failure_detector import PhiAccrualFailureDetector
from casty.topology import SubscribeTopology, TopologySnapshot
from casty.topology_actor import (
    DownMember,
    GetState,
    GossipMessage,
    JoinRequest,
    topology_actor,
)


NODE_SELF = NodeAddress(host="10.0.0.1", port=25520)
NODE_DEAD = NodeAddress(host="10.0.0.2", port=25520)


def make_initial_state() -> ClusterState:
    return ClusterState(
        members=frozenset({
            Member(
                address=NODE_SELF, status=MemberStatus.up,
                roles=frozenset(), id="self",
            ),
            Member(
                address=NODE_DEAD, status=MemberStatus.up,
                roles=frozenset(), id="dead",
            ),
        }),
        version=VectorClock().increment(NODE_SELF).increment(NODE_DEAD),
    )


def make_detector() -> PhiAccrualFailureDetector:
    return PhiAccrualFailureDetector(threshold=8.0)


async def test_dedup_survives_stale_gossip() -> None:
    """Stale gossip (node still 'up') between two DownMember should not cause duplicate cascades."""
    snapshots: list[TopologySnapshot] = []

    async with ActorSystem(name="test") as system:
        async def capture(_ctx: Any, msg: Any) -> Any:
            if isinstance(msg, TopologySnapshot):
                snapshots.append(msg)
            return Behaviors.same()

        sub = system.spawn(Behaviors.receive(capture), "sub")

        topo = system.spawn(
            topology_actor(
                self_node=NODE_SELF,
                node_id="self",
                roles=frozenset(),
                initial_state=make_initial_state(),
                detector=make_detector(),
            ),
            "_topology",
        )
        await asyncio.sleep(0.1)

        topo.tell(SubscribeTopology(reply_to=sub))  # type: ignore[arg-type]
        await asyncio.sleep(0.1)
        snapshots.clear()

        topo.tell(DownMember(address=NODE_DEAD))
        await asyncio.sleep(0.1)

        # Stale gossip arrives: NODE_DEAD still 'up', older vector clock
        stale_state = ClusterState(
            members=frozenset({
                Member(
                    address=NODE_SELF, status=MemberStatus.up,
                    roles=frozenset(), id="self",
                ),
                Member(
                    address=NODE_DEAD, status=MemberStatus.up,
                    roles=frozenset(), id="dead",
                ),
            }),
            version=VectorClock().increment(NODE_SELF),
        )
        topo.tell(GossipMessage(state=stale_state, from_node=NODE_DEAD))
        await asyncio.sleep(0.1)

        # Second DownMember — should be deduped (already down)
        topo.tell(DownMember(address=NODE_DEAD))
        await asyncio.sleep(0.1)

        state: ClusterState = await system.ask(
            topo, lambda r: GetState(reply_to=r), timeout=5.0,
        )
        node_dead = next(m for m in state.members if m.address == NODE_DEAD)
        assert node_dead.status == MemberStatus.down


async def test_dedup_clears_on_rejoin() -> None:
    """After a node rejoins (joining), it must be re-detectable if it fails again."""
    async with ActorSystem(name="test") as system:
        topo = system.spawn(
            topology_actor(
                self_node=NODE_SELF,
                node_id="self",
                roles=frozenset(),
                initial_state=make_initial_state(),
                detector=make_detector(),
            ),
            "_topology",
        )
        await asyncio.sleep(0.1)

        topo.tell(DownMember(address=NODE_DEAD))
        await asyncio.sleep(0.1)

        state: ClusterState = await system.ask(
            topo, lambda r: GetState(reply_to=r), timeout=5.0,
        )
        node_dead = next(m for m in state.members if m.address == NODE_DEAD)
        assert node_dead.status == MemberStatus.down

        # Node rejoins
        topo.tell(JoinRequest(
            node=NODE_DEAD, roles=frozenset(), node_id="dead-v2",
        ))
        await asyncio.sleep(0.1)

        state = await system.ask(
            topo, lambda r: GetState(reply_to=r), timeout=5.0,
        )
        node_dead = next(m for m in state.members if m.address == NODE_DEAD)
        assert node_dead.status == MemberStatus.joining

        # Node fails AGAIN — must cascade (not deduped)
        topo.tell(DownMember(address=NODE_DEAD))
        await asyncio.sleep(0.1)

        state = await system.ask(
            topo, lambda r: GetState(reply_to=r), timeout=5.0,
        )
        node_dead = next(m for m in state.members if m.address == NODE_DEAD)
        assert node_dead.status == MemberStatus.down
