# tests/test_gossip_actor.py
from __future__ import annotations

import asyncio

from casty import ActorSystem
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    VectorClock,
)
from casty.gossip_actor import (
    gossip_actor,
    GossipMessage,
    GetClusterState,
    JoinRequest,
)


async def test_gossip_actor_join_adds_member() -> None:
    """JoinRequest adds a new member in Joining state."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)
    initial_state = ClusterState().add_member(
        Member(address=self_node, status=MemberStatus.up, roles=frozenset(), id="self")
    )

    async with ActorSystem(name="test") as system:
        gossip_ref = system.spawn(
            gossip_actor(self_node=self_node, initial_state=initial_state),
            "gossip",
        )
        await asyncio.sleep(0.1)

        new_node = NodeAddress(host="127.0.0.2", port=25520)
        gossip_ref.tell(JoinRequest(node=new_node, roles=frozenset(), node_id="new"))
        await asyncio.sleep(0.1)

        state = await system.ask(
            gossip_ref, lambda r: GetClusterState(reply_to=r), timeout=5.0
        )

    addresses = {m.address for m in state.members}
    assert new_node in addresses


async def test_gossip_merge_updates_state() -> None:
    """Receiving a GossipMessage with newer state updates local state."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)
    other_node = NodeAddress(host="127.0.0.2", port=25520)

    initial_state = ClusterState().add_member(
        Member(address=self_node, status=MemberStatus.up, roles=frozenset(), id="self")
    )

    remote_state = (
        ClusterState()
        .add_member(Member(address=self_node, status=MemberStatus.up, roles=frozenset(), id="self"))
        .add_member(Member(address=other_node, status=MemberStatus.up, roles=frozenset(), id="other"))
    )
    remote_state = ClusterState(
        members=remote_state.members,
        unreachable=remote_state.unreachable,
        version=VectorClock().increment(other_node),
    )

    async with ActorSystem(name="test") as system:
        gossip_ref = system.spawn(
            gossip_actor(self_node=self_node, initial_state=initial_state),
            "gossip",
        )
        await asyncio.sleep(0.1)

        gossip_ref.tell(GossipMessage(state=remote_state, from_node=other_node))
        await asyncio.sleep(0.1)

        state = await system.ask(
            gossip_ref, lambda r: GetClusterState(reply_to=r), timeout=5.0
        )

    addresses = {m.address for m in state.members}
    assert other_node in addresses
