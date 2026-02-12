# tests/test_gossip_actor.py
from __future__ import annotations

import asyncio

from casty import ActorSystem
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    ServiceEntry,
    VectorClock,
)
from casty.gossip_actor import (
    DownMember,
    gossip_actor,
    GossipMessage,
    GetClusterState,
    JoinRequest,
    UpdateRegistry,
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


async def test_gossip_merges_registry_from_remote() -> None:
    """Remote gossip state with registry entries should be merged."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)
    remote_node = NodeAddress(host="127.0.0.2", port=25520)
    initial_state = ClusterState().add_member(
        Member(address=self_node, status=MemberStatus.up, roles=frozenset(), id="self")
    ).add_member(
        Member(address=remote_node, status=MemberStatus.up, roles=frozenset(), id="remote")
    )

    async with ActorSystem(name="test") as system:
        gossip_ref = system.spawn(
            gossip_actor(self_node=self_node, initial_state=initial_state),
            "gossip",
        )
        await asyncio.sleep(0.1)

        remote_entry = ServiceEntry(key="payment", node=remote_node, path="/payment")
        remote_state = ClusterState(
            members=initial_state.members,
            version=initial_state.version,
            registry=frozenset({remote_entry}),
        )
        gossip_ref.tell(GossipMessage(state=remote_state, from_node=remote_node))
        await asyncio.sleep(0.1)

        state = await system.ask(
            gossip_ref, lambda r: GetClusterState(reply_to=r), timeout=5.0
        )

    assert remote_entry in state.registry


async def test_gossip_update_registry_adds_local_entries() -> None:
    """UpdateRegistry message should add entries to local state."""
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

        entry = ServiceEntry(key="payment", node=self_node, path="/payment")
        gossip_ref.tell(UpdateRegistry(entries=frozenset({entry})))
        await asyncio.sleep(0.1)

        state = await system.ask(
            gossip_ref, lambda r: GetClusterState(reply_to=r), timeout=5.0
        )

    assert entry in state.registry


async def test_gossip_prunes_registry_entries_from_down_nodes() -> None:
    """Registry entries from down/removed nodes should be pruned during merge."""
    self_node = NodeAddress(host="127.0.0.1", port=25520)
    down_node = NodeAddress(host="127.0.0.3", port=25520)
    entry_self = ServiceEntry(key="alive", node=self_node, path="/alive")
    entry_down = ServiceEntry(key="dead", node=down_node, path="/dead")

    initial_state = ClusterState(
        members=frozenset({
            Member(address=self_node, status=MemberStatus.up, roles=frozenset(), id="self"),
            Member(address=down_node, status=MemberStatus.up, roles=frozenset(), id="down"),
        }),
        registry=frozenset({entry_self, entry_down}),
    )

    async with ActorSystem(name="test") as system:
        gossip_ref = system.spawn(
            gossip_actor(self_node=self_node, initial_state=initial_state),
            "gossip",
        )
        await asyncio.sleep(0.1)

        gossip_ref.tell(DownMember(address=down_node))
        await asyncio.sleep(0.1)

        state = await system.ask(
            gossip_ref, lambda r: GetClusterState(reply_to=r), timeout=5.0
        )

    assert entry_self in state.registry
    assert entry_down not in state.registry
