from __future__ import annotations

import asyncio

from casty import ActorSystem, PhiAccrualFailureDetector
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    VectorClock,
)
from casty.topology_actor import (
    DownMember,
    GetState,
    GossipMessage,
    JoinRequest,
    PromoteMember,
    topology_actor,
)


NODE_0 = NodeAddress(host="10.0.1.10", port=25520)
NODE_1 = NodeAddress(host="10.0.1.11", port=25520)
NODE_2 = NodeAddress(host="10.0.1.12", port=25520)


def make_member(addr: NodeAddress, status: MemberStatus, node_id: str) -> Member:
    return Member(address=addr, status=status, roles=frozenset(), id=node_id)


async def test_down_member_rejoin_survives_stale_gossip() -> None:
    """A node marked 'down' that sends a JoinRequest should rejoin even if
    stale gossip arrives with the old 'down' status and a concurrent vector clock.

    Scenario:
      1. Three-node cluster, all UP
      2. node-1 goes DOWN
      3. node-1 restarts and sends JoinRequest to node-0 (seed)
      4. node-0 marks node-1 as 'joining'
      5. node-2 (still sees node-1 as 'down') gossips with node-0
      6. Bug: concurrent clocks → down > joining → node-1 reverts to 'down'
    """
    # Step 1: build a 3-node cluster where all are UP, with a shared base clock
    base_clock = (
        VectorClock()
        .increment(NODE_0)
        .increment(NODE_1)
        .increment(NODE_2)
    )
    all_up_state = ClusterState(
        members=frozenset({
            make_member(NODE_0, MemberStatus.up, "node-0"),
            make_member(NODE_1, MemberStatus.up, "node-1"),
            make_member(NODE_2, MemberStatus.up, "node-2"),
        }),
        version=base_clock,
    )

    async with ActorSystem(name="test") as system:
        # Spawn topology actor for node-0 (the seed that will receive the rejoin)
        topology_ref = system.spawn(
            topology_actor(self_node=NODE_0, node_id="node-0", roles=frozenset(), detector=PhiAccrualFailureDetector(), initial_state=all_up_state),
            "topology",
        )
        await asyncio.sleep(0.1)

        # Step 2: mark node-1 as DOWN
        topology_ref.tell(DownMember(address=NODE_1))
        await asyncio.sleep(0.1)

        state_after_down = await system.ask(
            topology_ref, lambda r: GetState(reply_to=r), timeout=5.0
        )
        node1_member = next(m for m in state_after_down.members if m.address == NODE_1)
        assert node1_member.status == MemberStatus.down

        # Step 3: node-1 restarts and sends JoinRequest
        topology_ref.tell(JoinRequest(node=NODE_1, roles=frozenset(), node_id="node-1-restarted"))
        await asyncio.sleep(0.1)

        state_after_rejoin = await system.ask(
            topology_ref, lambda r: GetState(reply_to=r), timeout=5.0
        )
        node1_after_rejoin = next(m for m in state_after_rejoin.members if m.address == NODE_1)
        assert node1_after_rejoin.status == MemberStatus.joining, (
            f"After JoinRequest, node-1 should be 'joining' but is '{node1_after_rejoin.status.name}'"
        )

        # Step 4: node-2 gossips with stale state where node-1 is still 'down'
        # node-2's clock is concurrent with node-0's: node-2 incremented its own
        # entry (from normal gossip ticks) but hasn't seen node-0's JoinRequest increment
        stale_node2_clock = base_clock.increment(NODE_0).increment(NODE_2).increment(NODE_2)
        stale_state_from_node2 = ClusterState(
            members=frozenset({
                make_member(NODE_0, MemberStatus.up, "node-0"),
                make_member(NODE_1, MemberStatus.down, "node-1"),
                make_member(NODE_2, MemberStatus.up, "node-2"),
            }),
            version=stale_node2_clock,
        )

        topology_ref.tell(GossipMessage(state=stale_state_from_node2, from_node=NODE_2))
        await asyncio.sleep(0.1)

        # Verify: node-1 should STILL be 'joining', not reverted to 'down'
        state_after_stale_gossip = await system.ask(
            topology_ref, lambda r: GetState(reply_to=r), timeout=5.0
        )
        node1_final = next(m for m in state_after_stale_gossip.members if m.address == NODE_1)
        assert node1_final.status == MemberStatus.joining, (
            f"After stale gossip, node-1 should remain 'joining' but reverted to '{node1_final.status.name}'. "
            "The CRDT merge incorrectly prefers 'down' over 'joining' on concurrent vector clocks."
        )


async def test_rejoin_clears_unreachable() -> None:
    """When a downed node rejoins, it should be removed from the unreachable set."""
    base_clock = VectorClock().increment(NODE_0).increment(NODE_1)
    initial_state = ClusterState(
        members=frozenset({
            make_member(NODE_0, MemberStatus.up, "node-0"),
            make_member(NODE_1, MemberStatus.up, "node-1"),
        }),
        version=base_clock,
    )

    async with ActorSystem(name="test") as system:
        topology_ref = system.spawn(
            topology_actor(self_node=NODE_0, node_id="node-0", roles=frozenset(), detector=PhiAccrualFailureDetector(), initial_state=initial_state),
            "topology",
        )
        await asyncio.sleep(0.1)

        topology_ref.tell(DownMember(address=NODE_1))
        await asyncio.sleep(0.1)

        state = await system.ask(
            topology_ref, lambda r: GetState(reply_to=r), timeout=5.0
        )
        assert NODE_1 in state.unreachable

        topology_ref.tell(JoinRequest(node=NODE_1, roles=frozenset(), node_id="node-1-v2"))
        await asyncio.sleep(0.1)

        state = await system.ask(
            topology_ref, lambda r: GetState(reply_to=r), timeout=5.0
        )
        assert NODE_1 not in state.unreachable, (
            f"After rejoin, node-1 should not be unreachable but is. unreachable={state.unreachable}"
        )

        topology_ref.tell(PromoteMember(address=NODE_1))
        await asyncio.sleep(0.1)

        state = await system.ask(
            topology_ref, lambda r: GetState(reply_to=r), timeout=5.0
        )
        assert NODE_1 not in state.unreachable
        node1 = next(m for m in state.members if m.address == NODE_1)
        assert node1.status == MemberStatus.up


async def test_gossip_merge_does_not_revive_stale_unreachable() -> None:
    """Gossip merge should not re-add a node to unreachable if it's already
    rejoined (joining/up) in the merged member set."""
    base_clock = (
        VectorClock()
        .increment(NODE_0)
        .increment(NODE_1)
        .increment(NODE_2)
    )
    initial_state = ClusterState(
        members=frozenset({
            make_member(NODE_0, MemberStatus.up, "node-0"),
            make_member(NODE_1, MemberStatus.up, "node-1"),
            make_member(NODE_2, MemberStatus.up, "node-2"),
        }),
        version=base_clock,
    )

    async with ActorSystem(name="test") as system:
        topology_ref = system.spawn(
            topology_actor(self_node=NODE_0, node_id="node-0", roles=frozenset(), detector=PhiAccrualFailureDetector(), initial_state=initial_state),
            "topology",
        )
        await asyncio.sleep(0.1)

        topology_ref.tell(DownMember(address=NODE_1))
        await asyncio.sleep(0.1)

        topology_ref.tell(JoinRequest(node=NODE_1, roles=frozenset(), node_id="node-1-v2"))
        await asyncio.sleep(0.1)

        state = await system.ask(
            topology_ref, lambda r: GetState(reply_to=r), timeout=5.0
        )
        assert NODE_1 not in state.unreachable

        stale_clock = base_clock.increment(NODE_2).increment(NODE_2)
        stale_gossip = ClusterState(
            members=frozenset({
                make_member(NODE_0, MemberStatus.up, "node-0"),
                make_member(NODE_1, MemberStatus.down, "node-1"),
                make_member(NODE_2, MemberStatus.up, "node-2"),
            }),
            unreachable=frozenset({NODE_1}),
            version=stale_clock,
        )
        topology_ref.tell(GossipMessage(state=stale_gossip, from_node=NODE_2))
        await asyncio.sleep(0.1)

        state = await system.ask(
            topology_ref, lambda r: GetState(reply_to=r), timeout=5.0
        )
        assert NODE_1 not in state.unreachable, (
            f"Gossip merge re-added node-1 to unreachable from stale gossip. "
            f"unreachable={state.unreachable}"
        )
