"""Centralized cluster membership actor.

Owns the canonical ``ClusterState``, runs CRDT merge + phi accrual failure
detection + subscribe/push distribution.  All cluster consumers self-subscribe
via ``SubscribeTopology`` and receive ``TopologySnapshot`` pushes.
"""

from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.core.address import ActorAddress
from casty.cluster.state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
    NodeId,
    ServiceEntry,
    ShardAllocation,
)
from casty.cluster.failure_detector import PhiAccrualFailureDetector
from casty.ref import ActorRef
from casty.cluster.topology import (
    SubscribeTopology,
    TopologySnapshot,
    UnsubscribeTopology,
)

if TYPE_CHECKING:
    from casty.context import ActorContext
    from casty.remote.tcp_transport import RemoteTransport


@dataclass(frozen=True)
class GossipMessage:
    state: ClusterState
    from_node: NodeAddress
    is_reply: bool = False


@dataclass(frozen=True)
class JoinRequest:
    node: NodeAddress
    roles: frozenset[str]
    node_id: NodeId
    reply_to: ActorRef[JoinAccepted] | None = None


@dataclass(frozen=True)
class JoinAccepted:
    state: ClusterState


@dataclass(frozen=True)
class GossipTick:
    """Periodic trigger to push gossip to a random peer."""


@dataclass(frozen=True)
class PromoteMember:
    """Leader tells topology to promote a joining member to up."""

    address: NodeAddress


@dataclass(frozen=True)
class DownMember:
    """Mark a member as down (unreachable -> removed from leader election)."""

    address: NodeAddress


@dataclass(frozen=True)
class UpdateShardAllocations:
    """Published by coordinator leader -> topology for cluster-wide propagation."""

    shard_type: str
    allocations: dict[int, ShardAllocation]
    epoch: int


@dataclass(frozen=True)
class UpdateRegistry:
    """Update the local service registry (from receptionist)."""

    entries: frozenset[ServiceEntry]


@dataclass(frozen=True)
class ResolveNode:
    """Query to resolve a ``NodeId`` to its ``NodeAddress``."""

    node_id: NodeId
    reply_to: ActorRef[NodeAddress | None]


@dataclass(frozen=True)
class HeartbeatTick:
    """Periodic trigger to send heartbeats."""

    members: frozenset[NodeAddress]


@dataclass(frozen=True)
class HeartbeatRequest:
    from_node: NodeAddress
    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class HeartbeatResponse:
    from_node: NodeAddress


@dataclass(frozen=True)
class CheckAvailability:
    """Request to check which nodes are unreachable."""

    reply_to: ActorRef[Any]


@dataclass(frozen=True)
class GetState:
    reply_to: ActorRef[ClusterState]


@dataclass(frozen=True)
class WaitForMembers:
    n: int
    reply_to: ActorRef[ClusterState]


type TopologyMsg = (
    GossipMessage
    | GossipTick
    | JoinRequest
    | JoinAccepted
    | HeartbeatTick
    | HeartbeatRequest
    | HeartbeatResponse
    | CheckAvailability
    | PromoteMember
    | DownMember
    | SubscribeTopology
    | UnsubscribeTopology
    | UpdateShardAllocations
    | UpdateRegistry
    | GetState
    | WaitForMembers
)


def build_snapshot(state: ClusterState) -> TopologySnapshot:
    return TopologySnapshot(
        members=state.members,
        leader=state.leader,
        shard_allocations=state.shard_allocations,
        allocation_epoch=state.allocation_epoch,
        unreachable=state.unreachable,
        registry=state.registry,
    )


def push_to_subscribers(
    snapshot: TopologySnapshot,
    subscribers: frozenset[ActorRef[TopologySnapshot]],
) -> None:
    for sub in subscribers:
        sub.tell(snapshot)


def notify_waiters(
    state: ClusterState,
    waiters: tuple[WaitForMembers, ...],
) -> tuple[WaitForMembers, ...]:
    up_count = sum(1 for m in state.members if m.status == MemberStatus.up)
    remaining: list[WaitForMembers] = []
    for w in waiters:
        if up_count >= w.n:
            w.reply_to.tell(state)
        else:
            remaining.append(w)
    return tuple(remaining)


def merge_gossip(
    state: ClusterState,
    remote_state: ClusterState,
    self_node: NodeAddress,
    from_node: NodeAddress,
) -> ClusterState:
    merged_version = state.version.merge(remote_state.version)
    all_addresses = {m.address for m in state.members} | {
        m.address for m in remote_state.members
    }
    merged_members: set[Member] = set()
    for addr in all_addresses:
        local_m = next(
            (m for m in state.members if m.address == addr),
            None,
        )
        remote_m = next(
            (m for m in remote_state.members if m.address == addr),
            None,
        )
        if local_m and remote_m:
            if state.version.is_before(remote_state.version):
                merged_members.add(remote_m)
            elif remote_state.version.is_before(state.version):
                merged_members.add(local_m)
            else:
                merged_members.add(
                    max(local_m, remote_m, key=lambda m: m.status.merge_priority),
                )
        elif local_m:
            merged_members.add(local_m)
        elif remote_m:
            merged_members.add(remote_m)

    if remote_state.allocation_epoch > state.allocation_epoch:
        merged_allocs = remote_state.shard_allocations
        merged_epoch = remote_state.allocation_epoch
    else:
        merged_allocs = state.shard_allocations
        merged_epoch = state.allocation_epoch

    down_nodes = frozenset(
        m.address
        for m in merged_members
        if m.status in (MemberStatus.down, MemberStatus.removed)
    )
    merged_registry = frozenset(
        e for e in (state.registry | remote_state.registry) if e.node not in down_nodes
    )

    members_changed = frozenset(merged_members) != state.members
    if members_changed:
        merged_seen = frozenset({self_node, from_node})
    else:
        merged_seen = state.seen | remote_state.seen | {self_node, from_node}

    alive_addresses = frozenset(
        m.address
        for m in merged_members
        if m.status in (MemberStatus.joining, MemberStatus.up, MemberStatus.leaving)
    )
    merged_unreachable = (
        state.unreachable | remote_state.unreachable
    ) - alive_addresses

    return ClusterState(
        members=frozenset(merged_members),
        unreachable=merged_unreachable,
        version=merged_version.increment(self_node),
        shard_allocations=merged_allocs,
        allocation_epoch=merged_epoch,
        seen=merged_seen,
        registry=merged_registry,
    )


def topology_actor(
    *,
    self_node: NodeAddress,
    node_id: NodeId,
    roles: frozenset[str],
    initial_state: ClusterState,
    detector: PhiAccrualFailureDetector,
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
    fanout: int = 3,
    logger: logging.Logger | None = None,
) -> Behavior[TopologyMsg]:
    log = logger or logging.getLogger(f"casty.topology.{system_name}")

    def active(
        state: ClusterState,
        subscribers: frozenset[ActorRef[TopologySnapshot]],
        waiters: tuple[WaitForMembers, ...],
        handled_unreachable: frozenset[NodeAddress],
    ) -> Behavior[TopologyMsg]:
        async def receive(
            ctx: ActorContext[TopologyMsg],
            msg: TopologyMsg,
        ) -> Behavior[TopologyMsg]:
            match msg:
                case SubscribeTopology(reply_to=reply_to):
                    reply_to.tell(build_snapshot(state))
                    return active(
                        state,
                        subscribers | {reply_to},
                        waiters,
                        handled_unreachable,
                    )

                case UnsubscribeTopology(subscriber=subscriber):
                    return active(
                        state,
                        subscribers - {subscriber},
                        waiters,
                        handled_unreachable,
                    )

                case GossipMessage(remote_state, from_node, is_reply):
                    log.debug(
                        "Gossip from %s:%d (members=%d, reply=%s)",
                        from_node.host,
                        from_node.port,
                        len(remote_state.members),
                        is_reply,
                    )
                    new_state = merge_gossip(
                        state,
                        remote_state,
                        self_node,
                        from_node,
                    )

                    if not is_reply and remote_transport is not None:
                        reply_addr = ActorAddress(
                            system=system_name,
                            path="/_cluster/_topology",
                            host=from_node.host,
                            port=from_node.port,
                        )
                        reply_ref: ActorRef[Any] = remote_transport.make_ref(
                            reply_addr,
                        )
                        reply_ref.tell(
                            GossipMessage(
                                state=new_state,
                                from_node=self_node,
                                is_reply=True,
                            ),
                        )

                    if new_state.leader == self_node and new_state.is_converged:
                        for m in new_state.members:
                            if m.status == MemberStatus.joining:
                                log.info(
                                    "Promoting %s:%d -> up",
                                    m.address.host,
                                    m.address.port,
                                )
                                ctx.self.tell(PromoteMember(address=m.address))

                    snapshot = build_snapshot(new_state)
                    push_to_subscribers(snapshot, subscribers)
                    remaining = notify_waiters(new_state, waiters)

                    rejoined = frozenset(
                        m.address
                        for m in new_state.members
                        if m.address in handled_unreachable
                        and m.status == MemberStatus.joining
                    )
                    return active(
                        new_state,
                        subscribers,
                        remaining,
                        handled_unreachable - rejoined,
                    )

                case GossipTick():
                    if remote_transport is not None:
                        peers = [
                            m.address
                            for m in state.members
                            if m.address != self_node
                            and m.status in (MemberStatus.up, MemberStatus.joining)
                        ]
                        if peers:
                            unseen = [p for p in peers if p not in state.seen]
                            candidates = unseen if unseen else peers
                            targets = random.sample(
                                candidates,
                                min(fanout, len(candidates)),
                            )
                            for target_node in targets:
                                log.debug(
                                    "Gossip -> %s:%d",
                                    target_node.host,
                                    target_node.port,
                                )
                                gossip_addr = ActorAddress(
                                    system=system_name,
                                    path="/_cluster/_topology",
                                    host=target_node.host,
                                    port=target_node.port,
                                )
                                gossip_ref: ActorRef[Any] = remote_transport.make_ref(
                                    gossip_addr
                                )
                                gossip_ref.tell(
                                    GossipMessage(
                                        state=state,
                                        from_node=self_node,
                                    ),
                                )
                    return Behaviors.same()

                case JoinRequest(
                    node=node,
                    roles=join_roles,
                    node_id=nid,
                    reply_to=reply_to,
                ):
                    if node == self_node:
                        if reply_to is not None:
                            reply_to.tell(JoinAccepted(state=state))
                        return Behaviors.same()
                    log.info(
                        "Join request from %s:%d (id=%s)",
                        node.host,
                        node.port,
                        nid,
                    )
                    if remote_transport is not None:
                        remote_transport.clear_blacklist(node.host, node.port)
                    new_member = Member(
                        address=node,
                        status=MemberStatus.joining,
                        roles=join_roles,
                        id=nid,
                    )
                    existing = frozenset(m for m in state.members if m.address != node)
                    new_state = ClusterState(
                        members=existing | {new_member},
                        unreachable=state.unreachable - {node},
                        version=state.version.increment(self_node),
                        shard_allocations=state.shard_allocations,
                        allocation_epoch=state.allocation_epoch,
                        seen=frozenset({self_node}),
                        registry=state.registry,
                    )
                    if reply_to is not None:
                        reply_to.tell(JoinAccepted(state=new_state))
                    snapshot = build_snapshot(new_state)
                    push_to_subscribers(snapshot, subscribers)
                    remaining = notify_waiters(new_state, waiters)
                    return active(
                        new_state,
                        subscribers,
                        remaining,
                        handled_unreachable - {node},
                    )

                case JoinAccepted(accepted_state):
                    log.info(
                        "Join accepted (members=%d)",
                        len(accepted_state.members),
                    )
                    merged_version = state.version.merge(accepted_state.version)
                    if accepted_state.allocation_epoch > state.allocation_epoch:
                        merged_allocs = accepted_state.shard_allocations
                        merged_epoch = accepted_state.allocation_epoch
                    else:
                        merged_allocs = state.shard_allocations
                        merged_epoch = state.allocation_epoch
                    new_state = ClusterState(
                        members=state.merge_members(accepted_state),
                        unreachable=state.unreachable,
                        version=merged_version.increment(self_node),
                        shard_allocations=merged_allocs,
                        allocation_epoch=merged_epoch,
                        seen=frozenset({self_node}),
                        registry=state.registry | accepted_state.registry,
                    )
                    snapshot = build_snapshot(new_state)
                    push_to_subscribers(snapshot, subscribers)
                    remaining = notify_waiters(new_state, waiters)
                    return active(
                        new_state,
                        subscribers,
                        remaining,
                        handled_unreachable,
                    )

                case HeartbeatRequest(from_node=from_node, reply_to=reply_to):
                    reply_to.tell(
                        HeartbeatResponse(from_node=self_node),
                    )
                    return Behaviors.same()

                case HeartbeatResponse(from_node=from_node):
                    detector.heartbeat(
                        f"{from_node.host}:{from_node.port}",
                    )
                    return Behaviors.same()

                case HeartbeatTick(members=tick_members):
                    current = frozenset(
                        m.address
                        for m in state.members
                        if m.status in (MemberStatus.up, MemberStatus.joining)
                    ) - {self_node}
                    removed = tick_members - current - {self_node}
                    for gone in removed:
                        detector.remove(f"{gone.host}:{gone.port}")
                    if remote_transport is not None:
                        for member in current:
                            hb_addr = ActorAddress(
                                system=system_name,
                                path="/_cluster/_topology",
                                host=member.host,
                                port=member.port,
                            )
                            hb_ref: ActorRef[Any] = remote_transport.make_ref(hb_addr)
                            hb_ref.tell(
                                HeartbeatRequest(
                                    from_node=self_node,
                                    reply_to=ctx.self,
                                ),
                            )
                    return Behaviors.same()

                case CheckAvailability():
                    new_unreachable: set[NodeAddress] = set()
                    for member in state.members:
                        if member.address == self_node:
                            continue
                        node_key = f"{member.address.host}:{member.address.port}"
                        if not detector.is_available(node_key):
                            if member.address not in handled_unreachable:
                                log.warning(
                                    "Node unreachable: %s (phi=%.1f)",
                                    node_key,
                                    detector.phi(node_key),
                                )
                                new_unreachable.add(member.address)

                    if not new_unreachable:
                        return Behaviors.same()

                    new_state = state
                    for node in new_unreachable:
                        new_state = new_state.mark_unreachable(node)

                    for node in new_unreachable:
                        ctx.self.tell(DownMember(address=node))

                    snapshot = build_snapshot(new_state)
                    push_to_subscribers(snapshot, subscribers)
                    return active(
                        new_state,
                        subscribers,
                        waiters,
                        handled_unreachable | frozenset(new_unreachable),
                    )

                case PromoteMember(address=address):
                    promoted = frozenset(
                        Member(
                            address=m.address,
                            status=MemberStatus.up,
                            roles=m.roles,
                            id=m.id,
                        )
                        if m.address == address and m.status == MemberStatus.joining
                        else m
                        for m in state.members
                    )
                    if promoted != state.members:
                        log.info(
                            "Promoted %s:%d -> up",
                            address.host,
                            address.port,
                        )
                        new_state = ClusterState(
                            members=promoted,
                            unreachable=state.unreachable - {address},
                            version=state.version.increment(self_node),
                            shard_allocations=state.shard_allocations,
                            allocation_epoch=state.allocation_epoch,
                            seen=frozenset({self_node}),
                            registry=state.registry,
                        )
                        snapshot = build_snapshot(new_state)
                        push_to_subscribers(snapshot, subscribers)
                        remaining = notify_waiters(new_state, waiters)
                        return active(
                            new_state,
                            subscribers,
                            remaining,
                            handled_unreachable,
                        )
                    return Behaviors.same()

                case DownMember(address=address):
                    downed = frozenset(
                        Member(
                            address=m.address,
                            status=MemberStatus.down,
                            roles=m.roles,
                            id=m.id,
                        )
                        if m.address == address
                        and m.status
                        in (
                            MemberStatus.up,
                            MemberStatus.joining,
                            MemberStatus.leaving,
                        )
                        else m
                        for m in state.members
                    )
                    if downed != state.members:
                        log.info(
                            "Marked %s:%d -> down",
                            address.host,
                            address.port,
                        )
                        pruned_registry = frozenset(
                            e for e in state.registry if e.node != address
                        )
                        detector.remove(
                            f"{address.host}:{address.port}",
                        )
                        new_state = ClusterState(
                            members=downed,
                            unreachable=state.unreachable | {address},
                            version=state.version.increment(self_node),
                            shard_allocations=state.shard_allocations,
                            allocation_epoch=state.allocation_epoch,
                            seen=frozenset({self_node}),
                            registry=pruned_registry,
                        )
                        snapshot = build_snapshot(new_state)
                        push_to_subscribers(snapshot, subscribers)
                        return active(
                            new_state,
                            subscribers,
                            waiters,
                            handled_unreachable,
                        )
                    return Behaviors.same()

                case UpdateShardAllocations(
                    shard_type=shard_type,
                    allocations=allocations,
                    epoch=epoch,
                ):
                    if epoch > state.allocation_epoch:
                        log.debug(
                            "Shard allocations updated: %s epoch %d",
                            shard_type,
                            epoch,
                        )
                        new_state = state.with_allocations(
                            shard_type,
                            allocations,
                            epoch,
                        )
                        snapshot = build_snapshot(new_state)
                        push_to_subscribers(snapshot, subscribers)
                        return active(
                            new_state,
                            subscribers,
                            waiters,
                            handled_unreachable,
                        )
                    return Behaviors.same()

                case UpdateRegistry(entries=entries):
                    new_registry = state.registry | entries
                    new_state = ClusterState(
                        members=state.members,
                        unreachable=state.unreachable,
                        version=state.version.increment(self_node),
                        shard_allocations=state.shard_allocations,
                        allocation_epoch=state.allocation_epoch,
                        seen=frozenset({self_node}),
                        registry=new_registry,
                    )
                    snapshot = build_snapshot(new_state)
                    push_to_subscribers(snapshot, subscribers)
                    return active(
                        new_state,
                        subscribers,
                        waiters,
                        handled_unreachable,
                    )

                case GetState(reply_to=reply_to):
                    reply_to.tell(state)
                    return Behaviors.same()

                case WaitForMembers(n=n, reply_to=reply_to):
                    up_count = sum(
                        1 for m in state.members if m.status == MemberStatus.up
                    )
                    if up_count >= n:
                        reply_to.tell(state)
                        return Behaviors.same()
                    return active(
                        state,
                        subscribers,
                        (*waiters, WaitForMembers(n=n, reply_to=reply_to)),
                        handled_unreachable,
                    )

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(
        initial_state,
        frozenset(),
        (),
        frozenset(),
    )
