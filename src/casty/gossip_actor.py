# src/casty/_gossip_actor.py
from __future__ import annotations

import logging
import random
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.address import ActorAddress
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
)
from casty.ref import ActorRef

if TYPE_CHECKING:
    from casty.remote_transport import RemoteTransport
    from casty.replication import ShardAllocation


@dataclass(frozen=True)
class GossipMessage:
    state: ClusterState
    from_node: NodeAddress
    is_reply: bool = False


@dataclass(frozen=True)
class GetClusterState:
    reply_to: ActorRef[ClusterState]


@dataclass(frozen=True)
class JoinRequest:
    node: NodeAddress
    roles: frozenset[str]
    reply_to: ActorRef[JoinAccepted] | None = None


@dataclass(frozen=True)
class JoinAccepted:
    state: ClusterState


@dataclass(frozen=True)
class GossipTick:
    """Periodic trigger to push gossip to a random peer."""


@dataclass(frozen=True)
class PromoteMember:
    """Leader tells gossip to promote a joining member to up."""
    address: NodeAddress


@dataclass(frozen=True)
class UpdateShardAllocations:
    """Published by coordinator leader → gossip for cluster-wide propagation."""
    shard_type: str
    allocations: dict[int, ShardAllocation]
    epoch: int


type GossipMsg = GossipMessage | GetClusterState | JoinRequest | JoinAccepted | GossipTick | PromoteMember | UpdateShardAllocations


def gossip_actor(
    *,
    self_node: NodeAddress,
    initial_state: ClusterState,
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
    logger: logging.Logger | None = None,
    fanout: int = 3,
) -> Behavior[GossipMsg]:
    log = logger or logging.getLogger(f"casty.gossip.{system_name}")

    def active(state: ClusterState) -> Behavior[GossipMsg]:
        async def receive(ctx: Any, msg: GossipMsg) -> Any:
            match msg:
                case GossipMessage(remote_state, from_node, is_reply):
                    log.debug("Gossip from %s:%d (members=%d, reply=%s)", from_node.host, from_node.port, len(remote_state.members), is_reply)
                    merged_version = state.version.merge(remote_state.version)
                    all_addresses = {m.address for m in state.members} | {
                        m.address for m in remote_state.members
                    }
                    merged_members: set[Member] = set()
                    for addr in all_addresses:
                        local_m = next(
                            (m for m in state.members if m.address == addr), None
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
                                # Concurrent: higher status wins (joining < up < leaving < down < removed)
                                if remote_m.status.value > local_m.status.value:
                                    merged_members.add(remote_m)
                                else:
                                    merged_members.add(local_m)
                        elif local_m:
                            merged_members.add(local_m)
                        elif remote_m:
                            merged_members.add(remote_m)

                    # Merge shard allocations: higher epoch wins
                    if remote_state.allocation_epoch > state.allocation_epoch:
                        merged_allocs = remote_state.shard_allocations
                        merged_epoch = remote_state.allocation_epoch
                    else:
                        merged_allocs = state.shard_allocations
                        merged_epoch = state.allocation_epoch

                    members_changed = frozenset(merged_members) != state.members
                    if members_changed:
                        merged_seen = frozenset({self_node, from_node})
                    else:
                        merged_seen = state.seen | remote_state.seen | {self_node, from_node}

                    new_state = ClusterState(
                        members=frozenset(merged_members),
                        unreachable=state.unreachable | remote_state.unreachable,
                        version=merged_version.increment(self_node),
                        shard_allocations=merged_allocs,
                        allocation_epoch=merged_epoch,
                        seen=merged_seen,
                    )

                    if not is_reply and remote_transport is not None:
                        reply_addr = ActorAddress(
                            system=system_name,
                            path="/_cluster/_gossip",
                            host=from_node.host,
                            port=from_node.port,
                        )
                        reply_ref: ActorRef[Any] = remote_transport.make_ref(reply_addr)
                        reply_ref.tell(GossipMessage(state=new_state, from_node=self_node, is_reply=True))

                    return active(new_state)

                case GetClusterState(reply_to):
                    reply_to.tell(state)
                    return Behaviors.same()

                case JoinRequest(node, roles, reply_to):
                    if node == self_node:
                        if reply_to is not None:
                            reply_to.tell(JoinAccepted(state=state))
                        return Behaviors.same()
                    log.info("Join request from %s:%d", node.host, node.port)
                    new_member = Member(
                        address=node,
                        status=MemberStatus.joining,
                        roles=roles,
                    )
                    existing = frozenset(m for m in state.members if m.address != node)
                    new_state = ClusterState(
                        members=existing | {new_member},
                        unreachable=state.unreachable,
                        version=state.version.increment(self_node),
                        shard_allocations=state.shard_allocations,
                        allocation_epoch=state.allocation_epoch,
                        seen=frozenset({self_node}),
                    )
                    if reply_to is not None:
                        reply_to.tell(JoinAccepted(state=new_state))
                    return active(new_state)

                case JoinAccepted(accepted_state):
                    log.info("Join accepted (members=%d)", len(accepted_state.members))
                    # Merge accepted state from seed — higher epoch wins
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
                    )
                    return active(new_state)

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
                            targets = random.sample(candidates, min(fanout, len(candidates)))
                            for target_node in targets:
                                log.debug("Gossip -> %s:%d", target_node.host, target_node.port)
                                gossip_addr = ActorAddress(
                                    system=system_name,
                                    path="/_cluster/_gossip",
                                    host=target_node.host,
                                    port=target_node.port,
                                )
                                gossip_ref: ActorRef[Any] = remote_transport.make_ref(gossip_addr)
                                gossip_ref.tell(GossipMessage(state=state, from_node=self_node))
                    return Behaviors.same()

                case PromoteMember(address):
                    promoted = frozenset(
                        Member(address=m.address, status=MemberStatus.up, roles=m.roles)
                        if m.address == address and m.status == MemberStatus.joining
                        else m
                        for m in state.members
                    )
                    if promoted != state.members:
                        log.info("Promoted %s:%d -> up", address.host, address.port)
                        new_state = ClusterState(
                            members=promoted,
                            unreachable=state.unreachable,
                            version=state.version.increment(self_node),
                            shard_allocations=state.shard_allocations,
                            allocation_epoch=state.allocation_epoch,
                            seen=frozenset({self_node}),
                        )
                        return active(new_state)
                    return Behaviors.same()

                case UpdateShardAllocations(shard_type, allocations, epoch):
                    if epoch > state.allocation_epoch:
                        log.debug("Shard allocations updated: %s epoch %d", shard_type, epoch)
                        new_state = state.with_allocations(
                            shard_type, allocations, epoch
                        )
                        return active(new_state)
                    return Behaviors.same()

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(initial_state)
