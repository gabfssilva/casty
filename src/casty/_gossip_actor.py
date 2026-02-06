# src/casty/_gossip_actor.py
from __future__ import annotations

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


@dataclass(frozen=True)
class GossipMessage:
    state: ClusterState
    from_node: NodeAddress


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


type GossipMsg = GossipMessage | GetClusterState | JoinRequest | JoinAccepted | GossipTick


def gossip_actor(
    *,
    self_node: NodeAddress,
    initial_state: ClusterState,
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
) -> Behavior[GossipMsg]:
    def active(state: ClusterState) -> Behavior[GossipMsg]:
        async def receive(ctx: Any, msg: GossipMsg) -> Any:
            match msg:
                case GossipMessage(remote_state, _):
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
                            else:
                                merged_members.add(local_m)
                        elif local_m:
                            merged_members.add(local_m)
                        elif remote_m:
                            merged_members.add(remote_m)

                    new_state = ClusterState(
                        members=frozenset(merged_members),
                        unreachable=state.unreachable | remote_state.unreachable,
                        version=merged_version.increment(self_node),
                    )
                    return active(new_state)

                case GetClusterState(reply_to):
                    reply_to.tell(state)
                    return Behaviors.same()

                case JoinRequest(node, roles, reply_to):
                    new_member = Member(
                        address=node,
                        status=MemberStatus.joining,
                        roles=roles,
                    )
                    new_state = ClusterState(
                        members=state.members | {new_member},
                        unreachable=state.unreachable,
                        version=state.version.increment(self_node),
                    )
                    if reply_to is not None:
                        reply_to.tell(JoinAccepted(state=new_state))
                    return active(new_state)

                case JoinAccepted(accepted_state):
                    # Merge accepted state from seed
                    merged_version = state.version.merge(accepted_state.version)
                    new_state = ClusterState(
                        members=state.members | accepted_state.members,
                        unreachable=state.unreachable,
                        version=merged_version.increment(self_node),
                    )
                    return active(new_state)

                case GossipTick():
                    _push_gossip(state, self_node, remote_transport, system_name)
                    return Behaviors.same()

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(initial_state)


def _push_gossip(
    state: ClusterState,
    self_node: NodeAddress,
    remote_transport: RemoteTransport | None,
    system_name: str,
) -> None:
    """Pick a random up member and send GossipMessage via TCP."""
    if remote_transport is None:
        return

    peers = [
        m.address
        for m in state.members
        if m.address != self_node
        and m.status in (MemberStatus.up, MemberStatus.joining)
    ]
    if not peers:
        return

    target_node = random.choice(peers)
    gossip_addr = ActorAddress(
        system=system_name,
        path="/_cluster/_gossip",
        host=target_node.host,
        port=target_node.port,
    )
    gossip_ref: ActorRef[Any] = remote_transport.make_ref(gossip_addr)
    gossip_ref.tell(GossipMessage(state=state, from_node=self_node))
