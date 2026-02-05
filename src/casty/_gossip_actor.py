# src/casty/_gossip_actor.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from casty.actor import Behavior, Behaviors
from casty.cluster_state import (
    ClusterState,
    Member,
    MemberStatus,
    NodeAddress,
)
from casty.ref import ActorRef


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


@dataclass(frozen=True)
class JoinAccepted:
    state: ClusterState


type GossipMsg = GossipMessage | GetClusterState | JoinRequest


def gossip_actor(
    *,
    self_node: NodeAddress,
    initial_state: ClusterState,
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

                case JoinRequest(node, roles):
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
                    return active(new_state)

                case _:
                    return Behaviors.same()

        return Behaviors.receive(receive)

    return active(initial_state)
