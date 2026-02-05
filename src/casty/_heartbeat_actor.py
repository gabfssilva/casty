from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from casty.actor import Behavior, Behaviors
from casty.cluster_state import NodeAddress
from casty.failure_detector import PhiAccrualFailureDetector
from casty.ref import ActorRef


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
class NodeUnreachable:
    node: NodeAddress


type HeartbeatMsg = (
    HeartbeatTick | HeartbeatRequest | HeartbeatResponse | CheckAvailability
)


def heartbeat_actor(
    *,
    self_node: NodeAddress,
    detector: PhiAccrualFailureDetector,
) -> Behavior[HeartbeatMsg]:
    async def receive(ctx: Any, msg: HeartbeatMsg) -> Any:
        match msg:
            case HeartbeatRequest(from_node, reply_to):
                reply_to.tell(HeartbeatResponse(from_node=self_node))
                return Behaviors.same()

            case HeartbeatResponse(from_node):
                detector.heartbeat(f"{from_node.host}:{from_node.port}")
                return Behaviors.same()

            case CheckAvailability(reply_to):
                for node_key in detector.tracked_nodes:
                    if not detector.is_available(node_key):
                        host, port_str = node_key.rsplit(":", 1)
                        reply_to.tell(
                            NodeUnreachable(
                                node=NodeAddress(host=host, port=int(port_str))
                            )
                        )
                return Behaviors.same()

            case HeartbeatTick():
                return Behaviors.same()

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)
