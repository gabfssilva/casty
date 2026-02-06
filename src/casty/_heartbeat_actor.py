from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.actor import Behavior, Behaviors
from casty.address import ActorAddress
from casty.cluster_state import NodeAddress
from casty.failure_detector import PhiAccrualFailureDetector
from casty.ref import ActorRef

if TYPE_CHECKING:
    from casty.remote_transport import RemoteTransport


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
    remote_transport: RemoteTransport | None = None,
    system_name: str = "",
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

            case HeartbeatTick(members):
                _send_heartbeats(
                    members, self_node, ctx.self, remote_transport, system_name
                )
                return Behaviors.same()

            case _:
                return Behaviors.same()

    return Behaviors.receive(receive)


def _send_heartbeats(
    members: frozenset[NodeAddress],
    self_node: NodeAddress,
    self_ref: ActorRef[Any],
    remote_transport: RemoteTransport | None,
    system_name: str,
) -> None:
    """Send HeartbeatRequest to each peer's heartbeat actor via TCP."""
    if remote_transport is None:
        return

    for member in members:
        if member == self_node:
            continue
        hb_addr = ActorAddress(
            system=system_name,
            path="/_cluster/_heartbeat",
            host=member.host,
            port=member.port,
        )
        hb_ref: ActorRef[Any] = remote_transport.make_ref(hb_addr)
        hb_ref.tell(HeartbeatRequest(from_node=self_node, reply_to=self_ref))
