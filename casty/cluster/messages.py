from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from casty import LocalRef

from .merge.version import VectorClock
from .serializable import serializable


@serializable
@dataclass(frozen=True, slots=True)
class Node:
    id: str


@serializable
@dataclass(frozen=True, slots=True)
class Shard:
    key: str


@serializable
@dataclass(frozen=True, slots=True)
class All:
    pass


type Route = Node | Shard | All


@serializable
@dataclass(frozen=True, slots=True)
class Send:
    payload: Any
    to: Route


@serializable
@dataclass(frozen=True, slots=True)
class GetMembers:
    pass


@serializable
@dataclass(frozen=True, slots=True)
class GetNodeForKey:
    key: str


@serializable
@dataclass(frozen=True, slots=True)
class NodeJoined:
    node_id: str
    address: tuple[str, int]


@serializable
@dataclass(frozen=True, slots=True)
class NodeLeft:
    node_id: str


@serializable
@dataclass(frozen=True, slots=True)
class NodeFailed:
    node_id: str


type ClusterEvent = NodeJoined | NodeLeft | NodeFailed


@dataclass(frozen=True, slots=True)
class Subscribe:
    subscriber: LocalRef[ClusterEvent]


@dataclass(frozen=True, slots=True)
class Unsubscribe:
    subscriber: LocalRef[ClusterEvent]


@dataclass(frozen=True, slots=True)
class TransportSend:
    node_id: str
    payload: Any


@dataclass(frozen=True, slots=True)
class TransportReceived:
    node_id: str
    payload: Any


@dataclass(frozen=True, slots=True)
class TransportConnected:
    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class TransportDisconnected:
    node_id: str


type TransportEvent = TransportReceived | TransportConnected | TransportDisconnected


@serializable
@dataclass(frozen=True, slots=True)
class Ping:
    sequence: int


@serializable
@dataclass(frozen=True, slots=True)
class Ack:
    sequence: int


@serializable
@dataclass(frozen=True, slots=True)
class PingReq:
    target: str
    sequence: int


@serializable
@dataclass(frozen=True, slots=True)
class Suspect:
    node_id: str
    incarnation: int


@serializable
@dataclass(frozen=True, slots=True)
class Alive:
    node_id: str
    incarnation: int
    address: tuple[str, int]


@serializable
@dataclass(frozen=True, slots=True)
class Dead:
    node_id: str


@serializable
@dataclass(frozen=True, slots=True)
class Handshake:
    node_id: str
    address: tuple[str, int]


@serializable
@dataclass(frozen=True, slots=True)
class HandshakeAck:
    node_id: str
    address: tuple[str, int]


@serializable
@dataclass(frozen=True, slots=True)
class ReplicateState:
    """Sent after mutation to sync state between replicas."""
    actor_id: str
    version: VectorClock
    state: dict[str, Any]


@serializable
@dataclass(frozen=True, slots=True)
class ReplicateAck:
    """Acknowledgment of replication."""
    actor_id: str
    version: VectorClock
    success: bool


@serializable
@dataclass(frozen=True, slots=True)
class RequestFullSync:
    """Request full state sync (for new nodes)."""
    actor_id: str


@serializable
@dataclass(frozen=True, slots=True)
class FullSyncResponse:
    """Response with full state."""
    actor_id: str
    version: VectorClock
    state: dict[str, Any]


@serializable
@dataclass(frozen=True, slots=True)
class ClusteredSpawn:
    actor_id: str
    actor_cls_fqn: str
    replication: int
    singleton: bool


@serializable
@dataclass(frozen=True, slots=True)
class ClusteredSend:
    actor_id: str
    request_id: str
    payload_type: str
    payload: bytes
    consistency: int


@serializable
@dataclass(frozen=True, slots=True)
class ClusteredSendAck:
    request_id: str
    success: bool


@serializable
@dataclass(frozen=True, slots=True)
class ClusteredAsk:
    actor_id: str
    request_id: str
    payload_type: str
    payload: bytes
    consistency: int


@serializable
@dataclass(frozen=True, slots=True)
class ClusteredAskResponse:
    request_id: str
    payload_type: str
    payload: bytes
    success: bool


@dataclass(frozen=True, slots=True)
class RegisterClusteredActor:
    actor_id: str
    actor_cls: type
    replication: int
    singleton: bool


@serializable
@dataclass(frozen=True, slots=True)
class ActorRegistered:
    actor_id: str
    actor_cls_name: str
    replication: int
    singleton: bool
    owner_node: str


@dataclass(frozen=True, slots=True)
class GetClusteredActor:
    actor_id: str
