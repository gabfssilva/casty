from __future__ import annotations

from dataclasses import field
from typing import TYPE_CHECKING, Any

from .core import message

if TYPE_CHECKING:
    from .core import ActorRef
    from .io import Framer


# === Remote Messages ===

@message
class Listen:
    port: int
    host: str = "0.0.0.0"


@message
class Connect:
    host: str
    port: int


@message
class Listening:
    address: tuple[str, int]


@message
class RemoteConnected:
    remote_address: tuple[str, int]
    peer_id: str


Connected = RemoteConnected


@message
class ListenFailed:
    reason: str


@message
class ConnectFailed:
    reason: str
    peer: str | None = None


@message
class Expose:
    ref: "ActorRef"
    name: str


@message
class Unexpose:
    name: str


@message
class Lookup:
    name: str
    peer: str | None = None
    timeout: float | None = None
    ensure: bool = False
    initial_state: bytes | None = None
    behavior: str | None = None


@message
class Exposed:
    name: str


@message
class Unexposed:
    name: str


@message
class LookupResult:
    ref: "ActorRef | None"
    error: str | None = None
    peer: str | None = None


# === Cluster / Membership Messages ===

@message
class MemberSnapshot:
    node_id: str
    address: str
    state: str  # "alive" | "down"
    incarnation: int


@message
class Ping:
    sender: str
    members: list[MemberSnapshot] = field(default_factory=list)


@message
class Ack:
    sender: str
    members: list[MemberSnapshot] = field(default_factory=list)


@message
class PingReq:
    sender: str
    target: str
    members: list[MemberSnapshot] = field(default_factory=list)


@message
class PingReqAck:
    sender: str
    target: str
    success: bool
    members: list[MemberSnapshot] = field(default_factory=list)


@message
class Join:
    node_id: str
    address: str


@message
class SetLocalAddress:
    address: str


@message
class GetAliveMembers:
    pass


@message
class GetAllMembers:
    pass


@message
class GetResponsibleNodes:
    actor_id: str
    count: int = 1


@message
class GetAddress:
    node_id: str


@message
class MergeMembership:
    members: list[MemberSnapshot]


@message
class MarkDown:
    node_id: str


@message
class MarkAlive:
    node_id: str
    address: str


@message
class GetLeaderId:
    actor_id: str
    replicas: int = 3


@message
class IsLeader:
    actor_id: str
    replicas: int = 3


@message
class GetReplicaIds:
    actor_id: str
    replicas: int = 3


@message
class SwimTick:
    pass


@message
class ProbeTimeout:
    target: str


@message
class PingReqTimeout:
    target: str


@message
class IndirectProbeTimeout:
    target: str
    requester: str


# === Replication Messages ===

@message
class Replicate:
    actor_id: str
    version: int
    snapshot: bytes


@message
class ReplicateAck:
    actor_id: str
    version: int
    node_id: str


@message
class SyncRequest:
    actor_id: str


@message
class SyncResponse:
    actor_id: str
    version: int
    snapshot: bytes


# === IO Messages ===

@message
class Bind:
    handler: "ActorRef"
    port: int
    host: str = "0.0.0.0"
    framing: "Framer | None" = None


@message
class IoConnect:
    handler: "ActorRef"
    host: str
    port: int
    framing: "Framer | None" = None


@message
class Register:
    handler: "ActorRef"


@message
class Write:
    data: bytes


@message
class Close:
    pass


@message
class Bound:
    local_address: tuple[str, int]


@message
class BindFailed:
    reason: str


@message
class IoConnected:
    connection: "ActorRef"
    remote_address: tuple[str, int]
    local_address: tuple[str, int]


@message
class IoConnectFailed:
    reason: str


@message
class Received:
    data: bytes


@message
class PeerClosed:
    pass


@message
class ErrorClosed:
    reason: str


@message
class Aborted:
    pass


@message
class WritingResumed:
    pass


# === Gossip Messages ===

@message
class GossipPut:
    key: str
    value: bytes
    version: int = 0


@message
class GossipGet:
    key: str


@message
class GossipTick:
    pass


# === State Store Messages ===

@message
class StoreState:
    actor_id: str
    state: dict[str, Any]


@message
class StoreAck:
    actor_id: str
    success: bool = True


@message
class GetState:
    actor_id: str


@message
class DeleteState:
    actor_id: str


# === Type Aliases ===

type InboundEvent = (
    Bound | BindFailed |
    IoConnected | IoConnectFailed |
    Received |
    PeerClosed | ErrorClosed | Aborted | WritingResumed
)

type OutboundEvent = Register | Write | Close

type GossipMessage = GossipPut | GossipGet | GossipTick

type StatesMessage = StoreState | GetState | DeleteState


# === Clustered Actor Messages ===

@message
class Subscribe:
    pattern: str
    subscriber: "ActorRef"


@message
class Unsubscribe:
    pattern: str
    subscriber: "ActorRef"


@message
class Forward:
    payload: bytes
    original_sender_id: str | None = None


@message
class MembershipChanged:
    actor_id: str
    leader_id: str | None
    replica_nodes: list[str]
    addresses: dict[str, str]
