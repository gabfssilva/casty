"""Wire protocol messages using declarative @serializable.

This module defines all messages sent over TCP between cluster nodes.
Each message is decorated with @actor_protocol.serializable(ID) which:
1. Auto-applies @dataclass(frozen=True, slots=True)
2. Registers the class in the protocol registry
3. Enables automatic encode/decode

Usage:
    from casty.cluster.wire import actor_protocol, ActorMessage

    # Encode
    msg = ActorMessage(target_actor="counter", payload_type="Inc", payload=b"...")
    data = actor_protocol.encode(msg)

    # Decode
    decoded = actor_protocol.decode(data)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from casty.codec import ProtocolCodec

if TYPE_CHECKING:
    from casty import LocalRef
    from typing import Any


# =============================================================================
# Protocol Codec Instance
# =============================================================================

actor_protocol = ProtocolCodec("actor")
handshake_protocol = ProtocolCodec("handshake")


# =============================================================================
# Actor Protocol Messages (0x01 - 0x6F)
# =============================================================================


@actor_protocol.serializable(0x01)
class ActorMessage:
    """Fire-and-forget message to remote actor."""

    target_actor: str
    payload_type: str
    payload: bytes


@actor_protocol.serializable(0x02)
class AskRequest:
    """Request part of ask pattern."""

    request_id: str
    target_actor: str
    payload_type: str
    payload: bytes


@actor_protocol.serializable(0x03)
class AskResponse:
    """Response part of ask pattern."""

    request_id: str
    success: bool
    payload_type: str | None
    payload: bytes


@actor_protocol.serializable(0x04)
class RegisterActor:
    """Announce actor registration to cluster."""

    actor_name: str
    node_id: str


@actor_protocol.serializable(0x05)
class LookupActor:
    """Query for actor location."""

    actor_name: str
    request_id: str


@actor_protocol.serializable(0x06)
class LookupResponse:
    """Response to actor lookup."""

    actor_name: str
    request_id: str
    node_id: str | None


# =============================================================================
# Sharded Entity Messages (0x10 - 0x12)
# =============================================================================


@actor_protocol.serializable(0x10)
class EntitySend:
    """Fire-and-forget message to sharded entity."""

    entity_type: str
    entity_id: str
    payload_type: str
    payload: bytes
    actor_cls_fqn: str = ""


@actor_protocol.serializable(0x11)
class EntityAskRequest:
    """Request part of ask pattern for sharded entity."""

    request_id: str
    entity_type: str
    entity_id: str
    payload_type: str
    payload: bytes
    actor_cls_fqn: str = ""


@actor_protocol.serializable(0x12)
class EntityAskResponse:
    """Response part of ask pattern for sharded entity."""

    request_id: str
    success: bool
    payload_type: str | None
    payload: bytes


# =============================================================================
# Singleton Messages (0x20 - 0x22)
# =============================================================================


@actor_protocol.serializable(0x20)
class SingletonSend:
    """Fire-and-forget message to singleton actor."""

    singleton_name: str
    payload_type: str
    payload: bytes
    actor_cls_fqn: str = ""


@actor_protocol.serializable(0x21)
class SingletonAskRequest:
    """Request part of ask pattern for singleton actor."""

    request_id: str
    singleton_name: str
    payload_type: str
    payload: bytes
    actor_cls_fqn: str = ""


@actor_protocol.serializable(0x22)
class SingletonAskResponse:
    """Response part of ask pattern for singleton actor."""

    request_id: str
    success: bool
    payload_type: str | None
    payload: bytes


# =============================================================================
# Sharded Type Registration (0x30 - 0x31)
# =============================================================================


@actor_protocol.serializable(0x30)
class ShardedTypeRegister:
    """Wire: Register a sharded type on remote node."""

    request_id: str
    entity_type: str
    actor_cls_fqn: str


@actor_protocol.serializable(0x31)
class ShardedTypeAck:
    """Wire: Acknowledgment of sharded type registration."""

    request_id: str
    entity_type: str
    success: bool
    error: str | None = None


# =============================================================================
# Three-Way Merge Messages (0x40 - 0x42)
# =============================================================================


@actor_protocol.serializable(0x40)
class MergeRequest:
    """Wire: Request actor state for merge."""

    request_id: str
    entity_type: str
    entity_id: str
    my_version: int
    my_base_version: int


@actor_protocol.serializable(0x41)
class MergeState:
    """Wire: Actor state snapshot for merge."""

    request_id: str
    entity_type: str
    entity_id: str
    version: int
    base_version: int
    state: bytes
    base_state: bytes


@actor_protocol.serializable(0x42)
class MergeComplete:
    """Wire: Acknowledgment of merge completion."""

    request_id: str
    entity_type: str
    entity_id: str
    new_version: int


# =============================================================================
# Replication Messages (0x50 - 0x5F)
# =============================================================================


@actor_protocol.serializable(0x50)
class ReplicateWrite:
    """Wire: Coordinator sends write to replica."""

    entity_type: str
    entity_id: str
    payload_type: str
    payload: bytes
    coordinator_id: str
    request_id: str
    actor_cls_fqn: str = ""


@actor_protocol.serializable(0x51)
class ReplicateWriteAck:
    """Wire: Replica acknowledges write to coordinator."""

    request_id: str
    entity_type: str
    entity_id: str
    node_id: str
    success: bool
    error: str | None = None


@actor_protocol.serializable(0x56)
class HintedHandoff:
    """Wire: Replay stored write to recovered replica."""

    entity_type: str
    entity_id: str
    payload_type: str
    payload: bytes
    original_timestamp: float
    actor_cls_fqn: str = ""


# =============================================================================
# WAL-Based Replication (0x60 - 0x6F)
# =============================================================================


@actor_protocol.serializable(0x60)
class ReplicateWALEntry:
    """Wire: Primary sends WAL entry to replica."""

    request_id: str
    coordinator_id: str
    sequence: int
    entity_type: str
    entity_id: str
    state: bytes
    message_type: str
    primary_node: str
    timestamp: float
    checksum: int


@actor_protocol.serializable(0x61)
class ReplicateWALAck:
    """Wire: Replica acknowledges WAL entry application."""

    request_id: str
    entity_type: str
    entity_id: str
    node_id: str
    sequence: int
    success: bool
    error: str | None = None


@actor_protocol.serializable(0x62)
class RequestCatchUp:
    """Wire: Replica requests missing WAL entries."""

    entity_type: str
    entity_id: str
    since_sequence: int
    requesting_node: str


@actor_protocol.serializable(0x63)
class CatchUpEntries:
    """Wire: Primary sends missing WAL entries to replica."""

    entity_type: str
    entity_id: str
    entries: list[bytes]
    from_sequence: int
    to_sequence: int


# =============================================================================
# Handshake Protocol Messages (0x01 - 0x02)
# =============================================================================


@handshake_protocol.serializable(0x01)
class NodeHandshake:
    """Initial connection handshake."""

    node_id: str
    address: tuple[str, int]
    protocol_version: int = 1


@handshake_protocol.serializable(0x02)
class NodeHandshakeAck:
    """Handshake acknowledgment."""

    node_id: str
    address: tuple[str, int]
    accepted: bool
    reason: str | None = None


# =============================================================================
# Type Aliases
# =============================================================================

WireActorMessage = (
    ActorMessage
    | AskRequest
    | AskResponse
    | RegisterActor
    | LookupActor
    | LookupResponse
    | EntitySend
    | EntityAskRequest
    | EntityAskResponse
    | SingletonSend
    | SingletonAskRequest
    | SingletonAskResponse
    | ShardedTypeRegister
    | ShardedTypeAck
    | MergeRequest
    | MergeState
    | MergeComplete
    | ReplicateWrite
    | ReplicateWriteAck
    | HintedHandoff
    | ReplicateWALEntry
    | ReplicateWALAck
    | RequestCatchUp
    | CatchUpEntries
)

WireHandshakeMessage = NodeHandshake | NodeHandshakeAck
