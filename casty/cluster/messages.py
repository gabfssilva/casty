"""Cluster protocol messages for Casty.

This module defines all messages used by the Cluster actor for:
- Wire protocol (sent over TCP between nodes)
- Internal API (sent within the local actor system)
- Events (broadcast to subscribers)
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from casty import LocalRef


# =============================================================================
# Protocol Types (for multiplexing)
# =============================================================================


class ProtocolType(IntEnum):
    """Protocol discriminator for multiplexed port."""

    SWIM = 0x01  # SWIM failure detection messages
    ACTOR = 0x02  # Actor messaging (send/ask)
    HANDSHAKE = 0x03  # Node handshake
    GOSSIP = 0x04  # Gossip state replication


class ActorMessageType(IntEnum):
    """Message types within ACTOR protocol."""

    ACTOR_MESSAGE = 0x01
    ASK_REQUEST = 0x02
    ASK_RESPONSE = 0x03
    REGISTER_ACTOR = 0x04
    LOOKUP_ACTOR = 0x05
    LOOKUP_RESPONSE = 0x06
    # Sharded entity messages
    ENTITY_SEND = 0x10
    ENTITY_ASK_REQUEST = 0x11
    ENTITY_ASK_RESPONSE = 0x12
    # Singleton messages
    SINGLETON_SEND = 0x20
    SINGLETON_ASK_REQUEST = 0x21
    SINGLETON_ASK_RESPONSE = 0x22
    # Sharded type registration (for consistency levels)
    SHARDED_TYPE_REGISTER = 0x30
    SHARDED_TYPE_ACK = 0x31
    # Three-way merge messages
    MERGE_REQUEST = 0x40
    MERGE_STATE = 0x41
    MERGE_COMPLETE = 0x42


class HandshakeMessageType(IntEnum):
    """Message types within HANDSHAKE protocol."""

    NODE_HANDSHAKE = 0x01
    NODE_HANDSHAKE_ACK = 0x02


# =============================================================================
# Wire Protocol Messages - Actor (0x02)
# =============================================================================


@dataclass(frozen=True, slots=True)
class ActorMessage:
    """Fire-and-forget message to remote actor.

    Sent when using remote_ref.send(msg).
    """

    target_actor: str
    payload_type: str  # Fully-qualified class name for deserialization
    payload: bytes  # msgpack serialized message


@dataclass(frozen=True, slots=True)
class AskRequest:
    """Request part of ask pattern.

    Sent when using remote_ref.ask(msg).
    """

    request_id: str
    target_actor: str
    payload_type: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class AskResponse:
    """Response part of ask pattern.

    Sent as response to AskRequest.
    """

    request_id: str
    success: bool
    payload_type: str | None  # None if error
    payload: bytes  # Result or error message


@dataclass(frozen=True, slots=True)
class RegisterActor:
    """Announce actor registration to cluster.

    Sent when a new actor is registered with a name.
    """

    actor_name: str
    node_id: str


@dataclass(frozen=True, slots=True)
class LookupActor:
    """Query for actor location.

    Request to find which node hosts an actor.
    """

    actor_name: str
    request_id: str


@dataclass(frozen=True, slots=True)
class LookupResponse:
    """Response to actor lookup.

    Contains the node_id where the actor is hosted.
    """

    actor_name: str
    request_id: str
    node_id: str | None  # None if not found


# =============================================================================
# Wire Protocol Messages - Sharded Entities (0x02, types 0x10-0x12)
# =============================================================================


@dataclass(frozen=True, slots=True)
class EntitySend:
    """Fire-and-forget message to sharded entity.

    Sent when using sharded_ref["entity-id"].send(msg).
    """

    entity_type: str
    entity_id: str
    payload_type: str  # Fully-qualified class name for deserialization
    payload: bytes  # msgpack serialized message
    actor_cls_fqn: str = ""  # Fully-qualified actor class name for auto-registration


@dataclass(frozen=True, slots=True)
class EntityAskRequest:
    """Request part of ask pattern for sharded entity.

    Sent when using sharded_ref["entity-id"].ask(msg).
    """

    request_id: str
    entity_type: str
    entity_id: str
    payload_type: str
    payload: bytes
    actor_cls_fqn: str = ""  # Fully-qualified actor class name for auto-registration


@dataclass(frozen=True, slots=True)
class EntityAskResponse:
    """Response part of ask pattern for sharded entity.

    Sent as response to EntityAskRequest.
    """

    request_id: str
    success: bool
    payload_type: str | None  # None if error
    payload: bytes  # Result or error message


# =============================================================================
# Wire Protocol Messages - Singleton Actors (0x02, types 0x20-0x22)
# =============================================================================


@dataclass(frozen=True, slots=True)
class SingletonSend:
    """Fire-and-forget message to singleton actor.

    Sent when using singleton_ref.send(msg).
    """

    singleton_name: str
    payload_type: str  # Fully-qualified class name for deserialization
    payload: bytes  # msgpack serialized message
    actor_cls_fqn: str = ""  # Fully-qualified actor class name for auto-registration


@dataclass(frozen=True, slots=True)
class SingletonAskRequest:
    """Request part of ask pattern for singleton actor.

    Sent when using singleton_ref.ask(msg).
    """

    request_id: str
    singleton_name: str
    payload_type: str
    payload: bytes
    actor_cls_fqn: str = ""  # Fully-qualified actor class name for auto-registration


@dataclass(frozen=True, slots=True)
class SingletonAskResponse:
    """Response part of ask pattern for singleton actor.

    Sent as response to SingletonAskRequest.
    """

    request_id: str
    success: bool
    payload_type: str | None  # None if error
    payload: bytes  # Result or error message


# =============================================================================
# Wire Protocol Messages - Sharded Type Registration (0x02, types 0x30-0x31)
# =============================================================================


@dataclass(frozen=True, slots=True)
class ShardedTypeRegister:
    """Wire: Register a sharded type on remote node.

    Sent when spawning sharded actors with consistency > Eventual.
    The remote node should register the type and send an ack.
    """

    request_id: str
    entity_type: str
    actor_cls_fqn: str  # Fully-qualified class name for import


@dataclass(frozen=True, slots=True)
class ShardedTypeAck:
    """Wire: Acknowledgment of sharded type registration.

    Response to ShardedTypeRegister.
    """

    request_id: str
    entity_type: str
    success: bool
    error: str | None = None


# =============================================================================
# Wire Protocol Messages - Three-Way Merge (0x02, types 0x40-0x42)
# =============================================================================


@dataclass(frozen=True, slots=True)
class MergeRequest:
    """Wire: Request actor state for merge.

    Sent when conflict is detected between versions.
    The receiving node should respond with MergeState.
    """

    request_id: str
    entity_type: str
    entity_id: str
    my_version: int
    my_base_version: int


@dataclass(frozen=True, slots=True)
class MergeState:
    """Wire: Actor state snapshot for merge.

    Response to MergeRequest containing state and base snapshot
    for three-way merge.
    """

    request_id: str
    entity_type: str
    entity_id: str
    version: int
    base_version: int
    state: bytes  # msgpack serialized current state
    base_state: bytes  # msgpack serialized base snapshot


@dataclass(frozen=True, slots=True)
class MergeComplete:
    """Wire: Acknowledgment of merge completion.

    Sent after merge is complete to synchronize versions.
    """

    request_id: str
    entity_type: str
    entity_id: str
    new_version: int


# Wire message union type
type WireActorMessage = (
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
)


# =============================================================================
# Wire Protocol Messages - Handshake (0x03)
# =============================================================================


@dataclass(frozen=True, slots=True)
class NodeHandshake:
    """Initial connection handshake.

    First message sent when connecting to another node.
    """

    node_id: str
    address: tuple[str, int]
    protocol_version: int = 1


@dataclass(frozen=True, slots=True)
class NodeHandshakeAck:
    """Handshake acknowledgment.

    Response to NodeHandshake.
    """

    node_id: str
    address: tuple[str, int]
    accepted: bool
    reason: str | None = None


WireHandshakeMessage = NodeHandshake | NodeHandshakeAck


# =============================================================================
# Internal API Messages (Cluster Actor)
# =============================================================================


@dataclass(frozen=True, slots=True)
class RemoteSend:
    """Internal: Route send to remote actor.

    Sent to Cluster actor to forward a message to a remote node.
    """

    target_actor: str
    target_node: str
    payload: Any


@dataclass(frozen=True, slots=True)
class RemoteAsk:
    """Internal: Route ask to remote actor.

    Sent to Cluster actor to forward an ask to a remote node.
    """

    target_actor: str
    target_node: str
    payload: Any


@dataclass(frozen=True, slots=True)
class LocalRegister:
    """Internal: Register a local actor.

    Sent to Cluster to register an actor name -> ref mapping.
    """

    actor_name: str
    actor_ref: "LocalRef[Any]"


@dataclass(frozen=True, slots=True)
class LocalUnregister:
    """Internal: Unregister a local actor.

    Sent to Cluster when an actor stops.
    """

    actor_name: str


@dataclass(frozen=True, slots=True)
class GetRemoteRef:
    """Internal: Get or create RemoteRef.

    Request to get a reference to a remote actor.
    """

    actor_name: str
    node_id: str | None = None  # None = lookup required


@dataclass(frozen=True, slots=True)
class GetMembers:
    """Query cluster membership.

    Returns dict of node_id -> MemberInfo.
    """

    pass


@dataclass(frozen=True, slots=True)
class GetLocalActors:
    """Query local actor registry.

    Returns dict of actor_name -> ActorRef.
    """

    pass


# =============================================================================
# Internal API Messages - Sharded Entities
# =============================================================================


@dataclass(frozen=True, slots=True)
class RemoteEntitySend:
    """Internal: Route send to sharded entity.

    Sent to Cluster actor to forward a message to an entity.
    The Cluster uses HashRing to determine the target node.
    """

    entity_type: str
    entity_id: str
    payload: Any


@dataclass(frozen=True, slots=True)
class RemoteEntityAsk:
    """Internal: Route ask to sharded entity.

    Sent to Cluster actor to forward an ask to an entity.
    The Cluster uses HashRing to determine the target node.
    """

    entity_type: str
    entity_id: str
    payload: Any


@dataclass(frozen=True, slots=True)
class RegisterShardedType:
    """Internal: Register a sharded actor type.

    Sent to Cluster to register an entity_type -> actor_cls mapping.
    The consistency level determines how many nodes must acknowledge
    the registration before returning.
    """

    entity_type: str
    actor_cls: type
    consistency: Any = None  # ShardConsistency - use Any to avoid circular import


# =============================================================================
# Internal API Messages - Singleton Actors
# =============================================================================


@dataclass(frozen=True, slots=True)
class RegisterSingleton:
    """Internal: Register a singleton actor type.

    Sent to Cluster to register a singleton. The Cluster determines
    ownership via consistent hashing and spawns locally if owner.
    """

    singleton_name: str
    actor_cls: type


@dataclass(frozen=True, slots=True)
class RemoteSingletonSend:
    """Internal: Route send to singleton actor.

    Sent to Cluster actor to forward a message to a singleton.
    The Cluster looks up the current owner and routes accordingly.
    """

    singleton_name: str
    payload: Any


@dataclass(frozen=True, slots=True)
class RemoteSingletonAsk:
    """Internal: Route ask to singleton actor.

    Sent to Cluster actor to forward an ask to a singleton.
    The Cluster looks up the current owner and routes accordingly.
    """

    singleton_name: str
    payload: Any


# =============================================================================
# Events (broadcast to subscribers)
# =============================================================================


@dataclass(frozen=True, slots=True)
class NodeJoined:
    """Event: A new node joined the cluster."""

    node_id: str
    address: tuple[str, int]


@dataclass(frozen=True, slots=True)
class NodeLeft:
    """Event: A node left the cluster (graceful)."""

    node_id: str


@dataclass(frozen=True, slots=True)
class NodeFailed:
    """Event: A node failed (detected by SWIM)."""

    node_id: str


@dataclass(frozen=True, slots=True)
class ActorRegistered:
    """Event: An actor was registered in the cluster."""

    actor_name: str
    node_id: str


@dataclass(frozen=True, slots=True)
class ActorUnregistered:
    """Event: An actor was unregistered from the cluster."""

    actor_name: str
    node_id: str


ClusterEvent = NodeJoined | NodeLeft | NodeFailed | ActorRegistered | ActorUnregistered


@dataclass(frozen=True, slots=True)
class Subscribe:
    """Subscribe to cluster events."""

    subscriber: "LocalRef[ClusterEvent]"


@dataclass(frozen=True, slots=True)
class Unsubscribe:
    """Unsubscribe from cluster events."""

    subscriber: "LocalRef[ClusterEvent]"


# =============================================================================
# Internal TCP Events (from tcp.Connection)
# =============================================================================


@dataclass(frozen=True, slots=True)
class _ConnectionReady:
    """Internal: Connection completed handshake."""

    node_id: str
    connection: "LocalRef[Any]"


@dataclass(frozen=True, slots=True)
class _ConnectionClosed:
    """Internal: Connection was closed."""

    node_id: str


@dataclass(frozen=True, slots=True)
class _IncomingData:
    """Internal: Data received from connection."""

    node_id: str
    data: bytes
