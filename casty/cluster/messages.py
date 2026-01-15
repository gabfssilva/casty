"""Cluster protocol messages for Casty.

This module defines:
- Protocol type enums (for wire protocol)
- Internal API messages (sent within the local actor system)
- Events (broadcast to subscribers)

Wire protocol messages are now defined in wire.py using @serializable.
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
    """Message types within ACTOR protocol.

    These IDs match the message_id used in @serializable decorators in wire.py.
    Kept for backwards compatibility.
    """

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
    # Replication messages (0x50-0x5F) - Legacy message-based
    REPLICATE_WRITE = 0x50
    REPLICATE_WRITE_ACK = 0x51
    HINTED_HANDOFF = 0x56
    # WAL-based replication (0x60-0x6F)
    REPLICATE_WAL_ENTRY = 0x60
    REPLICATE_WAL_ACK = 0x61
    REQUEST_CATCH_UP = 0x62
    CATCH_UP_ENTRIES = 0x63


class HandshakeMessageType(IntEnum):
    """Message types within HANDSHAKE protocol."""

    NODE_HANDSHAKE = 0x01
    NODE_HANDSHAKE_ACK = 0x02


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
# Internal API Messages - Replication
# =============================================================================


@dataclass(frozen=True, slots=True)
class CoordinateReplicatedWrite:
    """Internal: Cluster asks ReplicationCoordinator to handle write.

    Sent to ReplicationCoordinator to coordinate a replicated write
    across all replicas based on consistency level.
    """

    entity_type: str
    entity_id: str
    payload: Any
    preference_list: list[str]
    consistency: Any  # ShardConsistency - use Any to avoid circular import
    actor_type: str  # "sharded" | "singleton" | "named"


@dataclass(frozen=True, slots=True)
class CoordinateWALReplication:
    """Internal: Coordinate WAL-based replication.

    Sent to ReplicationCoordinator to replicate a WAL entry to all
    replicas. Unlike CoordinateReplicatedWrite which sends the original
    message, this sends the resulting state after processing.
    """

    wal_entry: Any  # ReplicationWALEntry - use Any to avoid circular import
    preference_list: list[str]
    consistency: Any  # ShardConsistency - use Any to avoid circular import


@dataclass(frozen=True, slots=True)
class RegisterReplicatedType:
    """Internal: Register type with replication metadata.

    Sent to ReplicationCoordinator to register a replicated actor type
    with its replication factor and consistency level.
    """

    type_name: str  # entity_type for sharded, actor_name for singleton/named
    actor_cls: type
    actor_type: str  # "sharded" | "singleton" | "named"
    replication_factor: int
    write_consistency: Any  # ShardConsistency


@dataclass(frozen=True, slots=True)
class LocalReplicaWrite:
    """Internal: Deliver replicated write to local entity.

    Sent from ReplicationCoordinator to Cluster to deliver
    a replicated write to a local entity (and send ack back).
    """

    entity_type: str
    entity_id: str
    payload: Any
    request_id: str
    actor_cls_fqn: str


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
