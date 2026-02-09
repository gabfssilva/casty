"""Replication data types for primary-replica shard failover.

Defines configuration and internal messages used by the shard coordinator
and replica region actors to replicate events across cluster nodes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.cluster_state import NodeAddress
    from casty.journal import PersistedEvent
    from casty.ref import ActorRef


@dataclass(frozen=True)
class ReplicationConfig:
    """Configuration for shard replication.

    Controls how many replica copies are maintained and how many
    acknowledgements are required before a write is considered committed.

    Parameters
    ----------
    replicas : int
        Number of replica copies per shard (``0`` disables replication).
    min_acks : int
        Minimum replica acknowledgements required before confirming a write.
    ack_timeout : float
        Seconds to wait for replica acknowledgements.

    Examples
    --------
    >>> ReplicationConfig(replicas=2, min_acks=1, ack_timeout=3.0)
    ReplicationConfig(replicas=2, min_acks=1, ack_timeout=3.0)
    """
    replicas: int = 0
    min_acks: int = 0
    ack_timeout: float = 5.0


@dataclass(frozen=True)
class ShardAllocation:
    """Tracks which node owns a shard and where its replicas live.

    Parameters
    ----------
    primary : NodeAddress
        Node that owns the shard and handles writes.
    replicas : tuple[NodeAddress, ...]
        Nodes holding passive replica copies.

    Examples
    --------
    >>> alloc = ShardAllocation(primary=NodeAddress("10.0.0.1", 25520))
    >>> alloc.replicas
    ()
    """
    primary: NodeAddress
    replicas: tuple[NodeAddress, ...] = ()


@dataclass(frozen=True)
class ReplicateEvents:
    """Internal message: replicate persisted events to a replica node.

    Sent by the primary shard region to replica regions after persisting
    new events locally.

    Parameters
    ----------
    entity_id : str
        Entity whose events are being replicated.
    shard_id : int
        Shard that owns the entity.
    events : tuple[PersistedEvent[Any], ...]
        Events to replicate.
    reply_to : ActorRef[Any] | None
        Optional ref to acknowledge replication.

    Examples
    --------
    >>> ReplicateEvents("user-1", shard_id=7, events=(evt,))
    ReplicateEvents(entity_id='user-1', shard_id=7, ...)
    """
    entity_id: str
    shard_id: int
    events: tuple[PersistedEvent[Any], ...]
    reply_to: ActorRef[Any] | None = None


@dataclass(frozen=True)
class ReplicateEventsAck:
    """Acknowledgement that a replica has persisted replicated events.

    Parameters
    ----------
    entity_id : str
        Entity whose events were replicated.
    sequence_nr : int
        Highest sequence number persisted by the replica.

    Examples
    --------
    >>> ReplicateEventsAck("user-1", sequence_nr=5)
    ReplicateEventsAck(entity_id='user-1', sequence_nr=5)
    """
    entity_id: str
    sequence_nr: int


@dataclass(frozen=True)
class ReplicaPromoted:
    """Internal message: notifies a replica that it has been promoted to primary.

    Sent by the shard coordinator when the previous primary is detected as
    down.  The replica transitions from passive event persistence to active
    command handling.

    Parameters
    ----------
    entity_id : str
        Entity being promoted.
    shard_id : int
        Shard that owns the entity.

    Examples
    --------
    >>> ReplicaPromoted("user-1", shard_id=7)
    ReplicaPromoted(entity_id='user-1', shard_id=7)
    """
    entity_id: str
    shard_id: int
