"""Replication protocol types used by event sourcing and shard replication.

Defines ``ReplicationConfig``, ``ReplicateEvents``, and ``ReplicateEventsAck``
— the protocol primitives needed by ``core/event_sourcing.py`` (L1) and
consumed by sharding actors at higher layers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.core.journal import PersistedEvent
    from casty.core.ref import ActorRef


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
