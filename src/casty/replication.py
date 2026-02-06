from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from casty.cluster_state import NodeAddress
    from casty.journal import PersistedEvent
    from casty.ref import ActorRef


@dataclass(frozen=True)
class ReplicationConfig:
    replicas: int = 0
    min_acks: int = 0
    ack_timeout: float = 5.0


@dataclass(frozen=True)
class ShardAllocation:
    primary: NodeAddress
    replicas: list[NodeAddress] = field(
        default_factory=lambda: list[NodeAddress]()
    )


@dataclass(frozen=True)
class ReplicateEvents:
    entity_id: str
    shard_id: int
    events: list[PersistedEvent[Any]]
    reply_to: ActorRef[Any] | None = None


@dataclass(frozen=True)
class ReplicateEventsAck:
    entity_id: str
    sequence_nr: int


@dataclass(frozen=True)
class ReplicaPromoted:
    entity_id: str
    shard_id: int
