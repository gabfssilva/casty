"""Replication metadata and coordination data structures."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from casty.cluster.consistency import ShardConsistency


@dataclass
class ReplicationMetadata:
    """Metadata for a replicated actor type.

    Tracks configuration for a replicated actor type including
    replication factor and consistency level for writes.
    """

    type_name: str
    actor_cls: type[Any]
    actor_type: str  # "sharded" | "singleton" | "named"
    replication_factor: int
    write_consistency: ShardConsistency


@dataclass
class WriteCoordination:
    """Tracks in-flight replicated write coordination state.

    Used by ReplicationCoordinator to manage parallel writes to replicas
    and track acknowledgments.
    """

    request_id: str
    entity_type: str
    entity_id: str
    payload: Any
    target_replicas: list[str]
    acks_received: set[str]
    required_acks: int
    reply_future: asyncio.Future[bool]
    started_at: float
    timeout_task: asyncio.Task[None] | None = None


@dataclass
class WriteHint:
    """Stored write for offline replica (hinted handoff).

    When a replica is offline, we store the write as a hint.
    When the replica recovers, we replay these hints to catch it up.
    """

    entity_type: str
    entity_id: str
    payload_type: str  # FQN for deserialization
    payload: bytes  # msgpack serialized
    timestamp: float
    actor_cls_fqn: str = ""
