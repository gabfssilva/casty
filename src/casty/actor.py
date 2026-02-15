"""Actor behavior definitions and marker types.

Re-exports the core ``Behavior[M]`` primitive and ``Behaviors`` factory.
Defines marker dataclasses recognized by upper layers (sharding, singleton,
discovery, broadcast) and event sourcing types.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from casty.core.behavior import Behavior, Signal
from casty.core.spy import SpyEvent
from casty.behaviors import Behaviors

if TYPE_CHECKING:
    from casty.core.context import ActorContext
    from casty.core.journal import EventJournal
    from casty.core.ref import ActorRef
    from casty.core.replication import ReplicationConfig


@dataclass(frozen=True)
class ShardedBehavior[M]:
    entity_factory: Callable[[str], Behavior[M]]
    num_shards: int = 100
    replication: ReplicationConfig | None = None


@dataclass(frozen=True)
class SingletonBehavior[M]:
    factory: Callable[[], Behavior[M]]


@dataclass(frozen=True)
class BroadcastedBehavior[M]:
    behavior: Behavior[M]


@dataclass(frozen=True)
class SnapshotEvery:
    n_events: int


type SnapshotPolicy = SnapshotEvery


@dataclass(frozen=True)
class PersistedBehavior[M, E]:
    events: tuple[E, ...]
    then: Behavior[M]


@dataclass(frozen=True)
class EventSourcedBehavior[M, E, S]:
    entity_id: str
    journal: EventJournal
    initial_state: S
    on_event: Callable[[S, E], S]
    on_command: Callable[[ActorContext[M], S, M], Awaitable[Behavior[M]]]
    snapshot_policy: SnapshotPolicy | None = None
    replica_refs: tuple[ActorRef[Any], ...] = ()
    replication: ReplicationConfig | None = None


__all__ = [
    "Behavior",
    "Behaviors",
    "BroadcastedBehavior",

    "EventSourcedBehavior",
    "PersistedBehavior",
    "ShardedBehavior",
    "Signal",
    "SingletonBehavior",
    "SnapshotEvery",
    "SnapshotPolicy",
    "SpyEvent",
]
