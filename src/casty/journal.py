from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class PersistedEvent[E]:
    sequence_nr: int
    event: E
    timestamp: float


@dataclass(frozen=True)
class Snapshot[S]:
    sequence_nr: int
    state: S
    timestamp: float


class EventJournal(Protocol):
    async def persist(
        self, entity_id: str, events: Sequence[PersistedEvent[Any]]
    ) -> None: ...

    async def load(
        self, entity_id: str, from_sequence_nr: int = 0
    ) -> list[PersistedEvent[Any]]: ...

    async def save_snapshot(
        self, entity_id: str, snapshot: Snapshot[Any]
    ) -> None: ...

    async def load_snapshot(
        self, entity_id: str
    ) -> Snapshot[Any] | None: ...


class InMemoryJournal:
    def __init__(self) -> None:
        self._events: dict[str, list[PersistedEvent[Any]]] = {}
        self._snapshots: dict[str, Snapshot[Any]] = {}

    async def persist(
        self, entity_id: str, events: Sequence[PersistedEvent[Any]]
    ) -> None:
        existing = self._events.setdefault(entity_id, [])
        existing.extend(events)

    async def load(
        self, entity_id: str, from_sequence_nr: int = 0
    ) -> list[PersistedEvent[Any]]:
        events = self._events.get(entity_id, [])
        return [e for e in events if e.sequence_nr >= from_sequence_nr]

    async def save_snapshot(
        self, entity_id: str, snapshot: Snapshot[Any]
    ) -> None:
        self._snapshots[entity_id] = snapshot

    async def load_snapshot(
        self, entity_id: str
    ) -> Snapshot[Any] | None:
        return self._snapshots.get(entity_id)
