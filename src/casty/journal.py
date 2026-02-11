"""Event sourcing journal for persistence of events and snapshots.

Provides the ``EventJournal`` protocol, an ``InMemoryJournal`` implementation,
and the ``PersistedEvent`` / ``Snapshot`` envelope dataclasses used by the
event-sourcing layer.
"""

from __future__ import annotations

import asyncio
import pickle
import sqlite3
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol


class JournalKind(Enum):
    """Declares whether a journal backend is node-local or centralized.

    ``local``
        Each node has its own independent store (e.g. SQLite file).
        Replicas must persist events they receive via ``ReplicateEvents``.

    ``centralized``
        All nodes share the same durable store (e.g. PostgreSQL, S3).
        Replicas skip persistence — the primary's write is already visible
        to every node.
    """

    local = "local"
    centralized = "centralized"


@dataclass(frozen=True)
class PersistedEvent[E]:
    """An event wrapped with journal metadata.

    Parameters
    ----------
    sequence_nr : int
        Monotonically increasing sequence number for the entity.
    event : E
        The domain event payload.
    timestamp : float
        Unix timestamp when the event was persisted.

    Examples
    --------
    >>> from casty import PersistedEvent
    >>> evt = PersistedEvent(sequence_nr=1, event="deposited", timestamp=1.0)
    >>> evt.sequence_nr
    1
    """

    sequence_nr: int
    event: E
    timestamp: float


@dataclass(frozen=True)
class Snapshot[S]:
    """A point-in-time snapshot of an entity's state.

    Parameters
    ----------
    sequence_nr : int
        The sequence number up to which this snapshot covers.
    state : S
        The serialized actor state at snapshot time.
    timestamp : float
        Unix timestamp when the snapshot was taken.

    Examples
    --------
    >>> from casty import Snapshot
    >>> snap = Snapshot(sequence_nr=10, state={"balance": 100}, timestamp=2.0)
    >>> snap.state
    {'balance': 100}
    """

    sequence_nr: int
    state: S
    timestamp: float


class EventJournal(Protocol):
    """Protocol for event journal backends.

    Any class that implements ``persist``, ``load``, ``save_snapshot``, and
    ``load_snapshot`` satisfies this protocol via structural subtyping.

    The ``kind`` property tells the replication layer whether replicas need
    to persist events themselves (``local``) or can skip persistence because
    the store is shared (``centralized``).

    Examples
    --------
    >>> from casty import InMemoryJournal
    >>> journal: EventJournal = InMemoryJournal()
    """

    @property
    def kind(self) -> JournalKind:
        """Whether this journal is node-local or centralized."""
        ...

    async def persist(
        self, entity_id: str, events: Sequence[PersistedEvent[Any]]
    ) -> None:
        """Append events to the journal for a given entity.

        Parameters
        ----------
        entity_id : str
            Unique identifier of the entity.
        events : Sequence[PersistedEvent[Any]]
            Events to persist, in order.

        Examples
        --------
        >>> await journal.persist("acct-1", [PersistedEvent(1, "evt", 0.0)])
        """
        ...

    async def load(
        self, entity_id: str, from_sequence_nr: int = 0
    ) -> list[PersistedEvent[Any]]:
        """Load events for an entity starting from a sequence number.

        Parameters
        ----------
        entity_id : str
            Unique identifier of the entity.
        from_sequence_nr : int
            Minimum sequence number (inclusive). Defaults to 0.

        Returns
        -------
        list[PersistedEvent[Any]]
            Events with ``sequence_nr >= from_sequence_nr``.

        Examples
        --------
        >>> events = await journal.load("acct-1", from_sequence_nr=5)
        """
        ...

    async def save_snapshot(
        self, entity_id: str, snapshot: Snapshot[Any]
    ) -> None:
        """Save a snapshot for an entity, replacing any previous one.

        Parameters
        ----------
        entity_id : str
            Unique identifier of the entity.
        snapshot : Snapshot[Any]
            The snapshot to store.

        Examples
        --------
        >>> snap = Snapshot(sequence_nr=10, state=100, timestamp=1.0)
        >>> await journal.save_snapshot("acct-1", snap)
        """
        ...

    async def load_snapshot(
        self, entity_id: str
    ) -> Snapshot[Any] | None:
        """Load the latest snapshot for an entity, if one exists.

        Parameters
        ----------
        entity_id : str
            Unique identifier of the entity.

        Returns
        -------
        Snapshot[Any] | None
            The stored snapshot, or ``None`` if no snapshot exists.

        Examples
        --------
        >>> snap = await journal.load_snapshot("acct-1")
        """
        ...


class InMemoryJournal:
    """In-memory event journal for testing and development.

    Stores events and snapshots in plain dictionaries. Data is lost when
    the process exits. Satisfies the ``EventJournal`` protocol.

    Always ``local`` — each process has its own independent store.

    Examples
    --------
    >>> from casty import InMemoryJournal, PersistedEvent
    >>> journal = InMemoryJournal()
    >>> await journal.persist("e1", [PersistedEvent(1, "created", 0.0)])
    >>> events = await journal.load("e1")
    >>> len(events)
    1
    """

    def __init__(self) -> None:
        self._events: dict[str, list[PersistedEvent[Any]]] = {}
        self._snapshots: dict[str, Snapshot[Any]] = {}

    @property
    def kind(self) -> JournalKind:
        return JournalKind.local

    async def persist(
        self, entity_id: str, events: Sequence[PersistedEvent[Any]]
    ) -> None:
        """Append events to the in-memory store.

        Parameters
        ----------
        entity_id : str
            Unique identifier of the entity.
        events : Sequence[PersistedEvent[Any]]
            Events to persist.

        Examples
        --------
        >>> await journal.persist("e1", [PersistedEvent(1, "x", 0.0)])
        """
        existing = self._events.setdefault(entity_id, [])
        existing.extend(events)

    async def load(
        self, entity_id: str, from_sequence_nr: int = 0
    ) -> list[PersistedEvent[Any]]:
        """Load events from the in-memory store.

        Parameters
        ----------
        entity_id : str
            Unique identifier of the entity.
        from_sequence_nr : int
            Minimum sequence number (inclusive). Defaults to 0.

        Returns
        -------
        list[PersistedEvent[Any]]
            Matching events in insertion order.

        Examples
        --------
        >>> events = await journal.load("e1", from_sequence_nr=2)
        """
        events = self._events.get(entity_id, [])
        return [e for e in events if e.sequence_nr >= from_sequence_nr]

    async def save_snapshot(
        self, entity_id: str, snapshot: Snapshot[Any]
    ) -> None:
        """Save a snapshot, replacing any previous one for this entity.

        Parameters
        ----------
        entity_id : str
            Unique identifier of the entity.
        snapshot : Snapshot[Any]
            The snapshot to store.

        Examples
        --------
        >>> snap = Snapshot(sequence_nr=5, state="s", timestamp=1.0)
        >>> await journal.save_snapshot("e1", snap)
        """
        self._snapshots[entity_id] = snapshot

    async def load_snapshot(
        self, entity_id: str
    ) -> Snapshot[Any] | None:
        """Load the latest snapshot for an entity.

        Parameters
        ----------
        entity_id : str
            Unique identifier of the entity.

        Returns
        -------
        Snapshot[Any] | None
            The stored snapshot, or ``None``.

        Examples
        --------
        >>> snap = await journal.load_snapshot("e1")
        """
        return self._snapshots.get(entity_id)


class SqliteJournal:
    """SQLite-backed event journal for durable persistence.

    Uses Python's stdlib ``sqlite3`` with WAL mode for concurrent reads and
    ``asyncio.to_thread`` for non-blocking I/O. A ``threading.Lock`` serializes
    all writes.

    Parameters
    ----------
    path : str | Path
        Path to the SQLite database file, or ``":memory:"`` for an in-memory
        database (useful for testing).
    serialize : Callable[[Any], bytes]
        Serializer for events and snapshot state. Defaults to ``pickle.dumps``.
    deserialize : Callable[[bytes], Any]
        Deserializer for events and snapshot state. Defaults to ``pickle.loads``.

    Examples
    --------
    >>> from casty import SqliteJournal, PersistedEvent
    >>> journal = SqliteJournal()
    >>> await journal.persist("e1", [PersistedEvent(1, "created", 0.0)])
    >>> events = await journal.load("e1")
    >>> len(events)
    1
    """

    def __init__(
        self,
        path: str | Path = ":memory:",
        *,
        serialize: Callable[[Any], bytes] = pickle.dumps,
        deserialize: Callable[[bytes], Any] = pickle.loads,  # noqa: S301
    ) -> None:
        self._serialize = serialize
        self._deserialize = deserialize
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                entity_id   TEXT    NOT NULL,
                sequence_nr INTEGER NOT NULL,
                event_data  BLOB    NOT NULL,
                timestamp   REAL    NOT NULL,
                PRIMARY KEY (entity_id, sequence_nr)
            );
            CREATE TABLE IF NOT EXISTS snapshots (
                entity_id   TEXT    NOT NULL PRIMARY KEY,
                sequence_nr INTEGER NOT NULL,
                state_data  BLOB    NOT NULL,
                timestamp   REAL    NOT NULL
            );
            """
        )

    @property
    def kind(self) -> JournalKind:
        return JournalKind.local

    async def persist(
        self, entity_id: str, events: Sequence[PersistedEvent[Any]]
    ) -> None:
        rows = [
            (entity_id, e.sequence_nr, self._serialize(e.event), e.timestamp)
            for e in events
        ]
        await asyncio.to_thread(self._persist_sync, rows)

    def _persist_sync(self, rows: list[tuple[str, int, bytes, float]]) -> None:
        with self._lock:
            self._conn.executemany(
                "INSERT INTO events (entity_id, sequence_nr, event_data, timestamp) VALUES (?, ?, ?, ?)",
                rows,
            )
            self._conn.commit()

    async def load(
        self, entity_id: str, from_sequence_nr: int = 0
    ) -> list[PersistedEvent[Any]]:
        return await asyncio.to_thread(self._load_sync, entity_id, from_sequence_nr)

    def _load_sync(
        self, entity_id: str, from_sequence_nr: int
    ) -> list[PersistedEvent[Any]]:
        cursor = self._conn.execute(
            "SELECT sequence_nr, event_data, timestamp FROM events "
            "WHERE entity_id = ? AND sequence_nr >= ? ORDER BY sequence_nr",
            (entity_id, from_sequence_nr),
        )
        return [
            PersistedEvent(
                sequence_nr=row[0],
                event=self._deserialize(row[1]),
                timestamp=row[2],
            )
            for row in cursor
        ]

    async def save_snapshot(
        self, entity_id: str, snapshot: Snapshot[Any]
    ) -> None:
        data = self._serialize(snapshot.state)
        await asyncio.to_thread(
            self._save_snapshot_sync, entity_id, snapshot.sequence_nr, data, snapshot.timestamp
        )

    def _save_snapshot_sync(
        self, entity_id: str, sequence_nr: int, state_data: bytes, timestamp: float
    ) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO snapshots (entity_id, sequence_nr, state_data, timestamp) "
                "VALUES (?, ?, ?, ?)",
                (entity_id, sequence_nr, state_data, timestamp),
            )
            self._conn.commit()

    async def load_snapshot(
        self, entity_id: str
    ) -> Snapshot[Any] | None:
        return await asyncio.to_thread(self._load_snapshot_sync, entity_id)

    def _load_snapshot_sync(self, entity_id: str) -> Snapshot[Any] | None:
        cursor = self._conn.execute(
            "SELECT sequence_nr, state_data, timestamp FROM snapshots WHERE entity_id = ?",
            (entity_id,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return Snapshot(
            sequence_nr=row[0],
            state=self._deserialize(row[1]),
            timestamp=row[2],
        )

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self._conn.close()
