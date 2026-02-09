"""Event sourcing journal for persistence of events and snapshots.

Provides the ``EventJournal`` protocol, an ``InMemoryJournal`` implementation,
and the ``PersistedEvent`` / ``Snapshot`` envelope dataclasses used by the
event-sourcing layer.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol


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

    Examples
    --------
    >>> from casty import InMemoryJournal
    >>> journal: EventJournal = InMemoryJournal()
    """

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
