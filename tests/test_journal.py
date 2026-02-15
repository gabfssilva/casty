from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from casty.core.journal import (
    EventJournal,
    InMemoryJournal,
    PersistedEvent,
    Snapshot,
    SqliteJournal,
)


@dataclass(frozen=True)
class Deposited:
    amount: int


@pytest.fixture(params=["memory", "sqlite"])
def journal(request: pytest.FixtureRequest) -> EventJournal:
    match request.param:
        case "memory":
            return InMemoryJournal()
        case "sqlite":
            j = SqliteJournal()
            request.addfinalizer(j.close)
            return j
        case _:
            raise ValueError(request.param)


async def test_persist_and_load_events(journal: EventJournal) -> None:
    events = [
        PersistedEvent(sequence_nr=1, event=Deposited(100), timestamp=1.0),
        PersistedEvent(sequence_nr=2, event=Deposited(200), timestamp=2.0),
    ]
    await journal.persist("acc-1", events)
    loaded = await journal.load("acc-1")
    assert len(loaded) == 2
    assert loaded[0].event == Deposited(100)
    assert loaded[1].sequence_nr == 2


async def test_load_from_sequence_nr(journal: EventJournal) -> None:
    events = [
        PersistedEvent(sequence_nr=1, event=Deposited(100), timestamp=1.0),
        PersistedEvent(sequence_nr=2, event=Deposited(200), timestamp=2.0),
        PersistedEvent(sequence_nr=3, event=Deposited(300), timestamp=3.0),
    ]
    await journal.persist("acc-1", events)
    loaded = await journal.load("acc-1", from_sequence_nr=2)
    assert len(loaded) == 2
    assert loaded[0].sequence_nr == 2


async def test_load_empty_entity(journal: EventJournal) -> None:
    loaded = await journal.load("nonexistent")
    assert loaded == []


async def test_save_and_load_snapshot(journal: EventJournal) -> None:
    snapshot = Snapshot(sequence_nr=5, state={"balance": 500}, timestamp=5.0)
    await journal.save_snapshot("acc-1", snapshot)
    loaded = await journal.load_snapshot("acc-1")
    assert loaded is not None
    assert loaded.sequence_nr == 5
    assert loaded.state == {"balance": 500}


async def test_load_snapshot_nonexistent(journal: EventJournal) -> None:
    loaded = await journal.load_snapshot("nonexistent")
    assert loaded is None


async def test_snapshot_overwrites_previous(journal: EventJournal) -> None:
    await journal.save_snapshot(
        "acc-1", Snapshot(sequence_nr=5, state="old", timestamp=5.0)
    )
    await journal.save_snapshot(
        "acc-1", Snapshot(sequence_nr=10, state="new", timestamp=10.0)
    )
    loaded = await journal.load_snapshot("acc-1")
    assert loaded is not None
    assert loaded.state == "new"
    assert loaded.sequence_nr == 10


async def test_persist_appends_to_existing(journal: EventJournal) -> None:
    await journal.persist(
        "acc-1", [PersistedEvent(sequence_nr=1, event=Deposited(100), timestamp=1.0)]
    )
    await journal.persist(
        "acc-1", [PersistedEvent(sequence_nr=2, event=Deposited(200), timestamp=2.0)]
    )
    loaded = await journal.load("acc-1")
    assert len(loaded) == 2


async def test_sqlite_persistence_across_instances(tmp_path: Path) -> None:
    db_path = tmp_path / "test.db"
    journal = SqliteJournal(db_path)
    await journal.persist(
        "acc-1", [PersistedEvent(sequence_nr=1, event=Deposited(100), timestamp=1.0)]
    )
    await journal.save_snapshot(
        "acc-1", Snapshot(sequence_nr=1, state={"balance": 100}, timestamp=1.0)
    )
    journal.close()

    journal2 = SqliteJournal(db_path)
    loaded_events = await journal2.load("acc-1")
    assert len(loaded_events) == 1
    assert loaded_events[0].event == Deposited(100)

    loaded_snap = await journal2.load_snapshot("acc-1")
    assert loaded_snap is not None
    assert loaded_snap.state == {"balance": 100}
    journal2.close()


async def test_sqlite_custom_serializer() -> None:
    def serialize(obj: object) -> bytes:
        return json.dumps(obj, default=str).encode()

    def deserialize(data: bytes) -> object:
        return json.loads(data)

    journal = SqliteJournal(serialize=serialize, deserialize=deserialize)
    await journal.persist(
        "acc-1", [PersistedEvent(sequence_nr=1, event="deposited", timestamp=1.0)]
    )
    loaded = await journal.load("acc-1")
    assert len(loaded) == 1
    assert loaded[0].event == "deposited"

    await journal.save_snapshot(
        "acc-1", Snapshot(sequence_nr=1, state={"balance": 100}, timestamp=1.0)
    )
    snap = await journal.load_snapshot("acc-1")
    assert snap is not None
    assert snap.state == {"balance": 100}
    journal.close()
