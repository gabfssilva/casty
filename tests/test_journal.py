from __future__ import annotations

from dataclasses import dataclass

from casty.journal import InMemoryJournal, PersistedEvent, Snapshot


@dataclass(frozen=True)
class Deposited:
    amount: int


async def test_persist_and_load_events() -> None:
    journal = InMemoryJournal()
    events = [
        PersistedEvent(sequence_nr=1, event=Deposited(100), timestamp=1.0),
        PersistedEvent(sequence_nr=2, event=Deposited(200), timestamp=2.0),
    ]
    await journal.persist("acc-1", events)
    loaded = await journal.load("acc-1")
    assert len(loaded) == 2
    assert loaded[0].event == Deposited(100)
    assert loaded[1].sequence_nr == 2


async def test_load_from_sequence_nr() -> None:
    journal = InMemoryJournal()
    events = [
        PersistedEvent(sequence_nr=1, event=Deposited(100), timestamp=1.0),
        PersistedEvent(sequence_nr=2, event=Deposited(200), timestamp=2.0),
        PersistedEvent(sequence_nr=3, event=Deposited(300), timestamp=3.0),
    ]
    await journal.persist("acc-1", events)
    loaded = await journal.load("acc-1", from_sequence_nr=2)
    assert len(loaded) == 2
    assert loaded[0].sequence_nr == 2


async def test_load_empty_entity() -> None:
    journal = InMemoryJournal()
    loaded = await journal.load("nonexistent")
    assert loaded == []


async def test_save_and_load_snapshot() -> None:
    journal = InMemoryJournal()
    snapshot = Snapshot(sequence_nr=5, state={"balance": 500}, timestamp=5.0)
    await journal.save_snapshot("acc-1", snapshot)
    loaded = await journal.load_snapshot("acc-1")
    assert loaded is not None
    assert loaded.sequence_nr == 5
    assert loaded.state == {"balance": 500}


async def test_load_snapshot_nonexistent() -> None:
    journal = InMemoryJournal()
    loaded = await journal.load_snapshot("nonexistent")
    assert loaded is None


async def test_snapshot_overwrites_previous() -> None:
    journal = InMemoryJournal()
    await journal.save_snapshot("acc-1", Snapshot(sequence_nr=5, state="old", timestamp=5.0))
    await journal.save_snapshot("acc-1", Snapshot(sequence_nr=10, state="new", timestamp=10.0))
    loaded = await journal.load_snapshot("acc-1")
    assert loaded is not None
    assert loaded.state == "new"
    assert loaded.sequence_nr == 10


async def test_persist_appends_to_existing() -> None:
    journal = InMemoryJournal()
    await journal.persist("acc-1", [PersistedEvent(sequence_nr=1, event=Deposited(100), timestamp=1.0)])
    await journal.persist("acc-1", [PersistedEvent(sequence_nr=2, event=Deposited(200), timestamp=2.0)])
    loaded = await journal.load("acc-1")
    assert len(loaded) == 2
