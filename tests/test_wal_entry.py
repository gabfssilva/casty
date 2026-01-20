# tests/test_wal_entry.py
import pytest


def test_wal_entry_creation():
    from casty.wal.entry import WALEntry

    entry = WALEntry(
        actor_id="counter/c1",
        sequence=1,
        timestamp=1234567890.0,
        data=b"test data",
    )

    assert entry.actor_id == "counter/c1"
    assert entry.sequence == 1
    assert entry.data == b"test data"


def test_wal_entry_serialization():
    from casty.wal.entry import WALEntry

    entry = WALEntry(
        actor_id="counter/c1",
        sequence=1,
        timestamp=1234567890.0,
        data=b"test data",
    )

    serialized = entry.to_bytes()
    restored = WALEntry.from_bytes(serialized)

    assert restored == entry
