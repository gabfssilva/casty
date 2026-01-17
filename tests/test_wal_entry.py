import msgpack
from casty.wal.entry import WALEntry, EntryType
from casty.wal.version import VectorClock


def test_wal_entry_as_bytes_roundtrip():
    version = VectorClock({"node-a": 1, "node-b": 2})
    entry = WALEntry(
        version=version,
        delta={"count": 10, "name": "test"},
        timestamp=1234567890.0,
        entry_type=EntryType.DELTA,
    )

    data = entry.as_bytes
    restored = WALEntry.from_bytes(data)

    assert restored.version == entry.version
    assert restored.delta == entry.delta
    assert restored.timestamp == entry.timestamp
    assert restored.entry_type == entry.entry_type


def test_wal_entry_default_values():
    version = VectorClock()
    entry = WALEntry(version=version, delta={"x": 1})

    assert entry.entry_type == EntryType.DELTA
    assert entry.timestamp > 0
