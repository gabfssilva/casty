import tempfile
from pathlib import Path

import pytest
from casty.wal.backend import InMemoryStoreBackend, FileStoreBackend
from casty.wal.entry import WALEntry, EntryType
from casty.wal.version import VectorClock


@pytest.mark.asyncio
async def test_in_memory_backend_append_and_read():
    backend = InMemoryStoreBackend()
    await backend.initialize()

    entry1 = WALEntry(
        version=VectorClock({"node-a": 1}),
        delta={"count": 1},
    )
    entry2 = WALEntry(
        version=VectorClock({"node-a": 2}),
        delta={"count": 2},
    )

    await backend.append(entry1)
    await backend.append(entry2)

    entries = [e async for e in backend.read_all()]

    assert len(entries) == 2
    assert entries[0].delta == {"count": 1}
    assert entries[1].delta == {"count": 2}

    await backend.close()


@pytest.mark.asyncio
async def test_in_memory_backend_empty():
    backend = InMemoryStoreBackend()
    await backend.initialize()

    entries = [e async for e in backend.read_all()]
    assert entries == []


@pytest.mark.asyncio
async def test_file_backend_append_and_read():
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStoreBackend(Path(tmpdir))
        await backend.initialize()

        entry1 = WALEntry(
            version=VectorClock({"node-a": 1}),
            delta={"count": 1},
        )
        entry2 = WALEntry(
            version=VectorClock({"node-a": 2}),
            delta={"count": 2},
        )

        await backend.append(entry1)
        await backend.append(entry2)
        await backend.close()

        backend2 = FileStoreBackend(Path(tmpdir))
        await backend2.initialize()

        entries = [e async for e in backend2.read_all()]

        assert len(entries) == 2
        assert entries[0].delta == {"count": 1}
        assert entries[1].delta == {"count": 2}

        await backend2.close()


@pytest.mark.asyncio
async def test_file_backend_persistence():
    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileStoreBackend(Path(tmpdir))
        await backend.initialize()

        entry = WALEntry(
            version=VectorClock({"node-a": 5}),
            delta={"name": "persisted"},
        )
        await backend.append(entry)
        await backend.close()

        backend2 = FileStoreBackend(Path(tmpdir))
        await backend2.initialize()
        entries = [e async for e in backend2.read_all()]

        assert len(entries) == 1
        assert entries[0].delta == {"name": "persisted"}
        assert entries[0].version.clock == {"node-a": 5}

        await backend2.close()
