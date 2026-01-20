# tests/test_wal_backend.py
import pytest
import tempfile


@pytest.mark.asyncio
async def test_inmemory_backend():
    from casty.wal.backend import InMemoryBackend
    from casty.wal.entry import WALEntry

    backend = InMemoryBackend()

    entry = WALEntry("counter/c1", 1, 123.0, b"data1")
    await backend.append(entry)

    entries = await backend.read("counter/c1")
    assert len(entries) == 1
    assert entries[0] == entry


@pytest.mark.asyncio
async def test_inmemory_backend_multiple_actors():
    from casty.wal.backend import InMemoryBackend
    from casty.wal.entry import WALEntry

    backend = InMemoryBackend()

    await backend.append(WALEntry("a/1", 1, 123.0, b"a1"))
    await backend.append(WALEntry("b/1", 1, 124.0, b"b1"))
    await backend.append(WALEntry("a/1", 2, 125.0, b"a2"))

    a_entries = await backend.read("a/1")
    assert len(a_entries) == 2

    b_entries = await backend.read("b/1")
    assert len(b_entries) == 1


@pytest.mark.asyncio
async def test_file_backend():
    from casty.wal.backend import FileBackend
    from casty.wal.entry import WALEntry

    with tempfile.TemporaryDirectory() as tmpdir:
        backend = FileBackend(path=tmpdir)

        entry = WALEntry("counter/c1", 1, 123.0, b"data1")
        await backend.append(entry)

        entries = await backend.read("counter/c1")
        assert len(entries) == 1
        assert entries[0].data == b"data1"


@pytest.mark.asyncio
async def test_file_backend_persistence():
    from casty.wal.backend import FileBackend
    from casty.wal.entry import WALEntry

    with tempfile.TemporaryDirectory() as tmpdir:
        backend1 = FileBackend(path=tmpdir)
        await backend1.append(WALEntry("counter/c1", 1, 123.0, b"data"))

        backend2 = FileBackend(path=tmpdir)
        entries = await backend2.read("counter/c1")
        assert len(entries) == 1
