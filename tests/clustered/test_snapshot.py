import pytest
from casty.cluster.snapshot import Snapshot, InMemory


class TestSnapshot:
    def test_snapshot_creation(self):
        snapshot = Snapshot(data=b"test", version=1)
        assert snapshot.data == b"test"
        assert snapshot.version == 1

    def test_snapshot_default_version(self):
        snapshot = Snapshot(data=b"test")
        assert snapshot.version == 0


class TestInMemoryBackend:
    @pytest.mark.asyncio
    async def test_save_and_load_latest(self):
        backend = InMemory()

        await backend.save(Snapshot(data=b"v1", version=1))
        await backend.save(Snapshot(data=b"v2", version=2))

        latest = await backend.load_latest()
        assert latest is not None
        assert latest.data == b"v2"
        assert latest.version == 2

    @pytest.mark.asyncio
    async def test_load_latest_empty(self):
        backend = InMemory()
        latest = await backend.load_latest()
        assert latest is None

    @pytest.mark.asyncio
    async def test_prune_keeps_n_snapshots(self):
        backend = InMemory()

        for i in range(10):
            await backend.save(Snapshot(data=f"v{i}".encode(), version=i))

        await backend.prune(keep=3)

        latest = await backend.load_latest()
        assert latest is not None
        assert latest.data == b"v9"
        assert len(backend._history) == 3

    @pytest.mark.asyncio
    async def test_prune_with_fewer_snapshots(self):
        backend = InMemory()

        await backend.save(Snapshot(data=b"v1", version=1))
        await backend.prune(keep=10)

        assert len(backend._history) == 1
