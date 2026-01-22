import pytest
from casty.cluster.snapshot import Snapshot, InMemory
from casty.cluster.vector_clock import VectorClock


class TestSnapshot:
    def test_snapshot_creation(self):
        clock = VectorClock({"A": 1})
        snapshot = Snapshot(data=b"test", clock=clock)
        assert snapshot.data == b"test"
        assert snapshot.clock.versions == {"A": 1}


class TestInMemoryBackend:
    @pytest.mark.asyncio
    async def test_save_and_load_latest(self):
        backend = InMemory()

        await backend.save(Snapshot(data=b"v1", clock=VectorClock({"A": 1})))
        await backend.save(Snapshot(data=b"v2", clock=VectorClock({"A": 2})))

        latest = await backend.load_latest()
        assert latest is not None
        assert latest.data == b"v2"

    @pytest.mark.asyncio
    async def test_load_latest_empty(self):
        backend = InMemory()
        latest = await backend.load_latest()
        assert latest is None

    @pytest.mark.asyncio
    async def test_find_base_concurrent_clocks(self):
        backend = InMemory()

        await backend.save(Snapshot(data=b"base", clock=VectorClock({"A": 1, "B": 1})))
        await backend.save(Snapshot(data=b"v2", clock=VectorClock({"A": 2, "B": 1})))
        await backend.save(Snapshot(data=b"v3", clock=VectorClock({"A": 2, "B": 2})))

        # Concurrent: {A:3, B:1} vs {A:2, B:3}
        # Meet: {A:2, B:1}
        clock_a = VectorClock({"A": 3, "B": 1})
        clock_b = VectorClock({"A": 2, "B": 3})

        base = await backend.find_base(clock_a, clock_b)
        assert base is not None
        assert base.data == b"v2"  # clock {A:2, B:1} <= meet {A:2, B:1}

    @pytest.mark.asyncio
    async def test_find_base_no_match(self):
        backend = InMemory()

        await backend.save(Snapshot(data=b"v1", clock=VectorClock({"A": 5, "B": 5})))

        clock_a = VectorClock({"A": 1})
        clock_b = VectorClock({"B": 1})

        base = await backend.find_base(clock_a, clock_b)
        assert base is None

    @pytest.mark.asyncio
    async def test_prune_keeps_n_snapshots(self):
        backend = InMemory()

        for i in range(10):
            await backend.save(Snapshot(data=f"v{i}".encode(), clock=VectorClock({"A": i})))

        await backend.prune(keep=3)

        latest = await backend.load_latest()
        assert latest is not None
        assert latest.data == b"v9"

        # Verify only 3 remain
        assert len(backend._history) == 3

    @pytest.mark.asyncio
    async def test_prune_with_fewer_snapshots(self):
        backend = InMemory()

        await backend.save(Snapshot(data=b"v1", clock=VectorClock({"A": 1})))
        await backend.prune(keep=10)

        assert len(backend._history) == 1
