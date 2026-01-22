from __future__ import annotations

import pytest
import tempfile
import shutil
from pathlib import Path

from casty.cluster.snapshot import Snapshot, FileBackend
from casty.cluster.vector_clock import VectorClock


class TestFileBackend:
    @pytest.fixture
    def temp_dir(self):
        path = tempfile.mkdtemp()
        yield path
        shutil.rmtree(path, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_save_and_load_latest(self, temp_dir):
        backend = FileBackend(temp_dir)

        await backend.save(Snapshot(data=b"v1", clock=VectorClock({"A": 1})))
        await backend.save(Snapshot(data=b"v2", clock=VectorClock({"A": 2})))

        latest = await backend.load_latest()
        assert latest is not None
        assert latest.data == b"v2"

    @pytest.mark.asyncio
    async def test_load_latest_empty(self, temp_dir):
        backend = FileBackend(temp_dir)
        latest = await backend.load_latest()
        assert latest is None

    @pytest.mark.asyncio
    async def test_persistence_across_instances(self, temp_dir):
        backend1 = FileBackend(temp_dir)
        await backend1.save(Snapshot(data=b"persistent", clock=VectorClock({"A": 1})))

        backend2 = FileBackend(temp_dir)
        latest = await backend2.load_latest()
        assert latest is not None
        assert latest.data == b"persistent"

    @pytest.mark.asyncio
    async def test_find_base(self, temp_dir):
        backend = FileBackend(temp_dir)

        await backend.save(Snapshot(data=b"base", clock=VectorClock({"A": 1, "B": 1})))
        await backend.save(Snapshot(data=b"v2", clock=VectorClock({"A": 2, "B": 1})))

        clock_a = VectorClock({"A": 3, "B": 1})
        clock_b = VectorClock({"A": 2, "B": 3})

        base = await backend.find_base(clock_a, clock_b)
        assert base is not None
        assert base.data == b"v2"

    @pytest.mark.asyncio
    async def test_prune(self, temp_dir):
        backend = FileBackend(temp_dir)

        for i in range(5):
            await backend.save(Snapshot(data=f"v{i}".encode(), clock=VectorClock({"A": i})))

        await backend.prune(keep=2)

        files = list(Path(temp_dir).glob("snapshot-*.msgpack"))
        assert len(files) == 2

    @pytest.mark.asyncio
    async def test_creates_directory_if_not_exists(self, temp_dir):
        nested_path = Path(temp_dir) / "nested" / "path"
        backend = FileBackend(str(nested_path))

        await backend.save(Snapshot(data=b"test", clock=VectorClock({"A": 1})))

        assert nested_path.exists()
