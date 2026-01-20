# tests/test_wal_actor.py
import pytest
from casty import ActorSystem


@pytest.mark.asyncio
async def test_wal_actor_append_and_read():
    from casty.wal.actor import wal_actor, Append, ReadAll
    from casty.wal.backend import InMemoryBackend

    backend = InMemoryBackend()

    async with ActorSystem() as system:
        ref = await system.actor(wal_actor(backend, "test/actor"), name="wal")

        await ref.send(Append(b"entry1"))
        await ref.send(Append(b"entry2"))

        entries = await ref.ask(ReadAll())
        assert len(entries) == 2


@pytest.mark.asyncio
async def test_wal_actor_snapshot():
    from casty.wal.actor import wal_actor, Append, Snapshot, GetSnapshot
    from casty.wal.backend import InMemoryBackend

    backend = InMemoryBackend()

    async with ActorSystem() as system:
        ref = await system.actor(wal_actor(backend, "test/actor"), name="wal")

        await ref.send(Append(b"entry1"))
        await ref.send(Snapshot(b"snapshot_data"))

        snapshot = await ref.ask(GetSnapshot())
        assert snapshot == b"snapshot_data"
