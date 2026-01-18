import pytest
from casty import ActorSystem
from casty.wal import (
    WriteAheadLog,
    Append,
    GetCurrentVersion,
    GetCurrentState,
    Recover,
    InMemoryStoreBackend,
    VectorClock,
)


@pytest.mark.asyncio
async def test_wal_actor_append():
    async with ActorSystem.local() as system:
        wal = await system.spawn(
            WriteAheadLog,
            node_id="node-a",
            backend=InMemoryStoreBackend(),
        )

        version = await wal.ask(Append(delta={"count": 10}))

        assert isinstance(version, VectorClock)
        assert version.clock == {"node-a": 1}


@pytest.mark.asyncio
async def test_wal_actor_multiple_appends():
    async with ActorSystem.local() as system:
        wal = await system.spawn(
            WriteAheadLog,
            node_id="node-a",
            backend=InMemoryStoreBackend(),
        )

        v1 = await wal.ask(Append(delta={"count": 1}))
        v2 = await wal.ask(Append(delta={"count": 2}))
        v3 = await wal.ask(Append(delta={"name": "test"}))

        assert v1.clock == {"node-a": 1}
        assert v2.clock == {"node-a": 2}
        assert v3.clock == {"node-a": 3}


@pytest.mark.asyncio
async def test_wal_actor_get_current_state():
    async with ActorSystem.local() as system:
        wal = await system.spawn(
            WriteAheadLog,
            node_id="node-a",
            backend=InMemoryStoreBackend(),
        )

        await wal.ask(Append(delta={"count": 1}))
        await wal.ask(Append(delta={"count": 2}))
        await wal.ask(Append(delta={"name": "test"}))

        state = await wal.ask(GetCurrentState())

        assert state == {"count": 2, "name": "test"}


@pytest.mark.asyncio
async def test_wal_actor_get_current_version():
    async with ActorSystem.local() as system:
        wal = await system.spawn(
            WriteAheadLog,
            node_id="node-a",
            backend=InMemoryStoreBackend(),
        )

        v0 = await wal.ask(GetCurrentVersion())
        assert v0.clock == {}

        await wal.ask(Append(delta={"x": 1}))

        v1 = await wal.ask(GetCurrentVersion())
        assert v1.clock == {"node-a": 1}
