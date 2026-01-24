import asyncio
import pytest
from casty import ActorSystem
from casty.cluster.states import (
    states,
    StoreState,
    GetState,
    DeleteState,
    StoreAck,
    MemoryBackend,
)


@pytest.mark.asyncio
async def test_states_actor_store_and_get():
    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(states(), name="states")

            await ref.ask(StoreState("actor-1", {"count": 10}))

            result = await ref.ask(GetState("actor-1"))
            assert result == {"count": 10}


@pytest.mark.asyncio
async def test_states_actor_returns_none_for_unknown():
    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(states(), name="states")

            result = await ref.ask(GetState("unknown"))
            assert result is None


@pytest.mark.asyncio
async def test_states_actor_delete():
    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(states(), name="states")

            await ref.ask(StoreState("actor-1", {"count": 10}))
            await ref.send(DeleteState("actor-1"))
            await asyncio.sleep(0.1)

            result = await ref.ask(GetState("actor-1"))
            assert result is None


@pytest.mark.asyncio
async def test_states_actor_store_returns_ack():
    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(states(), name="states")

            ack = await ref.ask(StoreState("actor-1", {"count": 10}))
            assert isinstance(ack, StoreAck)
            assert ack.actor_id == "actor-1"
            assert ack.success is True


@pytest.mark.asyncio
async def test_states_actor_overwrites_state():
    async with asyncio.timeout(5):
        async with ActorSystem() as system:
            ref = await system.actor(states(), name="states")

            await ref.ask(StoreState("actor-1", {"count": 10}))
            await ref.ask(StoreState("actor-1", {"count": 20}))

            result = await ref.ask(GetState("actor-1"))
            assert result == {"count": 20}


@pytest.mark.asyncio
async def test_memory_backend():
    backend = MemoryBackend()

    await backend.store("actor-1", {"x": 1})
    result = await backend.get("actor-1")
    assert result == {"x": 1}

    await backend.delete("actor-1")
    result = await backend.get("actor-1")
    assert result is None
